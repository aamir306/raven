"""
Stage 3: Schema Selector
=========================
CHESS-style 4-step schema selection + QueryWeaver graph traversal.
  1. Column Filtering (1,200 tables → ~60 candidate columns)
  2. Graph Path Discovery (bridge table injection via NetworkX)
  3. Table Selection (→ 3-8 tables)
  4. Column Pruning (→ <15 columns per table with Content Awareness)
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from ..connectors.openai_client import OpenAIClient
from ..connectors.pgvector_store import PgVectorStore

logger = logging.getLogger(__name__)

PROMPTS_DIR = Path(__file__).resolve().parents[3] / "prompts"


class SchemaSelector:
    """Select and prune schema for SQL generation."""

    def __init__(self, openai: OpenAIClient, pgvector: PgVectorStore):
        self.openai = openai
        self.pgvector = pgvector
        self._graph = None  # NetworkX graph loaded during preprocessing

        # Load prompt templates
        self._column_filter_prompt = (PROMPTS_DIR / "ss_column_filter.txt").read_text()
        self._table_select_prompt = (PROMPTS_DIR / "ss_table_select.txt").read_text()
        self._column_prune_prompt = (PROMPTS_DIR / "ss_column_prune.txt").read_text()

    async def select(
        self,
        question: str,
        entity_matches: list[dict],
        glossary_matches: list[dict],
        similar_queries: list[dict],
        doc_snippets: list[dict],
        content_awareness: list[dict],
    ) -> dict:
        """
        Run 4-step schema selection.

        Returns dict with:
            candidate_columns, selected_tables, pruned_schema, join_paths
        """
        # Step 1: Column Filtering
        candidate_columns = await self._filter_columns(
            question, entity_matches, glossary_matches, similar_queries,
        )

        # Step 2: Graph Path Discovery (bridge tables)
        expanded_tables = self._discover_graph_paths(candidate_columns)

        # Step 3: Table Selection
        selected_tables, join_paths = await self._select_tables(
            question, expanded_tables,
        )

        # Step 4: Column Pruning
        pruned_schema = await self._prune_columns(
            question, selected_tables, content_awareness, doc_snippets,
        )

        return {
            "candidate_columns": candidate_columns,
            "selected_tables": selected_tables,
            "pruned_schema": pruned_schema,
            "join_paths": join_paths,
        }

    async def _filter_columns(
        self,
        question: str,
        entity_matches: list[dict],
        glossary_matches: list[dict],
        similar_queries: list[dict],
    ) -> list[str]:
        """Step 1: LLM identifies ~20-60 relevant columns from condensed catalog."""
        # Build context strings
        entity_str = "\n".join(
            f"- {m['keyword']} → {m['table']}.{m['column']}" for m in entity_matches
        ) or "None"

        glossary_str = "\n".join(
            f"- {m['term']}: {m['definition']}" for m in glossary_matches
        ) or "None"

        fewshot_tables = set()
        for q in similar_queries:
            # Extract table names from past SQL (simplified)
            sql = q.get("sql", "")
            # Tables will be extracted from SQL during preprocessing
            fewshot_tables.add(sql[:100])  # Placeholder
        fewshot_str = ", ".join(fewshot_tables) or "None"

        # Get condensed catalog from pgvector schema embeddings
        condensed_catalog = await self._get_condensed_catalog()

        prompt = (
            self._column_filter_prompt
            .replace("{user_question}", question)
            .replace("{entity_matches}", entity_str)
            .replace("{glossary_matches}", glossary_str)
            .replace("{fewshot_tables}", fewshot_str)
            .replace("{condensed_catalog}", condensed_catalog)
            .replace("{table_count}", str(condensed_catalog.count("\n") + 1))
        )

        response = await self.openai.complete(prompt=prompt, stage_name="ss_column_filter")

        # Parse response: each line is "table.column — reason"
        columns = []
        for line in response.strip().split("\n"):
            line = line.strip("- •").strip()
            if "." in line and "—" in line:
                col_ref = line.split("—")[0].strip()
                columns.append(col_ref)
        return columns

    def _discover_graph_paths(self, candidate_columns: list[str]) -> list[str]:
        """Step 2: Use NetworkX to find bridge tables between candidates."""
        if not self._graph:
            # Return unique tables from candidate columns
            return list({c.rsplit(".", 1)[0] for c in candidate_columns if "." in c})

        import networkx as nx
        from itertools import combinations

        candidate_tables = list({c.rsplit(".", 1)[0] for c in candidate_columns if "." in c})
        full_set = set(candidate_tables)

        for t1, t2 in combinations(candidate_tables, 2):
            try:
                path = nx.shortest_path(self._graph, t1, t2)
                full_set.update(path)
            except (nx.NetworkXNoPath, nx.NodeNotFound):
                pass

        return list(full_set)

    async def _select_tables(
        self,
        question: str,
        expanded_tables: list[str],
    ) -> tuple[list[str], list[str]]:
        """Step 3: LLM selects 3-8 tables and JOIN paths."""
        # Build table descriptions (will be populated during preprocessing)
        table_desc = "\n".join(f"- {t}" for t in expanded_tables)

        prompt = (
            self._table_select_prompt
            .replace("{user_question}", question)
            .replace("{candidate_tables_with_descriptions}", table_desc)
        )

        response = await self.openai.complete(prompt=prompt, stage_name="ss_table_select")

        # Parse selected tables and join paths
        selected_tables = []
        join_paths = []
        section = None

        for line in response.strip().split("\n"):
            line = line.strip()
            if "SELECTED_TABLES" in line.upper():
                section = "tables"
                continue
            elif "JOIN_PATH" in line.upper():
                section = "joins"
                continue

            if section == "tables" and line and line[0].isdigit():
                # Parse: "1. table_name — reason — JOIN: ..."
                parts = line.split("—")
                table = parts[0].strip().lstrip("0123456789. ")
                if table:
                    selected_tables.append(table)
            elif section == "joins" and "JOIN" in line.upper():
                join_paths.append(line)

        return selected_tables, join_paths

    async def _prune_columns(
        self,
        question: str,
        selected_tables: list[str],
        content_awareness: list[dict],
        doc_snippets: list[dict],
    ) -> str:
        """Step 4: Prune to needed columns with Content Awareness metadata."""
        # Build full column listing (populated during preprocessing)
        full_cols = "\n".join(f"TABLE: {t}\n  (columns loaded during preprocessing)" for t in selected_tables)

        awareness_str = "\n".join(
            f"- {a['table']}.{a['column']}: {a.get('data_type', '')} | "
            f"format: {a.get('format_pattern', '')} | null: {a.get('null_pct', '')}%"
            for a in content_awareness
        ) or "None"

        docs_str = "\n".join(
            f"- [{d['source']}] {d['table']}: {d['content'][:200]}" for d in doc_snippets
        ) or "None"

        prompt = (
            self._column_prune_prompt
            .replace("{user_question}", question)
            .replace("{selected_tables_full_columns}", full_cols)
            .replace("{content_awareness}", awareness_str)
            .replace("{doc_snippets}", docs_str)
        )

        response = await self.openai.complete(prompt=prompt, stage_name="ss_column_prune")
        return response.strip()

    async def _get_condensed_catalog(self) -> str:
        """Retrieve condensed catalog (table | description) from pgvector."""
        results = self.pgvector.search(
            table_name="schema_embeddings",
            query_embedding=[0.0] * 1536,  # Placeholder — will use actual embedding
            top_k=200,
        )
        lines = []
        for r in results:
            meta = r.get("metadata", {})
            lines.append(f"{meta.get('table_name', 'unknown')} | {meta.get('description', '')}")
        return "\n".join(lines) if lines else "(No catalog loaded — run preprocessing first)"

    def set_graph(self, graph: Any) -> None:
        """Set the NetworkX graph (loaded during preprocessing)."""
        self._graph = graph
