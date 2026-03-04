"""
Column Filter — Stage 3.1
==========================
First pass: LLM selects ~20-60 candidate columns from a condensed
catalog of 1,200+ tables.  Uses entity matches, glossary terms,
and few-shot table references as evidence.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from ..connectors.openai_client import OpenAIClient
from ..connectors.pgvector_store import PgVectorStore

logger = logging.getLogger(__name__)

PROMPT_PATH = Path(__file__).resolve().parents[3] / "prompts" / "ss_column_filter.txt"


class ColumnFilter:
    """Step 1 of Schema Selection — broad column identification."""

    def __init__(self, openai: OpenAIClient, pgvector: PgVectorStore):
        self.openai = openai
        self.pgvector = pgvector
        self._prompt_template = PROMPT_PATH.read_text()

    async def filter(
        self,
        question: str,
        entity_matches: list[dict],
        glossary_matches: list[dict],
        similar_queries: list[dict],
    ) -> list[str]:
        """
        Identify candidate columns from the full catalog.

        Args:
            question: User question.
            entity_matches: From LSH matcher.
            glossary_matches: From glossary retriever.
            similar_queries: Past Q-SQL pairs with tables_used.

        Returns:
            List of "catalog.schema.table.column" references.
        """
        # Build evidence strings
        entity_str = "\n".join(
            f"- {m['keyword']} → {m['table']}.{m['column']}" for m in entity_matches
        ) or "None"

        glossary_str = "\n".join(
            f"- {m['term']}: {m['definition']}" for m in glossary_matches
        ) or "None"

        # Collect tables referenced in few-shot examples
        fewshot_tables: set[str] = set()
        for q in similar_queries:
            tables = q.get("tables_used", [])
            if isinstance(tables, list):
                fewshot_tables.update(tables)
            # Also extract FROM/JOIN tables from SQL text (simple heuristic)
            sql = q.get("sql", "")
            for token in sql.replace(",", " ").split():
                if token.count(".") >= 2:  # catalog.schema.table
                    fewshot_tables.add(token.rstrip(")").lstrip("("))
        fewshot_str = ", ".join(sorted(fewshot_tables)) or "None"

        # Condensed catalog
        condensed_catalog = await self._get_condensed_catalog(question)

        prompt = (
            self._prompt_template
            .replace("{user_question}", question)
            .replace("{entity_matches}", entity_str)
            .replace("{glossary_matches}", glossary_str)
            .replace("{fewshot_tables}", fewshot_str)
            .replace("{condensed_catalog}", condensed_catalog)
            .replace("{table_count}", str(condensed_catalog.count("\n") + 1))
        )

        response = await self.openai.complete(
            prompt=prompt, stage_name="ss_column_filter",
        )

        columns = self._parse_columns(response)
        logger.info("Column filter: %d candidate columns selected", len(columns))
        return columns

    # ── Helpers ─────────────────────────────────────────────────────────

    async def _get_condensed_catalog(self, question: str) -> str:
        """
        Retrieve a condensed catalog from pgvector schema embeddings.

        Each line: "catalog.schema.table | description | key columns"
        Top-200 by embedding similarity to the question.
        """
        question_embedding = await self.openai.embed(question)
        results = self.pgvector.search(
            table_name="schema_embeddings",
            query_embedding=question_embedding,
            top_k=200,
        )
        lines: list[str] = []
        for r in results:
            meta = r.get("metadata", {})
            name = meta.get("table_name", "unknown")
            desc = meta.get("description", "")
            cols = meta.get("key_columns", "")
            lines.append(f"{name} | {desc} | {cols}")
        return "\n".join(lines) if lines else "(No catalog loaded — run preprocessing first)"

    @staticmethod
    def _parse_columns(response: str) -> list[str]:
        """Parse LLM response — each line: 'table.column — reason'."""
        columns: list[str] = []
        for line in response.strip().split("\n"):
            line = line.strip("- •").strip()
            if "." in line and "—" in line:
                col_ref = line.split("—")[0].strip()
                if col_ref:
                    columns.append(col_ref)
            elif "." in line and line[0].isalpha():
                # Fallback: bare table.column without reason
                col_ref = line.split()[0].strip(",")
                if col_ref.count(".") >= 1:
                    columns.append(col_ref)
        return columns
