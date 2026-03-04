"""
Schema Selector — Stage 3 Orchestrator
========================================
CHESS-style 4-step schema selection + QueryWeaver graph traversal.

Steps:
  3.1  ColumnFilter     – 1,200 tables → ~60 candidate columns (LLM)
  3.2  GraphPathFinder  – Bridge table injection (NetworkX, local)
  3.3  TableSelector    – → 3-8 tables (LLM)
  3.4  ColumnPruner     – → ≤15 columns/table with Content Awareness (LLM)
"""

from __future__ import annotations

import logging
from typing import Any

from ..connectors.openai_client import OpenAIClient
from ..connectors.pgvector_store import PgVectorStore
from .column_filter import ColumnFilter
from .graph_path_finder import GraphPathFinder
from .table_selector import TableSelector
from .column_pruner import ColumnPruner

logger = logging.getLogger(__name__)


class SchemaSelector:
    """Stage 3 orchestrator — run the 4-step schema selection pipeline."""

    def __init__(
        self,
        openai: OpenAIClient,
        pgvector: PgVectorStore,
        graph: Any = None,
    ):
        self.openai = openai
        self.pgvector = pgvector

        # Sub-modules
        self.column_filter = ColumnFilter(openai, pgvector)
        self.graph_finder = GraphPathFinder(graph)
        self.table_selector = TableSelector(openai)
        self.column_pruner = ColumnPruner(openai)

        # Optional full column catalog (populated during preprocessing)
        self._full_column_catalog: dict[str, list[dict]] | None = None

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

        Returns:
            {
                "candidate_columns": [...],
                "selected_tables": [...],
                "pruned_schema": "TABLE: ...\n  - col (type)...",
                "join_paths": [...],
            }
        """
        # ── Step 1: Column Filtering ──────────────────────────────────
        candidate_columns = await self.column_filter.filter(
            question, entity_matches, glossary_matches, similar_queries,
        )

        # ── Step 2: Graph Path Discovery (bridge tables) ─────────────
        expanded_tables = self.graph_finder.expand_tables(candidate_columns)
        graph_join_paths = self.graph_finder.find_join_paths(expanded_tables)

        # ── Step 3: Table Selection ───────────────────────────────────
        selected_tables, join_paths = await self.table_selector.select(
            question, expanded_tables, graph_join_paths,
        )

        # ── Step 4: Column Pruning ────────────────────────────────────
        pruned_schema = await self.column_pruner.prune(
            question,
            selected_tables,
            content_awareness,
            doc_snippets,
            full_column_catalog=self._full_column_catalog,
        )

        logger.info(
            "Schema selection done: %d candidate cols → %d tables → pruned",
            len(candidate_columns),
            len(selected_tables),
        )

        return {
            "candidate_columns": candidate_columns,
            "selected_tables": selected_tables,
            "pruned_schema": pruned_schema,
            "join_paths": join_paths,
        }

    # ── Hot-swap helpers ───────────────────────────────────────────────

    def set_graph(self, graph: Any) -> None:
        """Set the NetworkX table-relationship graph."""
        self.graph_finder.set_graph(graph)

    def set_column_catalog(self, catalog: dict[str, list[dict]]) -> None:
        """Set the full column catalog for column pruning."""
        self._full_column_catalog = catalog
