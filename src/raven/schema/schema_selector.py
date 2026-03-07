"""
Schema Selector — Stage 3 Orchestrator
========================================
CHESS-style 4-step schema selection + QueryWeaver graph traversal.
Integrated with OpenMetadata MCP for live lineage + quality checks.

Steps:
  3.1  ColumnFilter     – 1,200 tables → ~60 candidate columns (LLM + OM semantic search)
  3.2  GraphPathFinder  – Bridge table injection (NetworkX fallback / OM lineage)
  3.3  TableSelector    – → 3-8 tables (LLM)
  3.4  ColumnPruner     – → ≤15 columns/table with Content Awareness (LLM)
  3.5  QualityCheck     – Warn on failing DQ tests (OM, NEW)
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
        om_client: Any = None,
    ):
        self.openai = openai
        self.pgvector = pgvector
        self.om_client = om_client  # OpenMetadataMCPClient (optional)

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
        om_table_candidates: list[dict] | None = None,
    ) -> dict:
        """
        Run 4-step schema selection with optional OM integration.

        Returns:
            {
                "candidate_columns": [...],
                "selected_tables": [...],
                "pruned_schema": "TABLE: ...\n  - col (type)...",
                "join_paths": [...],
                "quality_warnings": [...],  # NEW: from OpenMetadata DQ
            }
        """
        # ── Step 1: Column Filtering ──────────────────────────────────
        candidate_columns = await self.column_filter.filter(
            question, entity_matches, glossary_matches, similar_queries,
        )

        # Supplement with OM semantic search results if available
        if om_table_candidates:
            candidate_columns = self._merge_om_candidates(
                candidate_columns, om_table_candidates
            )

        # ── Step 2: Graph Path Discovery (bridge tables) ─────────────
        # Try OM lineage first, fall back to local NetworkX graph
        om_available = await self._check_om_available()

        if om_available:
            expanded_tables, graph_join_paths = await self._om_graph_expansion(
                candidate_columns
            )
        else:
            expanded_tables = self.graph_finder.expand_tables(candidate_columns)
            graph_join_paths = self.graph_finder.find_join_paths(expanded_tables)

        # ── Step 3: Table Selection ───────────────────────────────────
        selected_tables, join_paths = await self.table_selector.select(
            question, expanded_tables, graph_join_paths,
        )

        # ── Step 3b: Quality Warnings (OM, if available) ─────────────
        quality_warnings = []
        if om_available and selected_tables:
            quality_warnings = await self._get_quality_warnings(selected_tables)

        # ── Step 4: Column Pruning ────────────────────────────────────
        # Enrich column catalog with OM metadata if available
        column_catalog = self._full_column_catalog
        if om_available and selected_tables:
            column_catalog = await self._enrich_column_catalog(
                selected_tables, column_catalog
            )

        pruned_schema = await self.column_pruner.prune(
            question,
            selected_tables,
            content_awareness,
            doc_snippets,
            full_column_catalog=column_catalog,
        )

        # Append quality warnings to pruned schema if any
        if quality_warnings:
            warning_lines = ["\n-- DATA QUALITY WARNINGS:"]
            for w in quality_warnings:
                warning_lines.append(
                    f"-- ⚠️ {w['table']}: {w['failing_tests']} failing test(s)"
                )
                for d in w.get("details", []):
                    warning_lines.append(
                        f"--   • {d.get('test', 'unknown')}: {d.get('message', '')}"
                    )
            pruned_schema += "\n".join(warning_lines)

        logger.info(
            "Schema selection done: %d candidate cols → %d tables → pruned"
            " (OM=%s, warnings=%d)",
            len(candidate_columns),
            len(selected_tables),
            om_available,
            len(quality_warnings),
        )

        return {
            "candidate_columns": candidate_columns,
            "selected_tables": selected_tables,
            "pruned_schema": pruned_schema,
            "join_paths": join_paths,
            "quality_warnings": quality_warnings,
        }

    # ── OpenMetadata integration helpers ───────────────────────────────

    async def _check_om_available(self) -> bool:
        """Check if OpenMetadata client is configured and available."""
        if not self.om_client:
            return False
        try:
            return await self.om_client.is_available()
        except Exception:
            return False

    def _merge_om_candidates(self, candidate_columns: list[str],
                             om_tables: list[dict]) -> list[str]:
        """Merge OM semantic search results into candidate column list."""
        existing_tables = set()
        for col in candidate_columns:
            if "." in col:
                table = col.rsplit(".", 1)[0]
                existing_tables.add(table)

        added = 0
        for t in om_tables:
            fqn = t.get("fqn", "")
            if fqn and fqn not in existing_tables:
                # Add table with a placeholder column ref
                # The ColumnPruner will resolve actual columns later
                columns = t.get("columns", [])
                if columns:
                    for col in columns[:3]:  # Add top 3 columns as candidates
                        col_name = col.get("name", "") if isinstance(col, dict) else str(col)
                        if col_name:
                            candidate_columns.append(f"{fqn}.{col_name}")
                            added += 1
                else:
                    candidate_columns.append(f"{fqn}.*")
                    added += 1
                existing_tables.add(fqn)

        if added:
            logger.debug("Merged %d OM candidate columns into column filter results", added)
        return candidate_columns

    async def _om_graph_expansion(self, candidate_columns: list[str]) -> tuple[list[str], list[dict]]:
        """Use OM lineage for bridge table discovery (replaces NetworkX)."""
        candidate_tables = GraphPathFinder._extract_tables(candidate_columns)

        if len(candidate_tables) < 2:
            return candidate_tables, []

        try:
            # Use OM client's composite method
            expanded_set = await self.om_client.find_bridge_tables(candidate_tables)
            expanded = sorted(expanded_set)

            # Build join paths from OM column lineage
            join_paths = []
            from itertools import combinations
            for t1, t2 in list(combinations(candidate_tables, 2))[:10]:
                try:
                    col_lineage = await self.om_client.get_column_lineage(t1, t2)
                    if col_lineage:
                        for mapping in col_lineage:
                            from_cols = mapping.get("fromColumns", [])
                            to_cols = mapping.get("toColumns", [])
                            join_keys = list(zip(from_cols, to_cols))
                            if join_keys:
                                join_paths.append({
                                    "from_table": t1,
                                    "to_table": t2,
                                    "path": [t1, t2],
                                    "join_keys": join_keys,
                                    "source": "openmetadata",
                                })
                except Exception:
                    pass

            logger.info(
                "OM lineage expansion: %d candidate → %d expanded, %d join paths",
                len(candidate_tables), len(expanded), len(join_paths),
            )

            # If OM found fewer results, supplement with local graph
            if len(expanded) <= len(candidate_tables):
                local_expanded = self.graph_finder.expand_tables(candidate_columns)
                local_paths = self.graph_finder.find_join_paths(local_expanded)
                expanded = sorted(set(expanded) | set(local_expanded))
                # Append local paths that don't duplicate OM paths
                seen_pairs = {
                    (p["from_table"], p["to_table"]) for p in join_paths
                }
                for lp in local_paths:
                    pair = (lp["from_table"], lp["to_table"])
                    if pair not in seen_pairs:
                        lp["source"] = "local_graph"
                        join_paths.append(lp)

            return expanded, join_paths

        except Exception as exc:
            logger.warning("OM lineage failed, falling back to local graph: %s", exc)
            expanded = self.graph_finder.expand_tables(candidate_columns)
            paths = self.graph_finder.find_join_paths(expanded)
            return expanded, paths

    async def _get_quality_warnings(self, selected_tables: list[str]) -> list[dict]:
        """Check quality status for selected tables via OM."""
        try:
            return await self.om_client.get_quality_warnings(selected_tables)
        except Exception as exc:
            logger.debug("OM quality check failed: %s", exc)
            return []

    async def _enrich_column_catalog(
        self, selected_tables: list[str],
        existing_catalog: dict[str, list[dict]] | None,
    ) -> dict[str, list[dict]]:
        """Enrich column catalog with live OM column metadata."""
        catalog = dict(existing_catalog) if existing_catalog else {}

        # Only fetch for tables not already in catalog
        missing = [t for t in selected_tables if t not in catalog]
        if not missing:
            return catalog

        try:
            import asyncio
            results = await asyncio.gather(*[
                self.om_client.get_table_by_fqn(fqn)
                for fqn in missing[:10]  # limit concurrent calls
            ], return_exceptions=True)

            for fqn, result in zip(missing, results):
                if isinstance(result, Exception) or not result:
                    continue
                columns = result.columns if hasattr(result, "columns") else []
                if columns:
                    catalog[fqn] = [
                        {
                            "name": c.get("name", ""),
                            "type": c.get("data_type", "unknown"),
                            "description": c.get("description", ""),
                            "tags": c.get("tags", []),
                            "constraint": c.get("constraint", ""),
                            "is_pii": c.get("is_pii", False),
                        }
                        for c in columns
                    ]

            logger.debug("Enriched column catalog with OM data for %d tables", len(missing))
        except Exception:
            logger.debug("OM column catalog enrichment failed", exc_info=True)

        return catalog

    # ── Hot-swap helpers ───────────────────────────────────────────────

    def set_graph(self, graph: Any) -> None:
        """Set the NetworkX table-relationship graph."""
        self.graph_finder.set_graph(graph)

    def set_column_catalog(self, catalog: dict[str, list[dict]]) -> None:
        """Set the full column catalog for column pruning."""
        self._full_column_catalog = catalog

    def set_om_client(self, om_client: Any) -> None:
        """Set or replace the OpenMetadata MCP client."""
        self.om_client = om_client
