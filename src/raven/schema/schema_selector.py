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
import re
from typing import Any

from ..connectors.openai_client import OpenAIClient
from ..connectors.pgvector_store import PgVectorStore
from ..semantic_assets import SemanticModelStore
from .column_filter import ColumnFilter
from .deterministic_linker import DeterministicLinker
from .graph_path_finder import GraphPathFinder
from .join_policy import JoinPolicy
from .table_selector import TableSelector
from .column_pruner import ColumnPruner

logger = logging.getLogger(__name__)

_SQL_ALIAS_RE = re.compile(
    r"\b(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_.]*)"
    r"(?:\s+(?:AS\s+)?([a-zA-Z_][a-zA-Z0-9_]*))?",
    re.IGNORECASE,
)
_QUALIFIED_REF_RE = re.compile(
    r"\b([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)\b"
)
_IDENTIFIER_RE = re.compile(r"\b([a-zA-Z_][a-zA-Z0-9_]*)\b")
_SQL_STOPWORDS = {
    "and",
    "array",
    "as",
    "asc",
    "avg",
    "by",
    "case",
    "cast",
    "count",
    "current_date",
    "date",
    "date_trunc",
    "day",
    "desc",
    "distinct",
    "else",
    "end",
    "extract",
    "false",
    "from",
    "group",
    "having",
    "in",
    "interval",
    "join",
    "left",
    "like",
    "limit",
    "max",
    "min",
    "month",
    "not",
    "null",
    "on",
    "or",
    "order",
    "over",
    "partition",
    "right",
    "row_number",
    "select",
    "sum",
    "then",
    "true",
    "try_cast",
    "week",
    "when",
    "where",
    "year",
}


class SchemaSelector:
    """Stage 3 orchestrator — run the 4-step schema selection pipeline."""

    def __init__(
        self,
        openai: OpenAIClient,
        pgvector: PgVectorStore,
        graph: Any = None,
        om_client: Any = None,
        semantic_store: SemanticModelStore | None = None,
    ):
        self.openai = openai
        self.pgvector = pgvector
        self.om_client = om_client  # OpenMetadataMCPClient (optional)
        self.semantic_store = semantic_store or SemanticModelStore()

        # Sub-modules
        self.column_filter = ColumnFilter(openai, pgvector)
        self.graph_finder = GraphPathFinder(graph)
        self.join_policy = JoinPolicy(self.semantic_store, graph=graph)
        self.deterministic_linker = DeterministicLinker(self.join_policy)
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
        preferred_tables: list[str] | None = None,
        metabase_evidence: list[dict] | None = None,
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
        candidate_columns = self._merge_semantic_candidates(
            candidate_columns,
            glossary_matches=glossary_matches,
            similar_queries=similar_queries,
            preferred_tables=preferred_tables or [],
            metabase_evidence=metabase_evidence or [],
        )
        candidate_columns = self._merge_preferred_tables(
            candidate_columns,
            preferred_tables or [],
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

        # ── Step 3: Deterministic Linking (preferred) ────────────────
        deterministic = self.deterministic_linker.select(
            glossary_matches=glossary_matches,
            similar_queries=similar_queries,
            preferred_tables=preferred_tables or [],
            metabase_evidence=metabase_evidence or [],
            om_table_candidates=om_table_candidates or [],
            candidate_columns=candidate_columns,
            expanded_tables=expanded_tables,
        )
        selected_tables = deterministic.get("selected_tables", [])
        join_paths: list[Any] = deterministic.get("join_paths", [])

        # ── Step 4: LLM fallback if deterministic linker is inconclusive ──
        if not selected_tables:
            selected_tables, join_paths = await self.table_selector.select(
                question, expanded_tables, graph_join_paths,
            )
            if not selected_tables and preferred_tables:
                selected_tables = list(dict.fromkeys(preferred_tables))[:8]
                logger.info(
                    "Schema selection fallback: using %d preferred tables",
                    len(selected_tables),
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
            required_columns=self._required_columns_for_pruning(
                selected_tables=selected_tables,
                glossary_matches=glossary_matches,
                join_paths=join_paths,
            ),
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
        ranked_tables = sorted(
            om_tables,
            key=lambda item: (
                item.get("quality_status") == "PASS",
                item.get("score", 0.0),
            ),
            reverse=True,
        )
        for t in ranked_tables:
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

    def _merge_preferred_tables(
        self,
        candidate_columns: list[str],
        preferred_tables: list[str],
    ) -> list[str]:
        """Inject trusted tables from semantic assets / focus context."""
        if not preferred_tables:
            return candidate_columns

        merged = list(candidate_columns)
        known_tables = {
            col.rsplit(".", 1)[0]
            for col in candidate_columns
            if "." in col
        }

        for table in preferred_tables:
            if table in known_tables:
                continue

            columns = (self._full_column_catalog or {}).get(table, [])
            if columns:
                for col in columns[:5]:
                    col_name = col.get("name") or col.get("column_name") or ""
                    if col_name:
                        merged.append(f"{table}.{col_name}")
            else:
                merged.append(f"{table}.*")
            known_tables.add(table)

        return merged

    def _merge_semantic_candidates(
        self,
        candidate_columns: list[str],
        *,
        glossary_matches: list[dict[str, Any]],
        similar_queries: list[dict[str, Any]],
        preferred_tables: list[str],
        metabase_evidence: list[dict[str, Any]],
    ) -> list[str]:
        """Inject contract-backed columns before graph expansion/LLM fallback."""
        merged = list(candidate_columns)
        known = set(candidate_columns)
        available_tables = self._tables_in_scope(
            glossary_matches=glossary_matches,
            similar_queries=similar_queries,
            preferred_tables=preferred_tables,
            metabase_evidence=metabase_evidence,
        )

        def add(ref: str) -> None:
            if not ref or ref in known:
                return
            merged.append(ref)
            known.add(ref)

        for match in glossary_matches:
            table = self.semantic_store.resolve_table_name(
                str(match.get("table", "")),
                candidates=available_tables or None,
            )
            field_name = str(match.get("field_name", ""))
            kind = str(match.get("kind", ""))
            if not (table and field_name):
                continue

            if kind in {"dimension", "time_dimension"}:
                add(f"{table}.{field_name}")
            elif kind == "metric":
                for ref in self._metric_candidate_columns(table, field_name):
                    add(ref)

        for asset in [*similar_queries, *metabase_evidence]:
            tables = list(asset.get("tables_used", []) or asset.get("tables", []) or [])
            sql = str(asset.get("sql", ""))
            if not tables or not sql:
                continue
            for ref in self._sql_candidate_columns(sql, tables):
                add(ref)

        return merged

    def _tables_in_scope(
        self,
        *,
        glossary_matches: list[dict[str, Any]],
        similar_queries: list[dict[str, Any]],
        preferred_tables: list[str],
        metabase_evidence: list[dict[str, Any]],
    ) -> set[str]:
        tables = {
            self.semantic_store.resolve_table_name(str(table))
            for table in preferred_tables
            if table
        }
        for match in glossary_matches:
            table = str(match.get("table", ""))
            if table:
                tables.add(self.semantic_store.resolve_table_name(table))
        for asset in [*similar_queries, *metabase_evidence]:
            for table in asset.get("tables_used", []) or asset.get("tables", []) or []:
                if table:
                    tables.add(self.semantic_store.resolve_table_name(str(table)))
        return {table for table in tables if table}

    def _metric_candidate_columns(self, table: str, metric_name: str) -> list[str]:
        asset = self.semantic_store.get_table_asset(table)
        if not asset:
            return []
        for metric in asset.metrics:
            if str(metric.get("name", "")) != str(metric_name):
                continue
            sql = str(metric.get("sql", ""))
            return self._sql_candidate_columns(sql, [table])
        return []

    def _sql_candidate_columns(self, sql: str, tables: list[str]) -> list[str]:
        resolved_tables = [
            self.semantic_store.resolve_table_name(str(table))
            for table in tables
            if table
        ]
        resolved_tables = list(dict.fromkeys(table for table in resolved_tables if table))
        if not sql or not resolved_tables:
            return []

        alias_lookup = self._sql_alias_lookup(sql, resolved_tables)
        refs: list[str] = []
        seen: set[str] = set()

        def add(ref: str) -> None:
            if ref and ref not in seen:
                refs.append(ref)
                seen.add(ref)

        for prefix, column in _QUALIFIED_REF_RE.findall(sql):
            table = alias_lookup.get(prefix)
            if table:
                add(f"{table}.{column}")

        catalog_columns = self._catalog_columns(resolved_tables)
        if not catalog_columns:
            return refs

        aliases = set(alias_lookup)
        for token in _IDENTIFIER_RE.findall(sql):
            lowered = token.lower()
            if lowered in _SQL_STOPWORDS or token in aliases:
                continue
            matches = [
                table
                for table, columns in catalog_columns.items()
                if token in columns
            ]
            if len(matches) == 1:
                add(f"{matches[0]}.{token}")

        return refs

    def _sql_alias_lookup(
        self,
        sql: str,
        tables: list[str],
    ) -> dict[str, str]:
        resolved_tables = [
            self.semantic_store.resolve_table_name(table, candidates=set(tables))
            for table in tables
        ]
        lookup: dict[str, str] = {}

        for table in resolved_tables:
            for alias in self.semantic_store.table_aliases(table):
                lookup.setdefault(alias, table)

        for table_name, alias in _SQL_ALIAS_RE.findall(sql):
            resolved = self.semantic_store.resolve_table_name(
                table_name,
                candidates=set(resolved_tables),
            )
            if not resolved:
                continue
            for key in self.semantic_store.table_aliases(table_name):
                lookup.setdefault(key, resolved)
            if alias:
                lookup.setdefault(alias, resolved)

        return lookup

    def _catalog_columns(self, tables: list[str]) -> dict[str, set[str]]:
        catalog = self._full_column_catalog or {}
        result: dict[str, set[str]] = {}
        for table in tables:
            columns = catalog.get(table, [])
            if not columns:
                continue
            result[table] = {
                str(col.get("name") or col.get("column_name") or "")
                for col in columns
                    if col.get("name") or col.get("column_name")
            }
        return result

    def _required_columns_for_pruning(
        self,
        *,
        selected_tables: list[str],
        glossary_matches: list[dict[str, Any]],
        join_paths: list[Any],
    ) -> list[str]:
        selected = {
            self.semantic_store.resolve_table_name(table, candidates=set(selected_tables))
            for table in selected_tables
            if table
        }
        required: list[str] = []
        seen: set[str] = set()

        def add(ref: str) -> None:
            if not ref or ref in seen:
                return
            table = ref.rsplit(".", 1)[0] if "." in ref else ""
            if table and selected and table not in selected:
                return
            required.append(ref)
            seen.add(ref)

        for match in glossary_matches:
            table = self.semantic_store.resolve_table_name(
                str(match.get("table", "")),
                candidates=selected or None,
            )
            field_name = str(match.get("field_name", ""))
            kind = str(match.get("kind", ""))
            if not (table and field_name):
                continue
            if kind in {"dimension", "time_dimension"}:
                add(f"{table}.{field_name}")
            elif kind == "metric":
                for ref in self._metric_candidate_columns(table, field_name):
                    add(ref)

        for ref in self._join_path_columns(join_paths, selected):
            add(ref)

        return required

    def _join_path_columns(
        self,
        join_paths: list[Any],
        selected_tables: set[str],
    ) -> list[str]:
        refs: list[str] = []
        seen: set[str] = set()

        def add(ref: str) -> None:
            if not ref or ref in seen:
                return
            table = ref.rsplit(".", 1)[0] if "." in ref else ""
            if selected_tables and table not in selected_tables:
                return
            refs.append(ref)
            seen.add(ref)

        for path in join_paths:
            if not isinstance(path, dict):
                continue
            for edge in path.get("edges", []) or []:
                for ref in self._refs_from_condition(str(edge.get("condition_sql", ""))):
                    add(ref)
            for left, right in path.get("join_keys", []) or []:
                for ref in (str(left), str(right)):
                    resolved = self._resolve_column_ref(ref, selected_tables)
                    if resolved:
                        add(resolved)

        return refs

    def _refs_from_condition(self, condition_sql: str) -> list[str]:
        refs: list[str] = []
        seen: set[str] = set()
        for prefix, column in _QUALIFIED_REF_RE.findall(condition_sql or ""):
            resolved = self._resolve_column_ref(f"{prefix}.{column}", None)
            if resolved and resolved not in seen:
                refs.append(resolved)
                seen.add(resolved)
        return refs

    def _resolve_column_ref(
        self,
        ref: str,
        selected_tables: set[str] | None,
    ) -> str:
        if "." not in ref:
            return ""
        table_ref, column = ref.rsplit(".", 1)
        resolved_table = self.semantic_store.resolve_table_name(
            table_ref,
            candidates=selected_tables or None,
        )
        if not (resolved_table and column):
            return ""
        return f"{resolved_table}.{column}"

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
        self.join_policy.set_graph(graph)

    def set_column_catalog(self, catalog: dict[str, list[dict]]) -> None:
        """Set the full column catalog for column pruning."""
        self._full_column_catalog = catalog

    def set_om_client(self, om_client: Any) -> None:
        """Set or replace the OpenMetadata MCP client."""
        self.om_client = om_client
