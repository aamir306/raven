"""
Deterministic table linker that ranks semantic/verified evidence before LLM fallback.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from .join_policy import JoinPolicy


class DeterministicLinker:
    """Select tables and join paths using contracts, evidence, and join policy."""

    def __init__(self, join_policy: JoinPolicy):
        self.join_policy = join_policy

    def select(
        self,
        *,
        glossary_matches: list[dict[str, Any]],
        similar_queries: list[dict[str, Any]],
        preferred_tables: list[str],
        metabase_evidence: list[dict[str, Any]],
        om_table_candidates: list[dict[str, Any]],
        candidate_columns: list[str],
        expanded_tables: list[str],
    ) -> dict[str, Any]:
        available_tables = {
            table
            for table in expanded_tables
            if table
        }
        available_tables.update(
            col.rsplit(".", 1)[0]
            for col in candidate_columns
            if "." in col
        )
        available_tables.update(item.get("fqn", "") for item in om_table_candidates)
        available_tables.update(preferred_tables)
        available_tables = {table for table in available_tables if table}

        scores: defaultdict[str, float] = defaultdict(float)
        metric_tables: list[tuple[float, str]] = []
        dimension_tables: list[tuple[float, str]] = []

        for match in glossary_matches:
            table = str(match.get("table", ""))
            if not table:
                continue
            resolved = self.join_policy.semantic_store.resolve_table_name(
                table,
                candidates=available_tables,
            )
            similarity = float(match.get("similarity", 0.0))
            kind = str(match.get("kind", ""))
            base = 0.45 if kind == "metric" else 0.25
            scores[resolved] += base + (similarity * 0.35)
            if kind == "metric":
                metric_tables.append((scores[resolved], resolved))
            elif kind in {"dimension", "time_dimension"}:
                dimension_tables.append((scores[resolved], resolved))

        for item in similar_queries:
            similarity = float(item.get("similarity", 0.0))
            bonus = 0.45 if item.get("exact_match") else 0.18 + (similarity * 0.15)
            for table in item.get("tables_used", []):
                resolved = self.join_policy.semantic_store.resolve_table_name(
                    str(table),
                    candidates=available_tables,
                )
                scores[resolved] += bonus

        for table in preferred_tables:
            resolved = self.join_policy.semantic_store.resolve_table_name(
                str(table),
                candidates=available_tables,
            )
            scores[resolved] += 0.22

        for evidence in metabase_evidence:
            evidence_score = 0.12 + min(float(evidence.get("score", 0.0)), 0.2)
            for table in evidence.get("tables", []):
                resolved = self.join_policy.semantic_store.resolve_table_name(
                    str(table),
                    candidates=available_tables,
                )
                scores[resolved] += evidence_score

        for item in om_table_candidates:
            resolved = self.join_policy.semantic_store.resolve_table_name(
                str(item.get("fqn", "")),
                candidates=available_tables,
            )
            if not resolved:
                continue
            scores[resolved] += min(float(item.get("score", 0.0)), 0.20)
            quality = str(item.get("quality_status", "UNKNOWN"))
            if quality == "PASS":
                scores[resolved] += 0.05
            elif quality == "FAIL":
                scores[resolved] -= 0.10

        for col in candidate_columns:
            if "." not in col:
                continue
            resolved = self.join_policy.semantic_store.resolve_table_name(
                col.rsplit(".", 1)[0],
                candidates=available_tables,
            )
            scores[resolved] += 0.02

        ranked_tables = sorted(scores.items(), key=lambda item: item[1], reverse=True)
        if not ranked_tables or ranked_tables[0][1] < 0.30:
            return {"selected_tables": [], "join_paths": [], "confidence": "LOW"}

        ranked_metric_tables = [table for _, table in sorted(metric_tables, reverse=True)]
        ranked_dimension_tables = [table for _, table in sorted(dimension_tables, reverse=True)]

        anchor = ranked_metric_tables[0] if ranked_metric_tables else ranked_tables[0][0]
        target_tables = [anchor]

        for table in ranked_dimension_tables:
            if table == anchor or table in target_tables:
                continue
            if len(target_tables) >= 3:
                break
            if self.join_policy.find_path(anchor, table, available_tables=available_tables):
                target_tables.append(table)

        for table, score in ranked_tables:
            if len(target_tables) >= 3:
                break
            if table in target_tables or score < 0.35:
                continue
            if self.join_policy.find_path(anchor, table, available_tables=available_tables):
                target_tables.append(table)

        selected_tables, join_paths = self.join_policy.connect_tables(
            target_tables,
            available_tables=available_tables,
        )
        if not selected_tables:
            selected_tables = [anchor]
            join_paths = []

        confidence = "HIGH" if join_paths or any(t in preferred_tables for t in selected_tables) else "MEDIUM"
        return {
            "selected_tables": selected_tables,
            "join_paths": join_paths,
            "confidence": confidence,
            "ranked_tables": ranked_tables[:8],
        }
