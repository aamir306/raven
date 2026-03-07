"""
Deterministic value grounding for the accuracy-first path.

This module turns semantic enums, content-awareness sample values, Metabase
filters, and business-rule fragments into grounded filter hints before SQL
generation.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
import re
from typing import Any

from ..semantic_assets import SemanticModelStore

_TOKEN_RE = re.compile(r"[a-z0-9_]+")


def _normalize(text: str) -> str:
    return " ".join(str(text).lower().split())


def _contains_phrase(question: str, phrase: str) -> bool:
    norm_phrase = _normalize(phrase)
    return bool(norm_phrase and norm_phrase in question)


def _is_filter_rule(sql_fragment: str) -> bool:
    fragment = (sql_fragment or "").lower().strip()
    if not fragment:
        return False
    if "order by" in fragment and "limit" in fragment:
        return False
    if re.search(r"(?:^|[^a-z])(sum|avg|count|min|max|approx_percentile)\s*\(", fragment):
        return False
    return any(op in fragment for op in (" = ", " ='", ">=", "<=", " !=", " in ", " like "))


def _quote(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


@dataclass(frozen=True)
class ResolvedFilter:
    table: str = ""
    column: str = ""
    operator: str = "="
    value: Any | None = None
    sql_expression: str = ""
    source: str = ""
    confidence: float = 0.0
    matched_text: str = ""

    def to_sql(self) -> str:
        if self.sql_expression:
            return self.sql_expression
        if not self.column:
            return ""
        column_ref = self.column
        if self.table and "." not in column_ref:
            column_ref = f"{self.table}.{column_ref}"
        return f"{column_ref} {self.operator} {_quote(self.value)}"

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["sql"] = self.to_sql()
        return payload


@dataclass
class GroundingResult:
    filters: list[ResolvedFilter] = field(default_factory=list)
    ambiguities: list[dict[str, Any]] = field(default_factory=list)
    matched_rules: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "filters": [item.to_dict() for item in self.filters],
            "ambiguities": list(self.ambiguities),
            "matched_rules": list(self.matched_rules),
        }


class ValueResolver:
    """Resolve grounded filters from semantic rules, enums, and metadata."""

    def __init__(self, semantic_store: SemanticModelStore):
        self.semantic_store = semantic_store

    def resolve(
        self,
        *,
        question: str,
        content_awareness: list[dict[str, Any]],
        preferred_tables: list[str],
        instruction_matches: list[dict[str, Any]],
        focus: Any | None = None,
    ) -> GroundingResult:
        question_norm = _normalize(question)
        filters: list[ResolvedFilter] = []
        ambiguities: list[dict[str, Any]] = []

        # 1. Semantic rule fragments become trusted filter hints.
        matched_rules = [m for m in instruction_matches if _is_filter_rule(m.get("sql_fragment", ""))]
        for match in matched_rules:
            filters.append(
                ResolvedFilter(
                    sql_expression=match.get("sql_fragment", ""),
                    source=match.get("source", "semantic_model"),
                    confidence=min(match.get("similarity", 0.0) + 0.15, 1.0),
                    matched_text=match.get("term", ""),
                )
            )

        # 2. Enumerated semantic values on preferred tables.
        preferred = set(preferred_tables)
        for table in self.semantic_store.table_assets:
            if preferred and table.name not in preferred:
                continue
            for dimension in (*table.dimensions, *table.time_dimensions):
                column = str(dimension.get("name", ""))
                for value in dimension.get("values", []) or []:
                    if not _contains_phrase(question_norm, str(value)):
                        continue
                    filters.append(
                        ResolvedFilter(
                            table=table.name,
                            column=column,
                            operator="=",
                            value=value,
                            source="semantic_enum",
                            confidence=0.90,
                            matched_text=str(value),
                        )
                    )

        # 3. Sample values from OM/local content awareness.
        sample_matches: dict[tuple[str, str], list[ResolvedFilter]] = {}
        for awareness in content_awareness:
            table = str(awareness.get("table", ""))
            column = str(awareness.get("column", ""))
            if preferred and table and table not in preferred:
                continue
            for value in awareness.get("sample_values", []) or []:
                normalized = _normalize(value)
                if len(normalized) < 3 or not _contains_phrase(question_norm, normalized):
                    continue
                resolved = ResolvedFilter(
                    table=table,
                    column=column,
                    operator="=",
                    value=value,
                    source=awareness.get("source", "content_awareness"),
                    confidence=0.75,
                    matched_text=str(value),
                )
                sample_matches.setdefault((table, column), []).append(resolved)

        for (table, column), matches in sample_matches.items():
            if len(matches) == 1:
                filters.append(matches[0])
                continue
            exact = [m for m in matches if _normalize(m.matched_text) == question_norm]
            if len(exact) == 1:
                filters.append(exact[0])
                continue
            ambiguities.append(
                {
                    "type": "value_match",
                    "table": table,
                    "column": column,
                    "candidates": [m.matched_text for m in matches],
                }
            )

        # 4. Metabase filter names are weak signals; record only as ambiguity hints.
        focus_filters = list(getattr(focus, "dashboard_filters", []) or [])
        for raw in focus_filters:
            filter_name = str(raw.get("name") or raw.get("slug") or "")
            if filter_name and _contains_phrase(question_norm, filter_name):
                ambiguities.append(
                    {
                        "type": "metabase_filter_reference",
                        "filter": filter_name,
                        "source": "metabase",
                    }
                )

        deduped: list[ResolvedFilter] = []
        seen: set[tuple[str, str, str]] = set()
        for item in sorted(filters, key=lambda f: f.confidence, reverse=True):
            key = (item.table, item.column, item.to_sql())
            if key in seen or not item.to_sql():
                continue
            seen.add(key)
            deduped.append(item)

        return GroundingResult(
            filters=deduped,
            ambiguities=ambiguities,
            matched_rules=matched_rules,
        )
