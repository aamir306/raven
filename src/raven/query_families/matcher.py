"""
Trusted query-family matcher for verified queries and Metabase cards.
"""

from __future__ import annotations

import re
from typing import Any

from .compiler import QueryFamilyCompiler

_TOKEN_RE = re.compile(r"[a-z0-9_<>]+")
_TOP_BOTTOM_RE = re.compile(r"\b(top|bottom)\s+(\d+)\b", re.IGNORECASE)
_LAST_N_RE = re.compile(
    r"\b(last|past)\s+(\d+)\s+(day|days|week|weeks|month|months|year|years)\b",
    re.IGNORECASE,
)
_TIME_GRAIN_PATTERNS = [
    (re.compile(r"\b(daily|day-wise|day wise|per day)\b", re.IGNORECASE), "<grain>"),
    (re.compile(r"\b(weekly|week-wise|week wise|per week)\b", re.IGNORECASE), "<grain>"),
    (re.compile(r"\b(monthly|month-wise|month wise|per month)\b", re.IGNORECASE), "<grain>"),
    (re.compile(r"\b(yearly|year-wise|year wise|per year)\b", re.IGNORECASE), "<grain>"),
]
_SQL_FILTER_RE = re.compile(
    r"(?P<column>(?:[a-zA-Z_][a-zA-Z0-9_]*\.)?[a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*(?P<literal>'(?:[^']|'')*'|-?\d+(?:\.\d+)?|true|false)(?=\s|,|\)|$)",
    re.IGNORECASE,
)
_STOPWORDS = {
    "a",
    "an",
    "are",
    "for",
    "how",
    "is",
    "many",
    "me",
    "show",
    "the",
    "there",
    "what",
}


def _singular(value: str) -> str:
    text = str(value or "").lower()
    return text[:-1] if text.endswith("s") else text


def _normalize_question(question: str) -> str:
    text = " ".join(str(question or "").lower().split())
    text = _TOP_BOTTOM_RE.sub("<rank> <number>", text)
    text = _LAST_N_RE.sub(lambda m: f"{m.group(1).lower()} <n> {_singular(m.group(3))}", text)
    for pattern, replacement in _TIME_GRAIN_PATTERNS:
        text = pattern.sub(replacement, text)
    tokens = [
        _singular(token)
        for token in _TOKEN_RE.findall(text)
        if token not in _STOPWORDS
    ]
    return " ".join(tokens)


def _extract_slots(question: str) -> dict[str, Any]:
    slots: dict[str, Any] = {}
    top_bottom = _TOP_BOTTOM_RE.search(question)
    if top_bottom:
        slots["limit"] = int(top_bottom.group(2))
        slots["order_direction"] = "DESC" if top_bottom.group(1).lower() == "top" else "ASC"

    interval = _LAST_N_RE.search(question)
    if interval:
        slots["interval"] = {
            "value": int(interval.group(2)),
            "unit": _singular(interval.group(3)),
        }

    question_lower = str(question or "").lower()
    if any(token in question_lower for token in ("daily", "day-wise", "day wise", "per day")):
        slots["time_grain"] = "day"
    elif any(token in question_lower for token in ("weekly", "week-wise", "week wise", "per week")):
        slots["time_grain"] = "week"
    elif any(token in question_lower for token in ("monthly", "month-wise", "month wise", "per month")):
        slots["time_grain"] = "month"
    elif any(token in question_lower for token in ("yearly", "year-wise", "year wise", "per year")):
        slots["time_grain"] = "year"

    return slots


def _normalize_with_phrases(question: str, phrases: list[str]) -> str:
    text = " ".join(str(question or "").lower().split())
    for phrase in sorted(
        {str(item).strip().lower() for item in phrases if str(item).strip()},
        key=len,
        reverse=True,
    ):
        text = re.sub(rf"\b{re.escape(phrase)}\b", "<value>", text)
    return _normalize_question(text)


def _parse_literal(literal: str) -> Any:
    text = str(literal or "").strip()
    if not text:
        return ""
    lowered = text.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if text.startswith("'") and text.endswith("'"):
        return text[1:-1].replace("''", "'")
    try:
        if "." in text:
            return float(text)
        return int(text)
    except ValueError:
        return text


def _extract_sql_filters(sql: str) -> list[dict[str, Any]]:
    filters: list[dict[str, Any]] = []
    for match in _SQL_FILTER_RE.finditer(str(sql or "")):
        column_ref = str(match.group("column") or "").strip()
        literal = str(match.group("literal") or "").strip()
        if not column_ref or not literal:
            continue
        table = ""
        column = column_ref
        if "." in column_ref:
            table, column = column_ref.rsplit(".", 1)
        filters.append(
            {
                "column_ref": column_ref,
                "table": table,
                "column": column,
                "literal": literal,
                "value": _parse_literal(literal),
            }
        )
    return filters


def _filter_aliases(table: str, column: str, column_ref: str = "") -> list[str]:
    aliases: list[str] = []
    raw_column_ref = str(column_ref or "").strip()
    if raw_column_ref:
        aliases.append(raw_column_ref)
    raw_column = str(column or "").strip()
    raw_table = str(table or "").strip()
    if raw_table and raw_column:
        aliases.append(f"{raw_table}.{raw_column}")
    if raw_column:
        aliases.append(raw_column)
    return list(dict.fromkeys(alias for alias in aliases if alias))


def _resolved_filter_phrases(item: dict[str, Any]) -> list[str]:
    phrases = []
    matched_text = str(item.get("matched_text", "")).strip()
    value = item.get("value")
    if matched_text:
        phrases.append(matched_text)
    if value not in (None, ""):
        phrases.append(str(value))
    return list(dict.fromkeys(phrases))


def _match_filter_replacements(
    asset: dict[str, Any],
    resolved_filters: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[str]]:
    asset_filters = _extract_sql_filters(asset.get("sql", ""))
    replacements: list[dict[str, Any]] = []
    candidate_question_phrases: list[str] = []
    used_keys: set[tuple[str, str]] = set()

    for current in resolved_filters:
        column = str(current.get("column", "")).strip()
        value = current.get("value")
        if not column or value in (None, ""):
            continue

        current_aliases = set(
            _filter_aliases(
                table=str(current.get("table", "")),
                column=column,
            )
        )
        if not current_aliases:
            continue

        matching_asset = None
        for asset_filter in asset_filters:
            asset_aliases = set(
                _filter_aliases(
                    table=str(asset_filter.get("table", "")),
                    column=str(asset_filter.get("column", "")),
                    column_ref=str(asset_filter.get("column_ref", "")),
                )
            )
            if current_aliases & asset_aliases:
                matching_asset = asset_filter
                break

        if not matching_asset:
            continue

        key = (
            str(matching_asset.get("column_ref", "")),
            str(matching_asset.get("literal", "")),
        )
        if key in used_keys:
            continue
        used_keys.add(key)

        candidate_question_phrases.append(str(matching_asset.get("value", "")))
        replacements.append(
            {
                "column_aliases": _filter_aliases(
                    table=str(matching_asset.get("table", "")),
                    column=str(matching_asset.get("column", "")),
                    column_ref=str(matching_asset.get("column_ref", "")),
                ),
                "old_literal": matching_asset.get("literal", ""),
                "value": value,
            }
        )

    return replacements, candidate_question_phrases


class QueryFamilyMatcher:
    """Find conservative query-family matches and compile trusted SQL."""

    def __init__(self):
        self.compiler = QueryFamilyCompiler()

    def match(
        self,
        *,
        question: str,
        verified_queries: list[dict[str, Any]],
        metabase_evidence: list[dict[str, Any]],
        resolved_filters: list[dict[str, Any]] | None = None,
        question_dimension_phrases: list[str] | None = None,
        question_metric_phrases: list[str] | None = None,
    ) -> dict[str, Any] | None:
        resolved_filters = list(resolved_filters or [])
        question_value_phrases: list[str] = []
        for item in resolved_filters:
            question_value_phrases.extend(_resolved_filter_phrases(item))
        question_dimension_phrases = list(question_dimension_phrases or [])
        question_metric_phrases = list(question_metric_phrases or [])

        question_norm = _normalize_with_phrases(
            question,
            [*question_value_phrases, *question_dimension_phrases, *question_metric_phrases],
        )
        question_slots = _extract_slots(question)
        if not question_norm:
            return None

        best_match: dict[str, Any] | None = None
        for asset in self._candidate_assets(verified_queries, metabase_evidence):
            if asset.get("exact_match"):
                continue

            filter_replacements, candidate_value_phrases = _match_filter_replacements(
                asset,
                resolved_filters,
            )
            family_norm = _normalize_with_phrases(
                asset["question"],
                [
                    *candidate_value_phrases,
                    *list(asset.get("dimension_question_phrases", []) or []),
                    *list(asset.get("metric_question_phrases", []) or []),
                ],
            )
            if family_norm != question_norm:
                continue

            compiled_sql = self.compiler.compile(
                asset["sql"],
                question_slots,
                filter_replacements=filter_replacements,
                metric_replacements=list(asset.get("metric_replacements", []) or []),
                join_replacements=list(asset.get("join_replacements", []) or []),
                dimension_replacements=list(asset.get("dimension_replacements", []) or []),
            )
            if not compiled_sql:
                continue

            score = float(asset.get("score", asset.get("similarity", 0.0)))
            if question_slots:
                score += 0.10
            if filter_replacements:
                score += 0.10
            if asset.get("metric_replacements"):
                score += 0.10
            if asset.get("join_replacements"):
                score += 0.10
            if asset.get("dimension_replacements"):
                score += 0.10

            candidate = {
                "question": asset["question"],
                "sql": compiled_sql,
                "template_sql": asset["sql"],
                "tables_used": list(asset.get("tables_used", [])),
                "source": asset.get("source", "semantic_model"),
                "metadata": dict(asset.get("metadata", {})),
                "slots": question_slots,
                "filter_replacements": filter_replacements,
                "metric_replacements": list(asset.get("metric_replacements", []) or []),
                "join_replacements": list(asset.get("join_replacements", []) or []),
                "dimension_replacements": list(asset.get("dimension_replacements", []) or []),
                "similarity": min(score, 1.0),
                "family_key": family_norm,
            }
            if best_match is None or candidate["similarity"] > best_match["similarity"]:
                best_match = candidate

        if best_match and best_match["similarity"] >= 0.45:
            return best_match
        return None

    @staticmethod
    def _candidate_assets(
        verified_queries: list[dict[str, Any]],
        metabase_evidence: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        assets: list[dict[str, Any]] = []
        for item in verified_queries:
            if item.get("question") and item.get("sql"):
                assets.append(
                    {
                        "question": item["question"],
                        "sql": item["sql"],
                        "tables_used": item.get("tables_used", []),
                        "source": item.get("source", "semantic_model"),
                        "metadata": item.get("metadata", {}),
                        "similarity": float(item.get("similarity", 0.0)),
                        "exact_match": bool(item.get("exact_match", False)),
                        "dimension_question_phrases": list(item.get("dimension_question_phrases", []) or []),
                        "metric_question_phrases": list(item.get("metric_question_phrases", []) or []),
                        "metric_replacements": list(item.get("metric_replacements", []) or []),
                        "join_replacements": list(item.get("join_replacements", []) or []),
                        "dimension_replacements": list(item.get("dimension_replacements", []) or []),
                    }
                )

        for item in metabase_evidence:
            if item.get("kind") != "metabase_card":
                continue
            question = str(item.get("name", ""))
            sql = str(item.get("sql", ""))
            if not (question and sql):
                continue
            assets.append(
                {
                    "question": question,
                    "sql": sql,
                    "tables_used": item.get("tables", []),
                    "source": item.get("source", "metabase"),
                    "metadata": {
                        "focus_name": item.get("focus_name"),
                    },
                    "score": float(item.get("score", 0.0)) + 0.05,
                    "exact_match": False,
                    "dimension_question_phrases": list(item.get("dimension_question_phrases", []) or []),
                    "metric_question_phrases": list(item.get("metric_question_phrases", []) or []),
                    "metric_replacements": list(item.get("metric_replacements", []) or []),
                    "join_replacements": list(item.get("join_replacements", []) or []),
                    "dimension_replacements": list(item.get("dimension_replacements", []) or []),
                }
            )

        return assets
