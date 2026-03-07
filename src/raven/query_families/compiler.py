"""
Conservative SQL compilation for trusted query families.
"""

from __future__ import annotations

import re
from typing import Any

_LIMIT_RE = re.compile(r"\bLIMIT\s+(\d+)\b", re.IGNORECASE)
_INTERVAL_RE = re.compile(
    r"INTERVAL\s+'(\d+)'\s+(DAY|DAYS|WEEK|WEEKS|MONTH|MONTHS|YEAR|YEARS)\b",
    re.IGNORECASE,
)
_ORDER_RE = re.compile(r"\bORDER\s+BY\b(?P<body>.*?)(?P<limit>\bLIMIT\b|$)", re.IGNORECASE | re.DOTALL)
_DATE_TRUNC_RE = re.compile(r"DATE_TRUNC\('(?P<grain>day|week|month|year)',\s*(?P<expr>[^)]+)\)", re.IGNORECASE)
_DATE_WRAP_RE = re.compile(r"DATE\((?P<expr>[^)]+)\)", re.IGNORECASE)


def _sql_literal(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


class QueryFamilyCompiler:
    """Apply safe slot substitutions to a trusted family SQL template."""

    def compile(
        self,
        sql: str,
        slots: dict[str, Any],
        filter_replacements: list[dict[str, Any]] | None = None,
        metric_replacements: list[dict[str, Any]] | None = None,
        join_replacements: list[dict[str, Any]] | None = None,
        dimension_replacements: list[dict[str, Any]] | None = None,
    ) -> str | None:
        compiled = str(sql or "")
        if not compiled.strip():
            return None

        limit = slots.get("limit")
        if limit is not None:
            if not _LIMIT_RE.search(compiled):
                return None
            compiled = _LIMIT_RE.sub(f"LIMIT {int(limit)}", compiled)

        interval = slots.get("interval")
        if interval:
            value = int(interval["value"])
            unit = str(interval["unit"]).upper()
            replaced = False

            def _replace(match: re.Match[str]) -> str:
                nonlocal replaced
                existing_unit = match.group(2).upper()
                existing_singular = existing_unit.rstrip("S")
                new_singular = unit.rstrip("S")
                if existing_singular != new_singular:
                    return match.group(0)
                replaced = True
                return f"INTERVAL '{value}' {existing_unit}"

            compiled = _INTERVAL_RE.sub(_replace, compiled)
            if not replaced:
                return None

        for replacement in metric_replacements or []:
            old_expr = str(replacement.get("old_metric_sql", "")).strip()
            new_expr = str(replacement.get("new_metric_sql", "")).strip()
            old_alias = str(replacement.get("old_metric_name", "")).strip()
            new_alias = str(replacement.get("new_metric_name", "")).strip()
            if not (old_expr and new_expr and old_alias and new_alias):
                return None

            select_pattern = re.compile(
                rf"{re.escape(old_expr)}\s+AS\s+{re.escape(old_alias)}\b",
                re.IGNORECASE,
            )
            compiled, select_count = select_pattern.subn(
                f"{new_expr} AS {new_alias}",
                compiled,
                count=1,
            )
            if not select_count:
                return None

            order_pattern = re.compile(
                rf"(\bORDER\s+BY\s+){re.escape(old_alias)}\b",
                re.IGNORECASE,
            )
            compiled = order_pattern.sub(rf"\1{new_alias}", compiled, count=1)

        order_direction = str(slots.get("order_direction", "")).upper().strip()
        if order_direction in {"ASC", "DESC"}:
            compiled = self._replace_order_direction(compiled, order_direction)
            if not compiled:
                return None

        time_grain = str(slots.get("time_grain", "")).lower().strip()
        if time_grain:
            compiled = self._replace_time_grain(compiled, time_grain)
            if not compiled:
                return None

        for replacement in filter_replacements or []:
            aliases = [
                str(alias).strip()
                for alias in replacement.get("column_aliases", [])
                if str(alias).strip()
            ]
            old_literal = str(replacement.get("old_literal", "")).strip()
            if not aliases or not old_literal:
                return None

            new_literal = _sql_literal(replacement.get("value"))
            replaced = False
            for alias in sorted(set(aliases), key=len, reverse=True):
                pattern = re.compile(
                    rf"(\b{re.escape(alias)}\b\s*=\s*){re.escape(old_literal)}(?=\s|,|\)|$)",
                    re.IGNORECASE,
                )
                compiled, count = pattern.subn(rf"\1{new_literal}", compiled, count=1)
                if count:
                    replaced = True
                    break

            if not replaced:
                return None

        for replacement in join_replacements or []:
            old_join_sql = str(replacement.get("old_join_sql", "")).strip()
            new_join_sql = str(replacement.get("new_join_sql", "")).strip()
            if not old_join_sql or not new_join_sql:
                return None
            if old_join_sql not in compiled:
                return None
            compiled = compiled.replace(old_join_sql, new_join_sql, 1)

        for replacement in dimension_replacements or []:
            ref_map = {
                str(old_ref).strip(): str(new_ref).strip()
                for old_ref, new_ref in (replacement.get("ref_map") or {}).items()
                if str(old_ref).strip() and str(new_ref).strip()
            }
            if not ref_map:
                return None

            replaced = False
            for old_ref, new_ref in sorted(ref_map.items(), key=lambda item: len(item[0]), reverse=True):
                pattern = re.compile(rf"\b{re.escape(old_ref)}\b", re.IGNORECASE)
                compiled, count = pattern.subn(new_ref, compiled)
                if count:
                    replaced = True

            if not replaced:
                return None

        return compiled

    @staticmethod
    def _replace_order_direction(sql: str, direction: str) -> str | None:
        match = _ORDER_RE.search(sql)
        if not match:
            return sql

        body = match.group("body")
        if not body or "," in body:
            return sql

        if re.search(r"\bASC\b|\bDESC\b", body, re.IGNORECASE):
            new_body = re.sub(r"\bASC\b|\bDESC\b", direction, body, count=1, flags=re.IGNORECASE)
        else:
            new_body = body.rstrip() + f" {direction}"

        start, end = match.span("body")
        return sql[:start] + new_body + sql[end:]

    @staticmethod
    def _replace_time_grain(sql: str, time_grain: str) -> str | None:
        normalized = time_grain.lower()
        if normalized not in {"day", "week", "month", "year"}:
            return None

        if _DATE_TRUNC_RE.search(sql):
            return _DATE_TRUNC_RE.sub(
                lambda m: f"DATE_TRUNC('{normalized}', {m.group('expr')})",
                sql,
            )

        if _DATE_WRAP_RE.search(sql):
            if normalized == "day":
                return sql
            return _DATE_WRAP_RE.sub(
                lambda m: f"DATE_TRUNC('{normalized}', {m.group('expr')})",
                sql,
            )

        return sql
