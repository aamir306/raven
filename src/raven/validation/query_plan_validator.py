"""
Validate SQL candidates against the intended typed query plan.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any


_TABLE_RE = re.compile(r"\b(?:from|join)\s+([a-zA-Z_][a-zA-Z0-9_.]*)", re.IGNORECASE)
_LIMIT_RE = re.compile(r"\blimit\s+(\d+)\b", re.IGNORECASE)
_ORDER_BY_RE = re.compile(r"\border\s+by\s+(.+?)(?:\blimit\b|$)", re.IGNORECASE)


def _normalize_sql(text: str) -> str:
    return " ".join(str(text).lower().split())


def _quoted(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


@dataclass
class PlanValidationResult:
    ok: bool
    violations: list[str] = field(default_factory=list)


class QueryPlanValidator:
    """Check that candidate SQL satisfies the planned tables, joins, and filters."""

    def validate(self, sql: str, query_plan: dict[str, Any] | None) -> PlanValidationResult:
        if not query_plan:
            return PlanValidationResult(ok=True, violations=[])

        sql_norm = _normalize_sql(sql)
        tables_in_sql = {table.lower() for table in _TABLE_RE.findall(sql)}
        violations: list[str] = []

        required_tables = [
            str(table).lower()
            for table in query_plan.get("source_tables", []) or [query_plan.get("table", "")]
            if table
        ]
        for table in required_tables:
            if table not in tables_in_sql:
                violations.append(f"missing_table:{table}")

        for join in query_plan.get("joins", []) or []:
            condition = _normalize_sql(join.get("condition_sql", ""))
            if condition and condition not in sql_norm:
                violations.append(f"missing_join:{condition}")

        if query_plan.get("group_by_sql"):
            group_by_sql = _normalize_sql(query_plan.get("group_by_sql", ""))
            if " group by " not in f" {sql_norm} ":
                violations.append("missing_group_by")
            elif group_by_sql and group_by_sql not in sql_norm:
                violations.append(f"missing_group_column:{group_by_sql}")

        raw_time_dimension_sql = query_plan.get("time_dimension_sql")
        time_dimension_sql = _normalize_sql(raw_time_dimension_sql) if raw_time_dimension_sql else ""
        if time_dimension_sql:
            if " group by " not in f" {sql_norm} ":
                violations.append("missing_time_group_by")
            raw_time_bucket_sql = query_plan.get("time_bucket_sql")
            time_bucket_expr = _normalize_sql(raw_time_bucket_sql) if raw_time_bucket_sql else ""
            if not time_bucket_expr:
                grain = str(query_plan.get("time_grain", "")).lower()
                if grain == "day":
                    time_bucket_expr = _normalize_sql(f"DATE({query_plan.get('time_dimension_sql', '')})")
                elif grain in {"week", "month", "year"}:
                    time_bucket_expr = _normalize_sql(
                        f"DATE_TRUNC('{grain}', {query_plan.get('time_dimension_sql', '')})"
                    )
                else:
                    time_bucket_expr = time_dimension_sql
            if time_bucket_expr and time_bucket_expr not in sql_norm:
                violations.append(f"missing_time_bucket:{time_bucket_expr}")
            if " as time_bucket" not in sql_norm:
                violations.append("missing_time_bucket_alias:time_bucket")
            if not self._order_contains(sql_norm, "time_bucket") and (
                time_bucket_expr and not self._order_contains(sql_norm, time_bucket_expr)
            ):
                violations.append("missing_time_order")

        for item in query_plan.get("filters", []) or []:
            expected = self._expected_filter(item)
            if expected and expected not in sql_norm:
                violations.append(f"missing_filter:{expected}")

        metric_name = str(query_plan.get("metric_name", "")).strip().lower()
        if metric_name and f" as {metric_name}" not in sql_norm:
            violations.append(f"missing_metric_alias:{metric_name}")

        metric_sql = _normalize_sql(query_plan.get("metric_sql", ""))
        if metric_sql and metric_sql not in sql_norm:
            violations.append(f"missing_metric_expression:{metric_sql}")

        if str(query_plan.get("intent", "")).upper() == "TOP_K":
            expected_limit = query_plan.get("limit")
            if expected_limit is not None:
                actual_limit = self._extract_limit(sql_norm)
                if actual_limit is None:
                    violations.append(f"missing_limit:{expected_limit}")
                elif actual_limit != int(expected_limit):
                    violations.append(f"wrong_limit:{actual_limit}")

            expected_direction = str(query_plan.get("order_direction", "DESC")).upper()
            expected_order_expr = metric_name or metric_sql
            if expected_order_expr and not self._order_matches(sql_norm, expected_order_expr, expected_direction):
                violations.append(
                    f"missing_order:{_normalize_sql(expected_order_expr)} {expected_direction.lower()}"
                )

        return PlanValidationResult(ok=not violations, violations=violations)

    @staticmethod
    def _expected_filter(item: dict[str, Any]) -> str:
        sql = str(item.get("sql", "") or item.get("sql_expression", "")).strip()
        if sql:
            return _normalize_sql(sql)

        column = str(item.get("column", "")).strip()
        table = str(item.get("table", "")).strip()
        operator = str(item.get("operator", "=")).strip()
        if not column:
            return ""
        column_ref = f"{table}.{column}" if table and "." not in column else column
        return _normalize_sql(f"{column_ref} {operator} {_quoted(item.get('value'))}")

    @staticmethod
    def _extract_limit(sql_norm: str) -> int | None:
        match = _LIMIT_RE.search(sql_norm)
        if not match:
            return None
        return int(match.group(1))

    @staticmethod
    def _order_contains(sql_norm: str, expected_expression: str) -> bool:
        order_clause = QueryPlanValidator._order_clause(sql_norm)
        expected = _normalize_sql(expected_expression)
        return bool(order_clause and expected and expected in order_clause)

    @staticmethod
    def _order_matches(sql_norm: str, expected_expression: str, expected_direction: str) -> bool:
        order_clause = QueryPlanValidator._order_clause(sql_norm)
        expected_expr = _normalize_sql(expected_expression)
        expected_dir = (expected_direction or "").lower()
        if not order_clause or not expected_expr:
            return False
        return f"{expected_expr} {expected_dir}" in order_clause

    @staticmethod
    def _order_clause(sql_norm: str) -> str:
        match = _ORDER_BY_RE.search(sql_norm)
        if not match:
            return ""
        return _normalize_sql(match.group(1))
