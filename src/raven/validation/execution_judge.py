"""
Execution-grounded result sanity checks for deterministic and validated SQL.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ExecutionJudgeResult:
    passed: bool
    issues: list[str] = field(default_factory=list)


class ExecutionJudge:
    """Reject obviously suspicious result shapes before rendering a final answer."""

    _PERCENTAGE_INTENTS = {
        "FILTER_PERCENTAGE",
        "FILTER_BREAKDOWN_PERCENTAGE",
        "SHARE",
    }

    _SINGLE_ROW_INTENTS = {
        "KPI",
        "FILTER_PERCENTAGE",
        "PERIOD_GROWTH",
    }

    def judge(self, df: Any, query_plan: dict[str, Any] | None) -> ExecutionJudgeResult:
        if query_plan is None:
            return ExecutionJudgeResult(passed=True, issues=[])
        if df is None:
            return ExecutionJudgeResult(passed=False, issues=["missing_result_frame"])

        issues: list[str] = []
        columns = [str(col) for col in getattr(df, "columns", [])]
        row_count = len(df)
        intent = str(query_plan.get("intent", "")).upper()

        metric_name = str(query_plan.get("metric_name", "")).strip()
        metric_col = self._find_column(columns, metric_name)
        if metric_name and not metric_col:
            issues.append(f"missing_result_metric:{metric_name}")

        group_col = None
        group_key = query_plan.get("group_by_sql") or query_plan.get("group_by")
        if group_key:
            group_col = self._find_column(columns, str(group_key))
            if not group_col:
                issues.append(f"missing_result_group:{group_key}")

        time_col = None
        if query_plan.get("time_dimension_sql") or query_plan.get("time_dimension"):
            time_col = self._find_column(columns, "time_bucket")
            if not time_col:
                issues.append("missing_result_time_bucket:time_bucket")

        if intent in self._SINGLE_ROW_INTENTS and row_count > 1:
            issues.append(f"unexpected_row_count:{row_count}")

        limit = query_plan.get("limit")
        if isinstance(limit, int) and row_count > limit:
            issues.append(f"row_count_exceeds_limit:{row_count}>{limit}")

        if group_col and row_count > 0:
            duplicates = getattr(df[group_col], "duplicated", None)
            if duplicates is not None and duplicates().any():
                issues.append(f"duplicate_group_values:{group_col}")

        if time_col and row_count > 1:
            duplicates = getattr(df[time_col], "duplicated", None)
            if duplicates is not None and duplicates().any():
                issues.append("duplicate_time_bucket_values")
            monotonic = getattr(df[time_col], "is_monotonic_increasing", None)
            if monotonic is False:
                issues.append("time_bucket_not_sorted")

        numeric_metric = self._numeric_values(df, metric_col)
        if intent == "TOP_K" and metric_col and len(numeric_metric) == row_count:
            direction = str(query_plan.get("order_direction", "DESC")).upper()
            if not self._ordered(numeric_metric, direction):
                issues.append(f"metric_order_mismatch:{direction.lower()}")

        if intent in self._PERCENTAGE_INTENTS and metric_col and len(numeric_metric) == row_count:
            if any(value < -0.01 or value > 100.01 for value in numeric_metric):
                issues.append("percentage_out_of_range")

        if intent == "FILTER_BREAKDOWN_PERCENTAGE" and metric_col and len(numeric_metric) == row_count and row_count > 0:
            total = sum(numeric_metric)
            if abs(total - 100.0) > 2.0:
                issues.append(f"percentage_total_not_100:{round(total, 2)}")

        if intent == "SHARE" and not limit and metric_col and len(numeric_metric) == row_count and row_count > 0:
            total = sum(numeric_metric)
            if abs(total - 100.0) > 2.0:
                issues.append(f"share_total_not_100:{round(total, 2)}")

        return ExecutionJudgeResult(passed=not issues, issues=issues)

    @staticmethod
    def _find_column(columns: list[str], expected: str) -> str | None:
        if not expected:
            return None
        expected_norm = str(expected).strip().lower()
        suffix = expected_norm.split(".")[-1]
        for column in columns:
            col_norm = column.lower()
            if col_norm == expected_norm or col_norm == suffix or col_norm.endswith(f".{suffix}"):
                return column
        return None

    @staticmethod
    def _numeric_values(df: Any, column: str | None) -> list[float]:
        if not column:
            return []
        values: list[float] = []
        try:
            for value in df[column].tolist():
                if value is None:
                    return []
                values.append(float(value))
        except Exception:
            return []
        return values

    @staticmethod
    def _ordered(values: list[float], direction: str) -> bool:
        if len(values) < 2:
            return True
        pairs = zip(values, values[1:])
        if direction == "ASC":
            return all(left <= right for left, right in pairs)
        return all(left >= right for left, right in pairs)
