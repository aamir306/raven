"""
Deterministic planner for trusted analytics questions.

This planner stays conservative: it prefers single-table plans, but it can
compile a small multi-table query when the metric contract is clear and the
join path is explicitly approved by the semantic relationships.
"""

from __future__ import annotations

import re
from typing import Any

from ..grounding.value_resolver import ResolvedFilter
from ..semantic_assets import SemanticModelStore
from .query_plan import PlanEvidence, PlanJoin, QueryPlan

_TOP_RE = re.compile(r"\btop\s+(\d+)\b", re.IGNORECASE)
_BOTTOM_RE = re.compile(r"\bbottom\s+(\d+)\b", re.IGNORECASE)
_YEAR_RE = re.compile(r"\b(20\d{2})\b")


def _question_tokens(text: str) -> set[str]:
    base = {
        token
        for token in re.findall(r"[a-z0-9_]+", str(text).lower())
        if len(token) > 1
    }
    expanded = set(base)
    for token in list(base):
        expanded.update(part for part in token.split("_") if len(part) > 1)
        if token.endswith("s") and len(token) > 3:
            expanded.add(token[:-1])
    return expanded


def _intent_from_question(question: str) -> tuple[str, str | None, int | None, str]:
    q = question.lower()
    time_grain = None
    limit = None
    order_direction = "DESC"
    is_share = any(
        token in q
        for token in (
            "share",
            "contribution",
            "percentage of total",
            "percent of total",
            "% of total",
        )
    )

    top_match = _TOP_RE.search(q)
    bottom_match = _BOTTOM_RE.search(q)
    if top_match:
        limit = int(top_match.group(1))
    if bottom_match:
        limit = int(bottom_match.group(1))
        order_direction = "ASC"

    if any(token in q for token in ("daily", "day-wise", "day wise", "per day")):
        time_grain = "day"
    elif any(token in q for token in ("weekly", "week-wise", "week wise", "per week")):
        time_grain = "week"
    elif any(token in q for token in ("monthly", "month-wise", "month wise", "per month", "mom")):
        time_grain = "month"
    elif any(token in q for token in ("yearly", "year-wise", "year wise", "yoy")):
        time_grain = "year"

    if is_share and (" by " in q or any(token in q for token in ("breakdown", "split", "grouped"))):
        return "SHARE", None, limit, order_direction
    if time_grain:
        return "TIME_SERIES", time_grain, limit, order_direction
    if " by " in q or any(token in q for token in ("breakdown", "split", "grouped")):
        return "GROUPED_AGG", None, limit, order_direction
    if limit:
        return "TOP_K", None, limit, order_direction
    return "KPI", None, None, order_direction


def _is_percentage_question(question: str) -> bool:
    q = question.lower()
    return any(token in q for token in ("percentage", "percent", "%", "ratio", "rate"))


def _is_growth_question(question: str) -> bool:
    q = question.lower()
    if any(
        token in q
        for token in (
            "growth",
            "growth %",
            "percentage change",
            "percent change",
            "pct change",
            "change %",
            "change percentage",
        )
    ):
        return True
    if "vs" in q or "versus" in q:
        return _has_period_comparison_context(question)
    return False


def _requests_grouping(question: str) -> bool:
    q = question.lower()
    return " by " in q or any(token in q for token in ("breakdown", "split", "grouped"))


def _has_period_comparison_context(question: str) -> bool:
    q = question.lower()
    if ("this month" in q and "last month" in q) or ("this year" in q and "last year" in q) or (
        "this week" in q and "last week" in q
    ):
        return True
    return len(_YEAR_RE.findall(q)) >= 2


class DeterministicPlanner:
    """Build conservative query plans from semantic assets and approved joins."""

    def __init__(self, semantic_store: SemanticModelStore):
        self.semantic_store = semantic_store

    def plan(
        self,
        *,
        question: str,
        glossary_matches: list[dict[str, Any]],
        selected_tables: list[str],
        preferred_tables: list[str],
        resolved_filters: list[dict[str, Any]],
        instruction_matches: list[dict[str, Any]],
        om_table_candidates: list[dict[str, Any]],
        metabase_evidence: list[dict[str, Any]],
        join_paths: list[Any],
    ) -> QueryPlan | None:
        intent, time_grain, limit, order_direction = _intent_from_question(question)

        metric = self._pick_metric(
            glossary_matches=glossary_matches,
            selected_tables=selected_tables,
            preferred_tables=preferred_tables,
            om_table_candidates=om_table_candidates,
            metabase_evidence=metabase_evidence,
        )
        if not metric:
            breakdown_plan = self._build_filter_breakdown_percentage_plan(
                question=question,
                selected_tables=selected_tables,
                resolved_filters=resolved_filters,
                instruction_matches=instruction_matches,
                om_table_candidates=om_table_candidates,
                metabase_evidence=metabase_evidence,
            )
            if breakdown_plan:
                return breakdown_plan
            breakdown_count_plan = self._build_filter_breakdown_aggregate_plan(
                question=question,
                metric=None,
                selected_tables=selected_tables,
                resolved_filters=resolved_filters,
                instruction_matches=instruction_matches,
                om_table_candidates=om_table_candidates,
                metabase_evidence=metabase_evidence,
                join_paths=join_paths,
            )
            if breakdown_count_plan:
                return breakdown_count_plan
            ratio_plan = self._build_filter_percentage_plan(
                question=question,
                selected_tables=selected_tables,
                resolved_filters=resolved_filters,
                instruction_matches=instruction_matches,
                om_table_candidates=om_table_candidates,
                metabase_evidence=metabase_evidence,
            )
            if ratio_plan:
                return ratio_plan
            return None

        metric_table = str(metric.get("table", ""))
        table_asset = self.semantic_store.get_table_asset(metric_table)
        if not table_asset:
            return None

        if not _is_growth_question(question):
            breakdown_agg_plan = self._build_filter_breakdown_aggregate_plan(
                question=question,
                metric=metric,
                selected_tables=selected_tables,
                resolved_filters=resolved_filters,
                instruction_matches=instruction_matches,
                om_table_candidates=om_table_candidates,
                metabase_evidence=metabase_evidence,
                join_paths=join_paths,
            )
            if breakdown_agg_plan:
                return breakdown_agg_plan

        growth_plan = self._build_period_growth_plan(
            question=question,
            metric=metric,
            table_asset=table_asset,
            glossary_matches=glossary_matches,
            selected_tables=selected_tables,
            resolved_filters=resolved_filters,
            instruction_matches=instruction_matches,
            om_table_candidates=om_table_candidates,
            metabase_evidence=metabase_evidence,
            join_paths=join_paths,
        )
        if growth_plan:
            return growth_plan

        joins: list[PlanJoin] = []
        source_tables = [metric_table]
        group_by = None
        group_by_sql = None
        time_dimension = None
        time_dimension_sql = None

        if intent in {"GROUPED_AGG", "TOP_K", "SHARE"}:
            dimension = self._pick_dimension(
                glossary_matches,
                question=question,
                metric_table=metric_table,
                join_paths=join_paths,
            )
            if not dimension:
                return None
            group_by = str(dimension.get("field_name", ""))
            dimension_table = str(dimension.get("table", metric_table))
            if dimension_table != metric_table:
                joins = self._joins_for_target(
                    metric_table=metric_table,
                    target_table=dimension_table,
                    join_paths=join_paths,
                )
                if not joins:
                    return None
                source_tables = self._source_tables(metric_table, joins)
                group_by_sql = f"{dimension_table}.{group_by}"
            else:
                group_by_sql = group_by

        if intent == "TIME_SERIES":
            time_dimension = self._pick_time_dimension(glossary_matches, table=metric_table)
            if not time_dimension and table_asset.time_dimensions:
                time_dimension = str(table_asset.time_dimensions[0].get("name", ""))
            if not time_dimension:
                return None
            time_dimension_sql = time_dimension

        filters = self._select_filters(
            allowed_tables=set(source_tables),
            resolved_filters=resolved_filters,
        )
        if self._has_ambiguity(resolved_filters):
            return None

        confidence = "HIGH" if any(item.get("source") == "metabase" for item in metabase_evidence) else "MEDIUM"
        if joins and confidence == "MEDIUM":
            confidence = "HIGH"

        path_type = "DETERMINISTIC_MULTI_TABLE" if joins else "DETERMINISTIC_SINGLE_TABLE"
        metric_name = str(metric["field_name"])
        metric_sql = str(metric["sql_fragment"])
        if intent == "SHARE":
            metric_name = self._share_metric_name(metric_name)
            metric_sql = self._share_metric_sql(metric_sql)
        evidence = self._build_evidence(
            metric=metric,
            table=metric_table,
            instruction_matches=instruction_matches,
            om_table_candidates=om_table_candidates,
            metabase_evidence=metabase_evidence,
            joins=joins,
        )

        return QueryPlan(
            path_type=path_type,
            intent=intent,
            table=metric_table,
            source_tables=source_tables,
            joins=joins,
            metric_name=metric_name,
            metric_sql=metric_sql,
            confidence=confidence,
            group_by=group_by,
            group_by_sql=group_by_sql,
            time_dimension=time_dimension,
            time_dimension_sql=time_dimension_sql,
            time_grain=time_grain,
            filters=filters,
            order_direction=order_direction,
            limit=limit,
            evidence=evidence,
        )

    def _build_filter_percentage_plan(
        self,
        *,
        question: str,
        selected_tables: list[str],
        resolved_filters: list[dict[str, Any]],
        instruction_matches: list[dict[str, Any]],
        om_table_candidates: list[dict[str, Any]],
        metabase_evidence: list[dict[str, Any]],
    ) -> QueryPlan | None:
        if not _is_percentage_question(question):
            return None
        if len(selected_tables) != 1:
            return None

        table = str(selected_tables[0])
        filters = self._select_filters(
            allowed_tables={table},
            resolved_filters=resolved_filters,
        )
        if self._has_ambiguity(resolved_filters):
            return None

        structured = [
            flt
            for flt in filters
            if flt.column and not flt.sql_expression and flt.table == table
        ]
        if len(structured) != 1:
            return None

        target = structured[0]
        remaining_filters = [flt for flt in filters if flt != target]
        target_sql = target.to_sql()
        if not target_sql:
            return None

        label = target.matched_text or str(target.value or target.column or "match")
        metric_name = self._percentage_metric_name(label)
        metric_sql = (
            "ROUND(100.0 * "
            f"COUNT_IF({target_sql}) / NULLIF(COUNT(*), 0), 2)"
        )
        confidence = "HIGH" if any(item.get("source") == "metabase" for item in metabase_evidence) else "MEDIUM"
        evidence = [
            PlanEvidence(
                kind="ratio_filter",
                source=target.source or "value_grounding",
                detail=target_sql,
                score=float(target.confidence or 0.0),
            )
        ]
        evidence.extend(
            self._build_evidence(
                metric={
                    "field_name": metric_name,
                    "similarity": float(target.confidence or 0.0),
                    "source": target.source or "value_grounding",
                },
                table=table,
                instruction_matches=instruction_matches,
                om_table_candidates=om_table_candidates,
                metabase_evidence=metabase_evidence,
                joins=[],
            )[1:]
        )

        return QueryPlan(
            path_type="DETERMINISTIC_SINGLE_TABLE",
            intent="FILTER_PERCENTAGE",
            table=table,
            source_tables=[table],
            metric_name=metric_name,
            metric_sql=metric_sql,
            confidence=confidence,
            filters=remaining_filters,
            evidence=evidence,
        )

    def _build_filter_breakdown_percentage_plan(
        self,
        *,
        question: str,
        selected_tables: list[str],
        resolved_filters: list[dict[str, Any]],
        instruction_matches: list[dict[str, Any]],
        om_table_candidates: list[dict[str, Any]],
        metabase_evidence: list[dict[str, Any]],
    ) -> QueryPlan | None:
        if not _is_percentage_question(question):
            return None
        if len(selected_tables) != 1:
            return None

        table = str(selected_tables[0])
        filters = self._select_filters(
            allowed_tables={table},
            resolved_filters=resolved_filters,
        )
        if self._has_ambiguity(resolved_filters):
            return None

        structured = [
            flt
            for flt in filters
            if flt.column and not flt.sql_expression and flt.table == table
        ]
        if len(structured) < 2:
            return None

        grouped: dict[tuple[str, str], list[ResolvedFilter]] = {}
        for flt in structured:
            grouped.setdefault((flt.table, flt.column), []).append(flt)

        ranked_groups = sorted(grouped.values(), key=len, reverse=True)
        candidates = [group for group in ranked_groups if len(group) >= 2]
        if len(candidates) != 1:
            return None

        target_group = candidates[0]
        column = str(target_group[0].column)
        values = [flt.value for flt in target_group if flt.value is not None]
        if len(values) < 2:
            return None

        in_filter = ResolvedFilter(
            sql_expression=self._in_filter_sql(table, column, values),
            source="value_grounding",
            confidence=min(max((flt.confidence for flt in target_group), default=0.0), 1.0),
            matched_text=column,
        )
        remaining_filters = [
            flt
            for flt in filters
            if flt not in target_group
        ]
        remaining_filters.append(in_filter)

        confidence = "HIGH" if any(item.get("source") == "metabase" for item in metabase_evidence) else "MEDIUM"
        evidence = [
            PlanEvidence(
                kind="ratio_breakdown",
                source="value_grounding",
                detail=in_filter.sql_expression,
                score=float(in_filter.confidence or 0.0),
            )
        ]
        evidence.extend(
            self._build_evidence(
                metric={
                    "field_name": self._percentage_metric_name(column),
                    "similarity": float(in_filter.confidence or 0.0),
                    "source": "value_grounding",
                },
                table=table,
                instruction_matches=instruction_matches,
                om_table_candidates=om_table_candidates,
                metabase_evidence=metabase_evidence,
                joins=[],
            )[1:]
        )

        return QueryPlan(
            path_type="DETERMINISTIC_SINGLE_TABLE",
            intent="FILTER_BREAKDOWN_PERCENTAGE",
            table=table,
            source_tables=[table],
            metric_name=self._percentage_metric_name(column),
            metric_sql="ROUND(100.0 * COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (), 0), 2)",
            confidence=confidence,
            group_by=column,
            group_by_sql=f"{table}.{column}",
            filters=remaining_filters,
            evidence=evidence,
        )

    def _build_filter_breakdown_aggregate_plan(
        self,
        *,
        question: str,
        metric: dict[str, Any] | None,
        selected_tables: list[str],
        resolved_filters: list[dict[str, Any]],
        instruction_matches: list[dict[str, Any]],
        om_table_candidates: list[dict[str, Any]],
        metabase_evidence: list[dict[str, Any]],
        join_paths: list[Any],
    ) -> QueryPlan | None:
        if _is_percentage_question(question):
            return None
        if _requests_grouping(question):
            return None
        if not selected_tables:
            return None

        metric_table = str(metric.get("table", "") or selected_tables[0]) if metric else str(selected_tables[0])
        if metric and metric_table not in selected_tables:
            return None
        if not metric and len(selected_tables) != 1:
            return None

        filters = self._select_filters(
            allowed_tables=set(selected_tables),
            resolved_filters=resolved_filters,
        )
        if self._has_ambiguity(resolved_filters):
            return None

        structured = [
            flt
            for flt in filters
            if flt.column and not flt.sql_expression and flt.table
        ]
        if len(structured) < 2:
            return None

        grouped: dict[tuple[str, str], list[ResolvedFilter]] = {}
        for flt in structured:
            grouped.setdefault((flt.table, flt.column), []).append(flt)

        ranked_groups = sorted(grouped.values(), key=len, reverse=True)
        candidates = [group for group in ranked_groups if len(group) >= 2]
        if len(candidates) != 1:
            return None

        target_group = candidates[0]
        target_table = str(target_group[0].table)
        target_column = str(target_group[0].column)
        values = [flt.value for flt in target_group if flt.value is not None]
        if len(values) < 2:
            return None

        joins: list[PlanJoin] = []
        source_tables = [metric_table]
        group_by_sql = f"{target_table}.{target_column}"
        if target_table != metric_table:
            if not metric:
                return None
            joins = self._joins_for_target(
                metric_table=metric_table,
                target_table=target_table,
                join_paths=join_paths,
            )
            if not joins:
                return None
            source_tables = self._source_tables(metric_table, joins)

        in_filter = ResolvedFilter(
            sql_expression=self._in_filter_sql(target_table, target_column, values),
            source="value_grounding",
            confidence=min(max((flt.confidence for flt in target_group), default=0.0), 1.0),
            matched_text=target_column,
        )
        remaining_filters = [flt for flt in filters if flt not in target_group]
        remaining_filters.append(in_filter)

        if metric:
            metric_name = str(metric.get("field_name", "metric"))
            metric_sql = str(metric.get("sql_fragment", ""))
            intent = "FILTER_BREAKDOWN_AGG"
            metric_evidence = metric
        else:
            metric_name = self._count_metric_name(target_column)
            metric_sql = "COUNT(*)"
            intent = "FILTER_BREAKDOWN_COUNT"
            metric_evidence = {
                "field_name": metric_name,
                "similarity": float(in_filter.confidence or 0.0),
                "source": "value_grounding",
            }

        confidence = "HIGH" if any(item.get("source") == "metabase" for item in metabase_evidence) else "MEDIUM"
        if joins and confidence == "MEDIUM":
            confidence = "HIGH"

        evidence = [
            PlanEvidence(
                kind="breakdown_filter",
                source="value_grounding",
                detail=in_filter.sql_expression,
                score=float(in_filter.confidence or 0.0),
            )
        ]
        evidence.extend(
            self._build_evidence(
                metric=metric_evidence,
                table=metric_table,
                instruction_matches=instruction_matches,
                om_table_candidates=om_table_candidates,
                metabase_evidence=metabase_evidence,
                joins=joins,
            )
        )

        return QueryPlan(
            path_type="DETERMINISTIC_MULTI_TABLE" if joins else "DETERMINISTIC_SINGLE_TABLE",
            intent=intent,
            table=metric_table,
            source_tables=source_tables,
            joins=joins,
            metric_name=metric_name,
            metric_sql=metric_sql,
            confidence=confidence,
            group_by=target_column,
            group_by_sql=group_by_sql,
            filters=remaining_filters,
            evidence=evidence,
        )

    def _build_period_growth_plan(
        self,
        *,
        question: str,
        metric: dict[str, Any],
        table_asset: Any,
        glossary_matches: list[dict[str, Any]],
        selected_tables: list[str],
        resolved_filters: list[dict[str, Any]],
        instruction_matches: list[dict[str, Any]],
        om_table_candidates: list[dict[str, Any]],
        metabase_evidence: list[dict[str, Any]],
        join_paths: list[Any],
    ) -> QueryPlan | None:
        if not _is_growth_question(question):
            return None

        table = str(metric.get("table", "") or selected_tables[0])
        if selected_tables and table not in selected_tables:
            return None

        time_dimension = self._pick_time_dimension([], table=table)
        if not time_dimension and getattr(table_asset, "time_dimensions", None):
            time_dimension = str(table_asset.time_dimensions[0].get("name", ""))
        if not time_dimension:
            return None

        time_column = f"{table}.{time_dimension}"
        period = self._period_growth_context(question, time_column)
        if not period:
            return None

        joins: list[PlanJoin] = []
        source_tables = [table]
        group_by = None
        group_by_sql = None
        grouped_requested = _requests_grouping(question)
        if grouped_requested:
            dimension = self._pick_dimension(
                glossary_matches,
                question=question,
                metric_table=table,
                join_paths=join_paths,
            )
            if not dimension:
                return None
            group_by = str(dimension.get("field_name", ""))
            dimension_table = str(dimension.get("table", table))
            if dimension_table != table:
                joins = self._joins_for_target(
                    metric_table=table,
                    target_table=dimension_table,
                    join_paths=join_paths,
                )
                if not joins:
                    return None
                source_tables = self._source_tables(table, joins)
                group_by_sql = f"{dimension_table}.{group_by}"
            else:
                group_by_sql = group_by

        current_sql = self._conditional_metric_sql(str(metric.get("sql_fragment", "")), period["current_condition"])
        previous_sql = self._conditional_metric_sql(str(metric.get("sql_fragment", "")), period["previous_condition"])
        if not current_sql or not previous_sql:
            return None

        filters = self._select_filters(
            allowed_tables=set(source_tables),
            resolved_filters=resolved_filters,
        )
        if self._has_ambiguity(resolved_filters):
            return None
        remaining_filters = [
            flt
            for flt in filters
            if not self._filter_touches_time_dimension(flt, table, time_dimension)
        ]

        metric_name = self._growth_metric_name(
            str(metric.get("field_name", "metric")),
            period["label"],
        )
        metric_sql = (
            "ROUND(100.0 * ("
            f"({current_sql}) - ({previous_sql})"
            f") / NULLIF(({previous_sql}), 0), 2)"
        )
        confidence = "HIGH" if any(item.get("source") == "metabase" for item in metabase_evidence) else "MEDIUM"
        if joins and confidence == "MEDIUM":
            confidence = "HIGH"
        evidence = [
            PlanEvidence(
                kind="period_comparison",
                source="planner",
                detail=period["label"],
                score=1.0,
            )
        ]
        evidence.extend(
            self._build_evidence(
                metric=metric,
                table=table,
                instruction_matches=instruction_matches,
                om_table_candidates=om_table_candidates,
                metabase_evidence=metabase_evidence,
                joins=joins,
            )
        )

        return QueryPlan(
            path_type="DETERMINISTIC_MULTI_TABLE" if joins else "DETERMINISTIC_SINGLE_TABLE",
            intent="GROUPED_PERIOD_GROWTH" if group_by_sql else "PERIOD_GROWTH",
            table=table,
            source_tables=source_tables,
            joins=joins,
            metric_name=metric_name,
            metric_sql=metric_sql,
            confidence=confidence,
            group_by=group_by,
            group_by_sql=group_by_sql,
            filters=remaining_filters,
            evidence=evidence,
        )

    @staticmethod
    def _percentage_metric_name(label: str) -> str:
        base = re.sub(r"[^a-zA-Z0-9_]+", "_", str(label).strip().lower()).strip("_")
        base = base or "matching"
        return f"{base}_percentage"

    @staticmethod
    def _count_metric_name(label: str) -> str:
        base = re.sub(r"[^a-zA-Z0-9_]+", "_", str(label).strip().lower()).strip("_")
        base = base or "matching"
        return f"{base}_count"

    @staticmethod
    def _in_filter_sql(table: str, column: str, values: list[Any]) -> str:
        quoted = ", ".join(
            "'" + str(value).replace("'", "''") + "'"
            if not isinstance(value, (int, float))
            else str(value)
            for value in values
        )
        return f"{table}.{column} IN ({quoted})"

    @staticmethod
    def _growth_metric_name(metric_name: str, label: str) -> str:
        metric_base = re.sub(r"[^a-zA-Z0-9_]+", "_", str(metric_name).strip().lower()).strip("_")
        label_base = re.sub(r"[^a-zA-Z0-9_]+", "_", str(label).strip().lower()).strip("_")
        metric_base = metric_base or "metric"
        label_base = label_base or "period"
        return f"{metric_base}_{label_base}_growth_pct"

    @staticmethod
    def _share_metric_name(metric_name: str) -> str:
        base = re.sub(r"[^a-zA-Z0-9_]+", "_", str(metric_name).strip()).strip("_")
        base = base or "metric"
        return f"{base}_share_pct"

    @staticmethod
    def _share_metric_sql(metric_sql: str) -> str:
        expr = str(metric_sql).strip()
        return (
            "ROUND(100.0 * "
            f"{expr} / NULLIF(SUM({expr}) OVER (), 0), 2)"
        )

    @staticmethod
    def _period_growth_context(question: str, time_column: str) -> dict[str, str] | None:
        q = question.lower()
        if "this month" in q and "last month" in q:
            return {
                "label": "last_month_to_this_month",
                "current_condition": f"{time_column} >= DATE_TRUNC('month', CURRENT_DATE)",
                "previous_condition": (
                    f"{time_column} >= DATE_ADD('month', -1, DATE_TRUNC('month', CURRENT_DATE)) "
                    f"AND {time_column} < DATE_TRUNC('month', CURRENT_DATE)"
                ),
            }
        if "this year" in q and "last year" in q:
            return {
                "label": "last_year_to_this_year",
                "current_condition": f"{time_column} >= DATE_TRUNC('year', CURRENT_DATE)",
                "previous_condition": (
                    f"{time_column} >= DATE_ADD('year', -1, DATE_TRUNC('year', CURRENT_DATE)) "
                    f"AND {time_column} < DATE_TRUNC('year', CURRENT_DATE)"
                ),
            }
        if "this week" in q and "last week" in q:
            return {
                "label": "last_week_to_this_week",
                "current_condition": f"{time_column} >= DATE_TRUNC('week', CURRENT_DATE)",
                "previous_condition": (
                    f"{time_column} >= DATE_ADD('week', -1, DATE_TRUNC('week', CURRENT_DATE)) "
                    f"AND {time_column} < DATE_TRUNC('week', CURRENT_DATE)"
                ),
            }

        years = _YEAR_RE.findall(q)
        if len(years) >= 2:
            previous_year, current_year = years[0], years[1]
            return {
                "label": f"{previous_year}_to_{current_year}",
                "current_condition": f"EXTRACT(YEAR FROM {time_column}) = {current_year}",
                "previous_condition": f"EXTRACT(YEAR FROM {time_column}) = {previous_year}",
            }
        return None

    @staticmethod
    def _conditional_metric_sql(metric_sql: str, condition: str) -> str | None:
        expr = str(metric_sql).strip()
        sum_match = re.match(r"(?is)^sum\((.+)\)$", expr)
        if sum_match:
            inner = sum_match.group(1).strip()
            return f"SUM(CASE WHEN {condition} THEN {inner} ELSE 0 END)"

        count_star_match = re.match(r"(?is)^count\(\s*(\*|1)\s*\)$", expr)
        if count_star_match:
            return f"COUNT_IF({condition})"

        count_distinct_match = re.match(r"(?is)^count\(\s*distinct\s+(.+)\)$", expr)
        if count_distinct_match:
            inner = count_distinct_match.group(1).strip()
            return f"COUNT(DISTINCT CASE WHEN {condition} THEN {inner} ELSE NULL END)"

        count_match = re.match(r"(?is)^count\((.+)\)$", expr)
        if count_match:
            inner = count_match.group(1).strip()
            return f"COUNT_IF({condition} AND {inner} IS NOT NULL)"

        avg_match = re.match(r"(?is)^avg\((.+)\)$", expr)
        if avg_match:
            inner = avg_match.group(1).strip()
            return f"AVG(CASE WHEN {condition} THEN {inner} ELSE NULL END)"

        return None

    @staticmethod
    def _filter_touches_time_dimension(
        flt: ResolvedFilter,
        table: str,
        time_dimension: str,
    ) -> bool:
        if flt.table == table and flt.column == time_dimension:
            return True
        sql = flt.to_sql()
        return bool(sql and f"{table}.{time_dimension}" in sql)

    @staticmethod
    def _has_ambiguity(resolved_filters: list[dict[str, Any]]) -> bool:
        return any(item.get("source") == "ambiguity" for item in resolved_filters)

    def _pick_metric(
        self,
        *,
        glossary_matches: list[dict[str, Any]],
        selected_tables: list[str],
        preferred_tables: list[str],
        om_table_candidates: list[dict[str, Any]],
        metabase_evidence: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        metrics = [item for item in glossary_matches if item.get("kind") == "metric"]
        if not metrics:
            return None

        selected = set(selected_tables)
        preferred = set(preferred_tables)
        om_scores = {
            item.get("fqn", ""): (
                float(item.get("score", 0.0)),
                item.get("quality_status", "UNKNOWN"),
            )
            for item in om_table_candidates
        }
        metabase_tables = {
            table
            for item in metabase_evidence
            for table in item.get("tables", [])
        }

        ranked = []
        for metric in metrics:
            score = float(metric.get("similarity", 0.0))
            table = metric.get("table", "")
            if table in selected:
                score += 0.20
            if table in preferred:
                score += 0.15
            if table in metabase_tables:
                score += 0.15
            om_score, quality_status = om_scores.get(table, (0.0, "UNKNOWN"))
            score += min(om_score, 0.2)
            if quality_status == "PASS":
                score += 0.05
            elif quality_status == "FAIL":
                score -= 0.10
            ranked.append((score, metric))

        ranked.sort(key=lambda item: item[0], reverse=True)
        best_score, best_metric = ranked[0]
        if best_score < 0.45:
            return None
        return best_metric

    @staticmethod
    def _pick_dimension(
        glossary_matches: list[dict[str, Any]],
        *,
        question: str,
        metric_table: str,
        join_paths: list[Any],
    ) -> dict[str, Any] | None:
        candidates = [
            item
            for item in glossary_matches
            if item.get("kind") == "dimension"
        ]
        if not candidates:
            return None

        question_tokens = _question_tokens(question)
        joinable_tables = {metric_table}
        for path in join_paths:
            if not isinstance(path, dict):
                continue
            route = path.get("path", []) or []
            if metric_table in route:
                joinable_tables.update(route)

        ranked: list[tuple[float, dict[str, Any]]] = []
        for item in candidates:
            table = str(item.get("table", ""))
            score = float(item.get("similarity", 0.0))
            field_tokens = _question_tokens(
                f"{item.get('field_name', '')} {item.get('term', '')}"
            )
            lexical_overlap = len(question_tokens & field_tokens)
            if lexical_overlap == 0:
                score -= 0.25
            else:
                score += min(0.12 * lexical_overlap, 0.24)
            if table == metric_table:
                score += 0.10
            elif table in joinable_tables:
                score += 0.12
            else:
                score -= 0.20
            ranked.append((score, item))

        ranked.sort(key=lambda item: item[0], reverse=True)
        best_score, best = ranked[0]
        if best_score < 0.35:
            return None
        return best

    @staticmethod
    def _pick_time_dimension(glossary_matches: list[dict[str, Any]], *, table: str) -> str | None:
        candidates = [
            item
            for item in glossary_matches
            if item.get("table") == table and item.get("kind") == "time_dimension"
        ]
        if not candidates:
            return None
        candidates.sort(key=lambda item: item.get("similarity", 0.0), reverse=True)
        best = candidates[0]
        return str(best.get("field_name", "")) or None

    @staticmethod
    def _select_filters(
        *,
        allowed_tables: set[str],
        resolved_filters: list[dict[str, Any]],
    ) -> list[ResolvedFilter]:
        filters: list[ResolvedFilter] = []
        for item in resolved_filters:
            filter_table = item.get("table", "")
            if filter_table and filter_table not in allowed_tables:
                continue
            filters.append(
                ResolvedFilter(
                    table=filter_table,
                    column=item.get("column", ""),
                    operator=item.get("operator", "="),
                    value=item.get("value"),
                    sql_expression=item.get("sql_expression", ""),
                    source=item.get("source", ""),
                    confidence=float(item.get("confidence", 0.0)),
                    matched_text=item.get("matched_text", ""),
                )
            )
        return filters

    @staticmethod
    def _joins_for_target(
        *,
        metric_table: str,
        target_table: str,
        join_paths: list[Any],
    ) -> list[PlanJoin]:
        if metric_table == target_table:
            return []
        for path in join_paths:
            if not isinstance(path, dict):
                continue
            route = path.get("path", []) or []
            if metric_table not in route or target_table not in route:
                continue
            joins = [
                PlanJoin(
                    left_table=str(edge.get("left_table", "")),
                    right_table=str(edge.get("right_table", "")),
                    condition_sql=str(edge.get("condition_sql", "")),
                    source=str(edge.get("source", "")),
                    notes=str(edge.get("notes", "")),
                )
                for edge in path.get("edges", [])
                if edge.get("condition_sql")
            ]
            if joins:
                return joins
        return []

    @staticmethod
    def _source_tables(metric_table: str, joins: list[PlanJoin]) -> list[str]:
        tables = [metric_table]
        for join in joins:
            if join.left_table not in tables:
                tables.append(join.left_table)
            if join.right_table not in tables:
                tables.append(join.right_table)
        return tables

    @staticmethod
    def _build_evidence(
        *,
        metric: dict[str, Any],
        table: str,
        instruction_matches: list[dict[str, Any]],
        om_table_candidates: list[dict[str, Any]],
        metabase_evidence: list[dict[str, Any]],
        joins: list[PlanJoin],
    ) -> list[PlanEvidence]:
        evidence = [
            PlanEvidence(
                kind="metric",
                source=metric.get("source", "semantic_model"),
                detail=f"{metric.get('field_name', metric.get('term', 'metric'))} on {table}",
                score=float(metric.get("similarity", 0.0)),
            )
        ]
        for join in joins:
            evidence.append(
                PlanEvidence(
                    kind="join_policy",
                    source=join.source or "semantic_relationship",
                    detail=f"{join.left_table} -> {join.right_table}",
                    score=1.0,
                )
            )
        for item in instruction_matches[:3]:
            evidence.append(
                PlanEvidence(
                    kind="instruction",
                    source=item.get("source", "semantic_model"),
                    detail=item.get("term", ""),
                    score=float(item.get("similarity", 0.0)),
                )
            )
        for item in om_table_candidates[:3]:
            if item.get("fqn") != table:
                continue
            evidence.append(
                PlanEvidence(
                    kind="openmetadata",
                    source="openmetadata",
                    detail=f"{table} quality={item.get('quality_status', 'UNKNOWN')}",
                    score=float(item.get("score", 0.0)),
                )
            )
        for item in metabase_evidence[:3]:
            if table not in item.get("tables", []):
                continue
            evidence.append(
                PlanEvidence(
                    kind="metabase",
                    source="metabase",
                    detail=item.get("name", ""),
                    score=float(item.get("score", 0.0)),
                )
            )
        return evidence
