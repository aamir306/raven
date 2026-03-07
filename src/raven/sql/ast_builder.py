"""
Typed SQL AST builder for deterministic query plans.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..planning.query_plan import QueryPlan


def _time_expr(grain: str, column: str) -> str:
    grain = (grain or "").lower()
    if grain == "day":
        return f"DATE({column})"
    if grain in {"week", "month", "year"}:
        return f"DATE_TRUNC('{grain}', {column})"
    return column


@dataclass(frozen=True)
class SelectItem:
    expression: str
    alias: str = ""


@dataclass(frozen=True)
class JoinItem:
    table: str
    condition_sql: str


@dataclass(frozen=True)
class OrderItem:
    expression: str
    direction: str = "ASC"


@dataclass
class QueryAst:
    from_table: str
    select_items: list[SelectItem] = field(default_factory=list)
    joins: list[JoinItem] = field(default_factory=list)
    where_clauses: list[str] = field(default_factory=list)
    group_by: list[str] = field(default_factory=list)
    order_by: list[OrderItem] = field(default_factory=list)
    limit: int | None = None


def build_query_ast(plan: QueryPlan) -> QueryAst:
    select_items: list[SelectItem] = []
    group_by: list[str] = []
    order_by: list[OrderItem] = []

    time_dimension_sql = plan.time_dimension_sql or plan.time_dimension
    group_by_sql = plan.group_by_sql or plan.group_by

    if time_dimension_sql:
        time_expr = _time_expr(plan.time_grain or "day", time_dimension_sql)
        select_items.append(SelectItem(expression=time_expr, alias="time_bucket"))
        group_by.append(time_expr)
        order_by.append(OrderItem(expression=time_expr, direction="ASC"))
    elif group_by_sql:
        select_items.append(SelectItem(expression=group_by_sql))
        group_by.append(group_by_sql)
        order_by.append(
            OrderItem(
                expression=plan.metric_name,
                direction=(plan.order_direction or "DESC").upper(),
            )
        )

    select_items.append(SelectItem(expression=plan.metric_sql, alias=plan.metric_name))

    joined_tables = {plan.table}
    joins: list[JoinItem] = []
    pending = list(plan.joins)
    while pending:
        progress = False
        for join in pending[:]:
            if join.left_table in joined_tables and join.right_table not in joined_tables:
                joins.append(
                    JoinItem(table=join.right_table, condition_sql=join.condition_sql)
                )
                joined_tables.add(join.right_table)
                pending.remove(join)
                progress = True
            elif join.right_table in joined_tables and join.left_table not in joined_tables:
                joins.append(
                    JoinItem(table=join.left_table, condition_sql=join.condition_sql)
                )
                joined_tables.add(join.left_table)
                pending.remove(join)
                progress = True
            elif join.left_table in joined_tables and join.right_table in joined_tables:
                pending.remove(join)
                progress = True
        if not progress:
            break

    where_clauses = [flt.to_sql() for flt in plan.filters if flt.to_sql()]

    return QueryAst(
        from_table=plan.table,
        select_items=select_items,
        joins=joins,
        where_clauses=where_clauses,
        group_by=group_by,
        order_by=order_by,
        limit=plan.limit,
    )
