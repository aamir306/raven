"""
Compile deterministic SQL AST objects to Trino SQL text.
"""

from __future__ import annotations

from .ast_builder import OrderItem, QueryAst, SelectItem


def _render_select(item: SelectItem) -> str:
    if item.alias:
        return f"{item.expression} AS {item.alias}"
    return item.expression


def _render_order(item: OrderItem) -> str:
    direction = (item.direction or "ASC").upper()
    return f"{item.expression} {direction}"


def compile_trino_sql(query: QueryAst) -> str:
    lines = [
        "SELECT " + ", ".join(_render_select(item) for item in query.select_items),
        f"FROM {query.from_table}",
    ]

    for join in query.joins:
        lines.append(f"JOIN {join.table} ON {join.condition_sql}")

    if query.where_clauses:
        lines.append("WHERE " + " AND ".join(query.where_clauses))

    if query.group_by:
        lines.append("GROUP BY " + ", ".join(query.group_by))

    if query.order_by:
        lines.append("ORDER BY " + ", ".join(_render_order(item) for item in query.order_by))

    if query.limit:
        lines.append(f"LIMIT {query.limit}")

    return "\n".join(lines)
