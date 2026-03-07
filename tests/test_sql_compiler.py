from __future__ import annotations

from src.raven.grounding.value_resolver import ResolvedFilter
from src.raven.planning.query_plan import PlanJoin, QueryPlan


def test_sql_compiler_emits_explicit_group_by_and_order():
    plan = QueryPlan(
        path_type="DETERMINISTIC_MULTI_TABLE",
        intent="TOP_K",
        table="acme.sales.orders",
        source_tables=["acme.sales.orders", "acme.crm.centers"],
        joins=[
            PlanJoin(
                left_table="acme.sales.orders",
                right_table="acme.crm.centers",
                condition_sql="acme.sales.orders.center_id = acme.crm.centers.center_id",
                source="semantic_model",
            )
        ],
        metric_name="total_revenue",
        metric_sql="SUM(amount)",
        group_by="center_name",
        group_by_sql="acme.crm.centers.center_name",
        order_direction="DESC",
        limit=5,
    )

    sql = plan.compiled_sql()

    assert "SELECT acme.crm.centers.center_name, SUM(amount) AS total_revenue" in sql
    assert "GROUP BY acme.crm.centers.center_name" in sql
    assert "ORDER BY total_revenue DESC" in sql
    assert "LIMIT 5" in sql


def test_sql_compiler_emits_explicit_time_bucket_grouping():
    plan = QueryPlan(
        path_type="DETERMINISTIC_SINGLE_TABLE",
        intent="TIME_SERIES",
        table="acme.sales.orders",
        metric_name="total_revenue",
        metric_sql="SUM(amount)",
        time_dimension="created_at",
        time_dimension_sql="acme.sales.orders.created_at",
        time_grain="month",
        filters=[
            ResolvedFilter(
                table="acme.sales.orders",
                column="status",
                value="completed",
            )
        ],
    )

    sql = plan.compiled_sql()

    assert "DATE_TRUNC('month', acme.sales.orders.created_at) AS time_bucket" in sql
    assert "WHERE acme.sales.orders.status = 'completed'" in sql
    assert "GROUP BY DATE_TRUNC('month', acme.sales.orders.created_at)" in sql
    assert "ORDER BY DATE_TRUNC('month', acme.sales.orders.created_at) ASC" in sql
