from __future__ import annotations

from src.raven.grounding.value_resolver import ResolvedFilter
from src.raven.planning.query_plan import PlanJoin, QueryPlan
from src.raven.validation.query_plan_validator import QueryPlanValidator


def test_query_plan_validator_accepts_single_table_compiled_sql():
    validator = QueryPlanValidator()
    plan = QueryPlan(
        path_type="DETERMINISTIC_SINGLE_TABLE",
        intent="KPI",
        table="acme.sales.orders",
        metric_name="total_revenue",
        metric_sql="SUM(amount)",
        filters=[
            ResolvedFilter(
                table="acme.sales.orders",
                column="status",
                value="completed",
            )
        ],
    )

    result = validator.validate(plan.compiled_sql(), plan.to_dict())

    assert result.ok is True
    assert result.violations == []


def test_query_plan_validator_accepts_multi_table_compiled_sql():
    validator = QueryPlanValidator()
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
        limit=5,
    )

    result = validator.validate(plan.compiled_sql(), plan.to_dict())

    assert result.ok is True
    assert result.violations == []


def test_query_plan_validator_flags_missing_required_filter():
    validator = QueryPlanValidator()
    plan = QueryPlan(
        path_type="DETERMINISTIC_SINGLE_TABLE",
        intent="KPI",
        table="acme.sales.orders",
        metric_name="total_revenue",
        metric_sql="SUM(amount)",
        filters=[
            ResolvedFilter(
                table="acme.sales.orders",
                column="status",
                value="completed",
            )
        ],
    )

    bad_sql = "SELECT SUM(amount) AS total_revenue FROM acme.sales.orders"
    result = validator.validate(bad_sql, plan.to_dict())

    assert result.ok is False
    assert any(item.startswith("missing_filter:") for item in result.violations)


def test_query_plan_validator_flags_missing_top_k_limit_and_order():
    validator = QueryPlanValidator()
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

    bad_sql = """
        SELECT acme.crm.centers.center_name, SUM(amount) AS total_revenue
        FROM acme.sales.orders
        JOIN acme.crm.centers ON acme.sales.orders.center_id = acme.crm.centers.center_id
        GROUP BY acme.crm.centers.center_name
        ORDER BY total_revenue ASC
    """
    result = validator.validate(bad_sql, plan.to_dict())

    assert result.ok is False
    assert "missing_limit:5" in result.violations
    assert "missing_order:total_revenue desc" in result.violations


def test_query_plan_validator_flags_missing_time_bucket_alias_and_order():
    validator = QueryPlanValidator()
    plan = QueryPlan(
        path_type="DETERMINISTIC_SINGLE_TABLE",
        intent="TIME_SERIES",
        table="acme.sales.orders",
        metric_name="total_revenue",
        metric_sql="SUM(amount)",
        time_dimension="created_at",
        time_dimension_sql="acme.sales.orders.created_at",
        time_grain="month",
    )

    bad_sql = """
        SELECT DATE_TRUNC('month', acme.sales.orders.created_at), SUM(amount) AS total_revenue
        FROM acme.sales.orders
        GROUP BY DATE_TRUNC('month', acme.sales.orders.created_at)
    """
    result = validator.validate(bad_sql, plan.to_dict())

    assert result.ok is False
    assert "missing_time_bucket_alias:time_bucket" in result.violations
    assert "missing_time_order" in result.violations


def test_query_plan_validator_accepts_share_compiled_sql():
    validator = QueryPlanValidator()
    plan = QueryPlan(
        path_type="DETERMINISTIC_MULTI_TABLE",
        intent="SHARE",
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
        metric_name="total_revenue_share_pct",
        metric_sql="ROUND(100.0 * SUM(amount) / NULLIF(SUM(SUM(amount)) OVER (), 0), 2)",
        group_by="center_name",
        group_by_sql="acme.crm.centers.center_name",
    )

    result = validator.validate(plan.compiled_sql(), plan.to_dict())

    assert result.ok is True
    assert result.violations == []


def test_query_plan_validator_accepts_filter_percentage_compiled_sql():
    validator = QueryPlanValidator()
    plan = QueryPlan(
        path_type="DETERMINISTIC_SINGLE_TABLE",
        intent="FILTER_PERCENTAGE",
        table="cdp.sales.orders",
        source_tables=["cdp.sales.orders"],
        metric_name="cancelled_percentage",
        metric_sql="ROUND(100.0 * COUNT_IF(cdp.sales.orders.status = 'cancelled') / NULLIF(COUNT(*), 0), 2)",
        filters=[
            ResolvedFilter(
                sql_expression="cdp.sales.orders.created_at >= DATE_TRUNC('month', CURRENT_DATE)"
            )
        ],
    )

    result = validator.validate(plan.compiled_sql(), plan.to_dict())

    assert result.ok is True
    assert result.violations == []


def test_query_plan_validator_accepts_period_growth_compiled_sql():
    validator = QueryPlanValidator()
    plan = QueryPlan(
        path_type="DETERMINISTIC_SINGLE_TABLE",
        intent="PERIOD_GROWTH",
        table="cdp.sales.orders",
        source_tables=["cdp.sales.orders"],
        metric_name="total_revenue_last_month_to_this_month_growth_pct",
        metric_sql=(
            "ROUND(100.0 * ("
            "(SUM(CASE WHEN cdp.sales.orders.created_at >= DATE_TRUNC('month', CURRENT_DATE) THEN amount ELSE 0 END)) - "
            "(SUM(CASE WHEN cdp.sales.orders.created_at >= DATE_ADD('month', -1, DATE_TRUNC('month', CURRENT_DATE)) "
            "AND cdp.sales.orders.created_at < DATE_TRUNC('month', CURRENT_DATE) THEN amount ELSE 0 END))"
            ") / NULLIF((SUM(CASE WHEN cdp.sales.orders.created_at >= DATE_ADD('month', -1, DATE_TRUNC('month', CURRENT_DATE)) "
            "AND cdp.sales.orders.created_at < DATE_TRUNC('month', CURRENT_DATE) THEN amount ELSE 0 END)), 0), 2)"
        ),
    )

    result = validator.validate(plan.compiled_sql(), plan.to_dict())

    assert result.ok is True
    assert result.violations == []


def test_query_plan_validator_accepts_filter_breakdown_percentage_compiled_sql():
    validator = QueryPlanValidator()
    plan = QueryPlan(
        path_type="DETERMINISTIC_SINGLE_TABLE",
        intent="FILTER_BREAKDOWN_PERCENTAGE",
        table="cdp.ops.batches",
        source_tables=["cdp.ops.batches"],
        metric_name="status_percentage",
        metric_sql="ROUND(100.0 * COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (), 0), 2)",
        group_by="status",
        group_by_sql="cdp.ops.batches.status",
        filters=[
            ResolvedFilter(
                sql_expression="cdp.ops.batches.status IN ('active', 'inactive', 'draft')"
            )
        ],
    )

    result = validator.validate(plan.compiled_sql(), plan.to_dict())

    assert result.ok is True
    assert result.violations == []


def test_query_plan_validator_accepts_grouped_period_growth_compiled_sql():
    validator = QueryPlanValidator()
    plan = QueryPlan(
        path_type="DETERMINISTIC_MULTI_TABLE",
        intent="GROUPED_PERIOD_GROWTH",
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
        metric_name="total_revenue_last_month_to_this_month_growth_pct",
        metric_sql=(
            "ROUND(100.0 * ("
            "(SUM(CASE WHEN acme.sales.orders.created_at >= DATE_TRUNC('month', CURRENT_DATE) THEN amount ELSE 0 END)) - "
            "(SUM(CASE WHEN acme.sales.orders.created_at >= DATE_ADD('month', -1, DATE_TRUNC('month', CURRENT_DATE)) "
            "AND acme.sales.orders.created_at < DATE_TRUNC('month', CURRENT_DATE) THEN amount ELSE 0 END))"
            ") / NULLIF((SUM(CASE WHEN acme.sales.orders.created_at >= DATE_ADD('month', -1, DATE_TRUNC('month', CURRENT_DATE)) "
            "AND acme.sales.orders.created_at < DATE_TRUNC('month', CURRENT_DATE) THEN amount ELSE 0 END)), 0), 2)"
        ),
        group_by="center_name",
        group_by_sql="acme.crm.centers.center_name",
    )

    result = validator.validate(plan.compiled_sql(), plan.to_dict())

    assert result.ok is True
    assert result.violations == []


def test_query_plan_validator_accepts_filter_breakdown_aggregate_compiled_sql():
    validator = QueryPlanValidator()
    plan = QueryPlan(
        path_type="DETERMINISTIC_SINGLE_TABLE",
        intent="FILTER_BREAKDOWN_AGG",
        table="cdp.sales.orders",
        source_tables=["cdp.sales.orders"],
        metric_name="total_revenue",
        metric_sql="SUM(amount)",
        group_by="status",
        group_by_sql="cdp.sales.orders.status",
        filters=[
            ResolvedFilter(
                sql_expression="cdp.sales.orders.created_at >= DATE_TRUNC('month', CURRENT_DATE)"
            ),
            ResolvedFilter(
                sql_expression="cdp.sales.orders.status IN ('active', 'inactive')"
            ),
        ],
    )

    result = validator.validate(plan.compiled_sql(), plan.to_dict())

    assert result.ok is True
    assert result.violations == []


def test_query_plan_validator_accepts_filter_breakdown_count_compiled_sql():
    validator = QueryPlanValidator()
    plan = QueryPlan(
        path_type="DETERMINISTIC_SINGLE_TABLE",
        intent="FILTER_BREAKDOWN_COUNT",
        table="cdp.ops.batches",
        source_tables=["cdp.ops.batches"],
        metric_name="status_count",
        metric_sql="COUNT(*)",
        group_by="status",
        group_by_sql="cdp.ops.batches.status",
        filters=[
            ResolvedFilter(
                sql_expression="cdp.ops.batches.status IN ('active', 'inactive', 'draft')"
            )
        ],
    )

    result = validator.validate(plan.compiled_sql(), plan.to_dict())

    assert result.ok is True
    assert result.violations == []
