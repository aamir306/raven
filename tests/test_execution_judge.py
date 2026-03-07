from __future__ import annotations

import pandas as pd

from src.raven.validation.execution_judge import ExecutionJudge


def test_execution_judge_accepts_filter_breakdown_percentage_result():
    judge = ExecutionJudge()
    df = pd.DataFrame(
        {
            "status": ["active", "inactive", "draft"],
            "status_percentage": [50.0, 30.0, 20.0],
        }
    )
    query_plan = {
        "intent": "FILTER_BREAKDOWN_PERCENTAGE",
        "group_by_sql": "cdp.ops.batches.status",
        "metric_name": "status_percentage",
    }

    result = judge.judge(df, query_plan)

    assert result.passed is True
    assert result.issues == []


def test_execution_judge_rejects_top_k_result_with_wrong_order():
    judge = ExecutionJudge()
    df = pd.DataFrame(
        {
            "center_name": ["A", "B", "C"],
            "total_revenue": [100.0, 200.0, 150.0],
        }
    )
    query_plan = {
        "intent": "TOP_K",
        "group_by_sql": "acme.crm.centers.center_name",
        "metric_name": "total_revenue",
        "order_direction": "DESC",
        "limit": 3,
    }

    result = judge.judge(df, query_plan)

    assert result.passed is False
    assert "metric_order_mismatch:desc" in result.issues


def test_execution_judge_rejects_single_row_intent_with_multiple_rows():
    judge = ExecutionJudge()
    df = pd.DataFrame({"total_revenue": [100.0, 200.0]})
    query_plan = {
        "intent": "KPI",
        "metric_name": "total_revenue",
    }

    result = judge.judge(df, query_plan)

    assert result.passed is False
    assert "unexpected_row_count:2" in result.issues
