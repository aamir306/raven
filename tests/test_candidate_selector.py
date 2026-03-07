from __future__ import annotations

import pytest

from src.raven.validation.candidate_selector import CandidateSelector


class _DummyOpenAI:
    async def complete(self, *args, **kwargs):
        return "WINNER: A"


class _DummyTrino:
    def explain(self, sql: str):
        return "ok"


@pytest.mark.asyncio
async def test_candidate_selector_prefers_fewest_plan_violations_before_pairwise():
    selector = CandidateSelector(_DummyOpenAI(), _DummyTrino())
    captured: list[str] = []

    async def _fake_pairwise(candidates, question, pruned_schema):
        captured.extend(candidates)
        return candidates[0]

    async def _fake_taxonomy(sql, question, pruned_schema, content_awareness):
        return []

    selector._pairwise_select = _fake_pairwise  # type: ignore[method-assign]
    selector._taxonomy_check = _fake_taxonomy  # type: ignore[method-assign]

    query_plan = {
        "intent": "TOP_K",
        "table": "acme.sales.orders",
        "source_tables": ["acme.sales.orders", "acme.crm.centers"],
        "joins": [
            {
                "left_table": "acme.sales.orders",
                "right_table": "acme.crm.centers",
                "condition_sql": "acme.sales.orders.center_id = acme.crm.centers.center_id",
            }
        ],
        "metric_name": "total_revenue",
        "metric_sql": "SUM(amount)",
        "group_by_sql": "acme.crm.centers.center_name",
        "order_direction": "DESC",
        "limit": 5,
    }

    candidates = [
        """
        SELECT acme.crm.centers.center_name, SUM(amount) AS total_revenue
        FROM acme.sales.orders
        JOIN acme.crm.centers ON acme.sales.orders.center_id = acme.crm.centers.center_id
        GROUP BY acme.crm.centers.center_name
        ORDER BY total_revenue ASC
        """,
        """
        SELECT acme.crm.centers.center_name, SUM(amount) AS total_revenue
        FROM acme.sales.orders
        JOIN acme.crm.centers ON acme.sales.orders.center_id = acme.crm.centers.center_id
        GROUP BY acme.crm.centers.center_name
        ORDER BY total_revenue DESC
        LIMIT 5
        """,
        """
        SELECT SUM(amount) AS total_revenue
        FROM acme.sales.orders
        """,
    ]

    result = await selector.select_best(
        question="top 5 centers by revenue",
        candidates=candidates,
        pruned_schema="TABLE: acme.sales.orders\n  - amount (double)",
        content_awareness=[],
        query_plan=query_plan,
    )

    assert result["sql"] == candidates[1]
    assert captured == [candidates[1]]


@pytest.mark.asyncio
async def test_candidate_selector_rejects_single_candidate_with_hard_plan_violation():
    selector = CandidateSelector(_DummyOpenAI(), _DummyTrino())

    async def _fake_taxonomy(sql, question, pruned_schema, content_awareness):
        return []

    selector._taxonomy_check = _fake_taxonomy  # type: ignore[method-assign]

    query_plan = {
        "intent": "TOP_K",
        "table": "acme.sales.orders",
        "source_tables": ["acme.sales.orders", "acme.crm.centers"],
        "joins": [
            {
                "left_table": "acme.sales.orders",
                "right_table": "acme.crm.centers",
                "condition_sql": "acme.sales.orders.center_id = acme.crm.centers.center_id",
            }
        ],
        "metric_name": "total_revenue",
        "metric_sql": "SUM(amount)",
        "group_by_sql": "acme.crm.centers.center_name",
        "order_direction": "DESC",
        "limit": 5,
    }

    bad_candidate = """
        SELECT SUM(amount) AS total_revenue
        FROM acme.sales.orders
    """

    result = await selector.select_best(
        question="top 5 centers by revenue",
        candidates=[bad_candidate],
        pruned_schema="TABLE: acme.sales.orders\n  - amount (double)",
        content_awareness=[],
        query_plan=query_plan,
    )

    assert result["sql"] == ""
    assert result["rejected"] is True
    assert "missing_table:acme.crm.centers" in result["rejection_reasons"]


@pytest.mark.asyncio
async def test_candidate_selector_rejects_when_all_candidates_have_hard_plan_violations():
    selector = CandidateSelector(_DummyOpenAI(), _DummyTrino())

    async def _unexpected_pairwise(candidates, question, pruned_schema):
        raise AssertionError("pairwise selection should not run when all candidates are structurally invalid")

    selector._pairwise_select = _unexpected_pairwise  # type: ignore[method-assign]

    query_plan = {
        "intent": "TOP_K",
        "table": "acme.sales.orders",
        "source_tables": ["acme.sales.orders", "acme.crm.centers"],
        "joins": [
            {
                "left_table": "acme.sales.orders",
                "right_table": "acme.crm.centers",
                "condition_sql": "acme.sales.orders.center_id = acme.crm.centers.center_id",
            }
        ],
        "metric_name": "total_revenue",
        "metric_sql": "SUM(amount)",
        "group_by_sql": "acme.crm.centers.center_name",
        "order_direction": "DESC",
        "limit": 5,
    }

    candidates = [
        "SELECT SUM(amount) AS total_revenue FROM acme.sales.orders",
        "SELECT acme.crm.centers.center_name FROM acme.crm.centers",
    ]

    result = await selector.select_best(
        question="top 5 centers by revenue",
        candidates=candidates,
        pruned_schema="TABLE: acme.sales.orders\n  - amount (double)",
        content_awareness=[],
        query_plan=query_plan,
    )

    assert result["sql"] == ""
    assert result["rejected"] is True
    assert result["confidence"] == "LOW"
