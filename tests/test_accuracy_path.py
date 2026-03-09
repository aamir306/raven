from __future__ import annotations

import asyncio
from pathlib import Path
import textwrap

import pandas as pd

from src.raven.focus import FocusContext
from src.raven.grounding import ValueResolver
from src.raven.planning import DeterministicPlanner
from src.raven.schema.join_policy import JoinPolicy
from src.raven.schema.schema_selector import SchemaSelector
from src.raven.semantic_assets import SemanticModelStore


class _DummyOpenAI:
    async def complete(self, *args, **kwargs):
        return ""

    async def embed(self, *args, **kwargs):
        return [0.0]

    def get_cost_summary(self):
        return {"total_usd": 0.0}


def test_semantic_store_exact_match_returns_trusted_query(tmp_path: Path):
    model_path = tmp_path / "semantic_model.yaml"
    model_path.write_text(
        textwrap.dedent(
            """
            name: test_model
            tables:
              - name: cdp.sales.orders
                description: Orders fact table
                synonyms: [orders, purchases]
                metrics:
                  - name: total_revenue
                    description: Total revenue
                    sql: SUM(amount)
            verified_queries:
              - question: "What is the total revenue?"
                sql: "SELECT SUM(amount) AS total_revenue FROM cdp.sales.orders"
                notes: canonical revenue query
                category: revenue
            """
        )
    )

    store = SemanticModelStore(model_path)
    result = store.retrieve("What is the total revenue?")

    assert result["trusted_query"] is not None
    assert result["trusted_query"]["exact_match"] is True
    assert result["trusted_query"]["source"] == "semantic_model"
    assert "cdp.sales.orders" in result["preferred_tables"]


def test_semantic_store_uses_focus_verified_queries(tmp_path: Path):
    model_path = tmp_path / "semantic_model.yaml"
    model_path.write_text(
        textwrap.dedent(
            """
            name: test_model
            tables: []
            verified_queries: []
            """
        )
    )

    focus = FocusContext(
        type="document",
        name="Revenue Focus",
        source_id="focus-1",
        tables=["cdp.sales.orders"],
        verified_queries=[
            {
                "question": "Daily revenue for the last 7 days",
                "sql": "SELECT order_date, SUM(amount) FROM cdp.sales.orders GROUP BY 1",
                "notes": "trusted dashboard query",
            }
        ],
    )

    store = SemanticModelStore(model_path)
    result = store.retrieve("Daily revenue for the last 7 days", focus=focus)

    assert result["trusted_query"] is not None
    assert result["trusted_query"]["source"] == "focus"
    assert result["trusted_query"]["exact_match"] is True
    assert result["trusted_query"]["tables_used"] == ["cdp.sales.orders"]


def test_schema_selector_merges_preferred_tables_into_candidate_columns():
    selector = SchemaSelector(_DummyOpenAI(), pgvector=None)
    selector.set_column_catalog(
        {
            "cdp.sales.orders": [
                {"name": "order_id", "type": "bigint"},
                {"name": "amount", "type": "double"},
            ]
        }
    )

    merged = selector._merge_preferred_tables(
        candidate_columns=["cdp.analytics.users.user_id"],
        preferred_tables=["cdp.sales.orders"],
    )

    assert "cdp.sales.orders.order_id" in merged
    assert "cdp.sales.orders.amount" in merged


def test_schema_selector_promotes_semantic_and_evidence_columns_when_llm_empty(
    tmp_path: Path,
):
    model_path = tmp_path / "semantic_model.yaml"
    model_path.write_text(
        textwrap.dedent(
            """
            name: test_model
            tables:
              - name: acme.sales.orders
                description: Orders fact table
                synonyms: [orders, revenue]
                metrics:
                  - name: total_revenue
                    description: Total revenue
                    sql: SUM(amount)
              - name: acme.crm.centers
                description: Center dimension
                synonyms: [center, centers]
                dimensions:
                  - name: center_name
                    description: Center name
            relationships:
              - left_table: acme.sales.orders
                right_table: acme.crm.centers
                join_columns:
                  left: center_id
                  right: center_id
            verified_queries:
              - question: "top centers by revenue"
                sql: |
                  SELECT c.center_name, SUM(o.amount) AS total_revenue
                  FROM acme.sales.orders o
                  JOIN acme.crm.centers c ON o.center_id = c.center_id
                  GROUP BY c.center_name
                  ORDER BY total_revenue DESC
                  LIMIT 10
                notes: canonical grouped revenue query
                category: revenue
            """
        )
    )

    store = SemanticModelStore(model_path)
    semantic = store.retrieve("show best centers by total revenue")

    selector = SchemaSelector(_DummyOpenAI(), pgvector=None, semantic_store=store)
    selector.set_column_catalog(
        {
            "acme.sales.orders": [
                {"name": "amount", "type": "double"},
                {"name": "center_id", "type": "bigint"},
            ],
            "acme.crm.centers": [
                {"name": "center_id", "type": "bigint"},
                {"name": "center_name", "type": "varchar"},
            ],
        }
    )

    async def _empty_filter(*args, **kwargs):
        return []

    selector.column_filter.filter = _empty_filter  # type: ignore[method-assign]

    result = asyncio.run(
        selector.select(
            question="show best centers by total revenue",
            entity_matches=[],
            glossary_matches=semantic["glossary_matches"],
            similar_queries=semantic["verified_queries"],
            doc_snippets=[],
            content_awareness=[],
            preferred_tables=semantic["preferred_tables"],
            metabase_evidence=semantic["metabase_evidence"],
        )
    )

    assert "acme.sales.orders.amount" in result["candidate_columns"]
    assert "acme.crm.centers.center_name" in result["candidate_columns"]
    assert "acme.sales.orders.center_id" in result["candidate_columns"]
    assert "acme.crm.centers.center_id" in result["candidate_columns"]
    assert "acme.sales.orders" in result["selected_tables"]
    assert "acme.crm.centers" in result["selected_tables"]


def test_schema_selector_pruner_restores_required_columns(tmp_path: Path):
    model_path = tmp_path / "semantic_model.yaml"
    model_path.write_text(
        textwrap.dedent(
            """
            name: test_model
            tables:
              - name: acme.sales.orders
                description: Orders fact table
                synonyms: [orders, revenue]
                metrics:
                  - name: total_revenue
                    description: Total revenue
                    sql: SUM(amount)
              - name: acme.crm.centers
                description: Center dimension
                synonyms: [center, centers]
                dimensions:
                  - name: center_name
                    description: Center name
            relationships:
              - left_table: acme.sales.orders
                right_table: acme.crm.centers
                join_columns:
                  left: center_id
                  right: center_id
            verified_queries: []
            """
        )
    )

    class _PruneDroppingOpenAI(_DummyOpenAI):
        async def complete(self, *args, **kwargs):
            if kwargs.get("stage_name") == "ss_column_prune":
                return textwrap.dedent(
                    """
                    TABLE: acme.sales.orders
                      - created_at (timestamp)
                    TABLE: acme.crm.centers
                      - region_name (varchar)
                    """
                ).strip()
            return ""

    store = SemanticModelStore(model_path)
    semantic = store.retrieve("top centers by revenue")
    selector = SchemaSelector(_PruneDroppingOpenAI(), pgvector=None, semantic_store=store)
    selector.set_column_catalog(
        {
            "acme.sales.orders": [
                {"name": "amount", "type": "double"},
                {"name": "center_id", "type": "bigint"},
                {"name": "created_at", "type": "timestamp"},
            ],
            "acme.crm.centers": [
                {"name": "center_id", "type": "bigint"},
                {"name": "center_name", "type": "varchar"},
                {"name": "region_name", "type": "varchar"},
            ],
        }
    )

    async def _empty_filter(*args, **kwargs):
        return []

    selector.column_filter.filter = _empty_filter  # type: ignore[method-assign]

    result = asyncio.run(
        selector.select(
            question="top centers by revenue",
            entity_matches=[],
            glossary_matches=semantic["glossary_matches"],
            similar_queries=[],
            doc_snippets=[],
            content_awareness=[],
            preferred_tables=semantic["preferred_tables"],
            metabase_evidence=[],
        )
    )

    pruned = result["pruned_schema"]
    assert "  - amount (double)" in pruned
    assert "  - center_name (varchar)" in pruned
    assert "  - center_id (bigint)" in pruned


def test_semantic_store_promotes_metabase_focus_queries(tmp_path: Path):
    model_path = tmp_path / "semantic_model.yaml"
    model_path.write_text(
        textwrap.dedent(
            """
            name: test_model
            tables: []
            verified_queries: []
            """
        )
    )

    focus = FocusContext(
        type="dashboard",
        name="Revenue Dashboard",
        source_id="dash-1",
        tables=["cdp.sales.orders"],
        verified_queries=[
            {
                "question": "Revenue this month",
                "sql": "SELECT SUM(amount) FROM cdp.sales.orders",
                "card_id": 42,
            }
        ],
        dashboard_cards=[
            {
                "card_id": 42,
                "name": "Revenue this month",
                "sql": "SELECT SUM(amount) FROM cdp.sales.orders",
                "tables": ["cdp.sales.orders"],
                "display": "scalar",
            }
        ],
    )

    store = SemanticModelStore(model_path)
    result = store.retrieve("Revenue this month", focus=focus)

    assert result["trusted_query"] is not None
    assert result["trusted_query"]["source"] == "metabase"
    assert result["metabase_evidence"][0]["source"] == "metabase"


def test_value_resolver_grounds_semantic_filters(tmp_path: Path):
    model_path = tmp_path / "semantic_model.yaml"
    model_path.write_text(
        textwrap.dedent(
            """
            name: test_model
            tables:
              - name: cdp.sales.orders
                description: Orders fact table
                synonyms: [orders]
                dimensions:
                  - name: status
                    description: Order status
                    values: [active, cancelled]
                time_dimensions:
                  - name: created_at
                    description: Creation timestamp
                metrics:
                  - name: total_revenue
                    description: Total revenue
                    sql: SUM(amount)
            business_rules:
              - term: current_month
                definition: Current month only
                sql_fragment: "created_at >= DATE_TRUNC('month', CURRENT_DATE)"
                synonyms: [this month]
            verified_queries: []
            """
        )
    )

    store = SemanticModelStore(model_path)
    semantic = store.retrieve("What is the total revenue this month for active orders?")
    resolver = ValueResolver(store)
    grounding = resolver.resolve(
        question="What is the total revenue this month for active orders?",
        content_awareness=[],
        preferred_tables=["cdp.sales.orders"],
        instruction_matches=semantic["instruction_matches"],
        focus=None,
    )

    sql_filters = [item.to_sql() for item in grounding.filters]
    assert "created_at >= DATE_TRUNC('month', CURRENT_DATE)" in sql_filters
    assert "cdp.sales.orders.status = 'active'" in sql_filters


def test_deterministic_planner_builds_single_table_metric_sql(tmp_path: Path):
    model_path = tmp_path / "semantic_model.yaml"
    model_path.write_text(
        textwrap.dedent(
            """
            name: test_model
            tables:
              - name: cdp.sales.orders
                description: Orders fact table
                synonyms: [orders, revenue]
                dimensions:
                  - name: status
                    description: Order status
                    values: [active, cancelled]
                time_dimensions:
                  - name: created_at
                    description: Creation timestamp
                metrics:
                  - name: total_revenue
                    description: Total revenue
                    sql: SUM(amount)
            business_rules:
              - term: current_month
                definition: Current month only
                sql_fragment: "created_at >= DATE_TRUNC('month', CURRENT_DATE)"
                synonyms: [this month]
            verified_queries: []
            """
        )
    )

    focus = FocusContext(
        type="dashboard",
        name="Revenue Dashboard",
        source_id="dash-1",
        tables=["cdp.sales.orders"],
        verified_queries=[],
        dashboard_cards=[
            {
                "card_id": 7,
                "name": "Total revenue this month",
                "sql": "SELECT SUM(amount) FROM cdp.sales.orders WHERE created_at >= DATE_TRUNC('month', CURRENT_DATE)",
                "tables": ["cdp.sales.orders"],
                "display": "scalar",
            }
        ],
    )

    store = SemanticModelStore(model_path)
    semantic = store.retrieve("What is the total revenue this month?", focus=focus)
    resolver = ValueResolver(store)
    grounding = resolver.resolve(
        question="What is the total revenue this month?",
        content_awareness=[],
        preferred_tables=semantic["preferred_tables"],
        instruction_matches=semantic["instruction_matches"],
        focus=focus,
    )
    planner = DeterministicPlanner(store)
    plan = planner.plan(
        question="What is the total revenue this month?",
        glossary_matches=semantic["glossary_matches"],
        selected_tables=["cdp.sales.orders"],
        preferred_tables=semantic["preferred_tables"],
        resolved_filters=[item.to_dict() for item in grounding.filters],
        instruction_matches=semantic["instruction_matches"],
        om_table_candidates=[
            {"fqn": "cdp.sales.orders", "score": 0.9, "quality_status": "PASS"}
        ],
        metabase_evidence=semantic["metabase_evidence"],
        join_paths=[],
    )

    assert plan is not None
    sql = plan.compiled_sql()
    assert "FROM cdp.sales.orders" in sql
    assert "SUM(amount) AS total_revenue" in sql
    assert "created_at >= DATE_TRUNC('month', CURRENT_DATE)" in sql


def test_semantic_keywords_are_model_driven(tmp_path: Path):
    model_path = tmp_path / "semantic_model.yaml"
    model_path.write_text(
        textwrap.dedent(
            """
            name: test_model
            tables:
              - name: acme.billing.invoices
                description: Billing invoices
                synonyms: [invoices, billing]
                dimensions:
                  - name: invoice_status
                    description: Invoice state
                metrics:
                  - name: invoice_total
                    description: Total invoiced amount
                    sql: SUM(amount)
            verified_queries: []
            """
        )
    )

    store = SemanticModelStore(model_path)
    pattern = store.keyword_pattern()

    assert pattern.search("show invoice total by invoice status")
    assert "invoice" in store.data_keywords()


def test_join_policy_resolves_semantic_relationships_with_aliases(tmp_path: Path):
    model_path = tmp_path / "semantic_model.yaml"
    model_path.write_text(
        textwrap.dedent(
            """
            name: test_model
            tables:
              - name: acme.sales.orders
                description: Orders fact
                metrics:
                  - name: total_revenue
                    description: Total revenue
                    sql: SUM(amount)
              - name: acme.crm.centers
                description: Center dimension
                dimensions:
                  - name: center_name
                    description: Center name
            relationships:
              - left_table: acme.sales.orders
                right_table: acme.crm.centers
                join_columns:
                  left: center_id
                  right: center_id
                cast_required: false
            verified_queries: []
            """
        )
    )

    store = SemanticModelStore(model_path)
    policy = JoinPolicy(store)
    edges = policy.find_path(
        "acme.sales.orders",
        "acme.crm.centers",
        available_tables={"sales.orders", "crm.centers"},
    )

    assert len(edges) == 1
    assert edges[0].left_table == "sales.orders"
    assert edges[0].right_table == "crm.centers"
    assert edges[0].condition_sql == "sales.orders.center_id = crm.centers.center_id"


def test_deterministic_planner_builds_multi_table_grouped_sql(tmp_path: Path):
    model_path = tmp_path / "semantic_model.yaml"
    model_path.write_text(
        textwrap.dedent(
            """
            name: test_model
            tables:
              - name: acme.sales.orders
                description: Orders fact table
                synonyms: [orders, revenue]
                dimensions:
                  - name: order_status
                    description: Order status
                metrics:
                  - name: total_revenue
                    description: Total revenue
                    sql: SUM(amount)
              - name: acme.crm.centers
                description: Center dimension
                synonyms: [centers, center]
                dimensions:
                  - name: center_name
                    description: Center name
            relationships:
              - left_table: acme.sales.orders
                right_table: acme.crm.centers
                join_columns:
                  left: center_id
                  right: center_id
                cast_required: false
            verified_queries: []
            """
        )
    )

    store = SemanticModelStore(model_path)
    semantic = store.retrieve("top centers by revenue")
    policy = JoinPolicy(store)
    selected_tables, join_paths = policy.connect_tables(
        ["acme.sales.orders", "acme.crm.centers"]
    )
    planner = DeterministicPlanner(store)
    plan = planner.plan(
        question="top centers by revenue",
        glossary_matches=semantic["glossary_matches"],
        selected_tables=selected_tables,
        preferred_tables=semantic["preferred_tables"],
        resolved_filters=[],
        instruction_matches=semantic["instruction_matches"],
        om_table_candidates=[],
        metabase_evidence=[],
        join_paths=join_paths,
    )

    assert plan is not None
    sql = plan.compiled_sql()
    assert plan.path_type == "DETERMINISTIC_MULTI_TABLE"
    assert "FROM acme.sales.orders" in sql
    assert "JOIN acme.crm.centers ON acme.sales.orders.center_id = acme.crm.centers.center_id" in sql
    assert "acme.crm.centers.center_name" in sql
    assert "SUM(amount) AS total_revenue" in sql


def test_deterministic_planner_builds_share_sql(tmp_path: Path):
    model_path = tmp_path / "semantic_model.yaml"
    model_path.write_text(
        textwrap.dedent(
            """
            name: test_model
            tables:
              - name: acme.sales.orders
                description: Orders fact table
                synonyms: [orders, revenue]
                metrics:
                  - name: total_revenue
                    description: Total revenue
                    sql: SUM(amount)
              - name: acme.crm.centers
                description: Center dimension
                synonyms: [centers, center]
                dimensions:
                  - name: center_name
                    description: Center name
            relationships:
              - left_table: acme.sales.orders
                right_table: acme.crm.centers
                join_columns:
                  left: center_id
                  right: center_id
                cast_required: false
            verified_queries: []
            """
        )
    )

    store = SemanticModelStore(model_path)
    semantic = store.retrieve("revenue contribution by center as percentage of total")
    policy = JoinPolicy(store)
    selected_tables, join_paths = policy.connect_tables(
        ["acme.sales.orders", "acme.crm.centers"]
    )
    planner = DeterministicPlanner(store)
    plan = planner.plan(
        question="revenue contribution by center as percentage of total",
        glossary_matches=semantic["glossary_matches"],
        selected_tables=selected_tables,
        preferred_tables=semantic["preferred_tables"],
        resolved_filters=[],
        instruction_matches=semantic["instruction_matches"],
        om_table_candidates=[],
        metabase_evidence=[],
        join_paths=join_paths,
    )

    assert plan is not None
    assert plan.intent == "SHARE"
    sql = plan.compiled_sql()
    assert "acme.crm.centers.center_name" in sql
    assert "ROUND(100.0 * SUM(amount) / NULLIF(SUM(SUM(amount)) OVER (), 0), 2) AS total_revenue_share_pct" in sql
    assert "GROUP BY acme.crm.centers.center_name" in sql


def test_deterministic_planner_builds_filter_percentage_sql(tmp_path: Path):
    model_path = tmp_path / "semantic_model.yaml"
    model_path.write_text(
        textwrap.dedent(
            """
            name: test_model
            tables:
              - name: cdp.sales.orders
                description: Orders fact table
                synonyms: [orders]
                dimensions:
                  - name: status
                    description: Order status
                    values: [completed, cancelled]
                time_dimensions:
                  - name: created_at
                    description: Creation timestamp
            business_rules:
              - term: current_month
                definition: Current month only
                sql_fragment: "cdp.sales.orders.created_at >= DATE_TRUNC('month', CURRENT_DATE)"
                synonyms: [this month]
            verified_queries: []
            """
        )
    )

    store = SemanticModelStore(model_path)
    semantic = store.retrieve("What percentage of orders are cancelled this month?")
    resolver = ValueResolver(store)
    grounding = resolver.resolve(
        question="What percentage of orders are cancelled this month?",
        content_awareness=[],
        preferred_tables=semantic["preferred_tables"],
        instruction_matches=semantic["instruction_matches"],
        focus=None,
    )
    planner = DeterministicPlanner(store)
    plan = planner.plan(
        question="What percentage of orders are cancelled this month?",
        glossary_matches=semantic["glossary_matches"],
        selected_tables=["cdp.sales.orders"],
        preferred_tables=semantic["preferred_tables"],
        resolved_filters=[item.to_dict() for item in grounding.filters],
        instruction_matches=semantic["instruction_matches"],
        om_table_candidates=[],
        metabase_evidence=[],
        join_paths=[],
    )

    assert plan is not None
    assert plan.intent == "FILTER_PERCENTAGE"
    sql = plan.compiled_sql()
    assert "COUNT_IF(cdp.sales.orders.status = 'cancelled')" in sql
    assert "NULLIF(COUNT(*), 0)" in sql
    assert "AS cancelled_percentage" in sql
    assert "WHERE cdp.sales.orders.created_at >= DATE_TRUNC('month', CURRENT_DATE)" in sql


def test_deterministic_planner_builds_period_growth_sql(tmp_path: Path):
    model_path = tmp_path / "semantic_model.yaml"
    model_path.write_text(
        textwrap.dedent(
            """
            name: test_model
            tables:
              - name: cdp.sales.orders
                description: Orders fact table
                synonyms: [orders, revenue]
                time_dimensions:
                  - name: created_at
                    description: Creation timestamp
                metrics:
                  - name: total_revenue
                    description: Total revenue
                    sql: SUM(amount)
            verified_queries: []
            """
        )
    )

    store = SemanticModelStore(model_path)
    semantic = store.retrieve("Revenue growth from last month to this month")
    planner = DeterministicPlanner(store)
    plan = planner.plan(
        question="Revenue growth from last month to this month",
        glossary_matches=semantic["glossary_matches"],
        selected_tables=["cdp.sales.orders"],
        preferred_tables=semantic["preferred_tables"],
        resolved_filters=[],
        instruction_matches=semantic["instruction_matches"],
        om_table_candidates=[],
        metabase_evidence=[],
        join_paths=[],
    )

    assert plan is not None
    assert plan.intent == "PERIOD_GROWTH"
    sql = plan.compiled_sql()
    assert "SUM(CASE WHEN cdp.sales.orders.created_at >= DATE_TRUNC('month', CURRENT_DATE) THEN amount ELSE 0 END)" in sql
    assert "DATE_ADD('month', -1, DATE_TRUNC('month', CURRENT_DATE))" in sql
    assert "AS total_revenue_last_month_to_this_month_growth_pct" in sql


def test_deterministic_planner_builds_year_vs_year_growth_sql(tmp_path: Path):
    model_path = tmp_path / "semantic_model.yaml"
    model_path.write_text(
        textwrap.dedent(
            """
            name: test_model
            tables:
              - name: cdp.sales.orders
                description: Orders fact table
                synonyms: [orders, revenue]
                time_dimensions:
                  - name: created_at
                    description: Creation timestamp
                metrics:
                  - name: total_revenue
                    description: Total revenue
                    sql: SUM(amount)
            verified_queries: []
            """
        )
    )

    store = SemanticModelStore(model_path)
    semantic = store.retrieve("Revenue percentage change 2024 vs 2025")
    planner = DeterministicPlanner(store)
    plan = planner.plan(
        question="Revenue percentage change 2024 vs 2025",
        glossary_matches=semantic["glossary_matches"],
        selected_tables=["cdp.sales.orders"],
        preferred_tables=semantic["preferred_tables"],
        resolved_filters=[],
        instruction_matches=semantic["instruction_matches"],
        om_table_candidates=[],
        metabase_evidence=[],
        join_paths=[],
    )

    assert plan is not None
    assert plan.intent == "PERIOD_GROWTH"
    sql = plan.compiled_sql()
    assert "EXTRACT(YEAR FROM cdp.sales.orders.created_at) = 2025" in sql
    assert "EXTRACT(YEAR FROM cdp.sales.orders.created_at) = 2024" in sql


def test_deterministic_planner_builds_filter_breakdown_percentage_sql(tmp_path: Path):
    model_path = tmp_path / "semantic_model.yaml"
    model_path.write_text(
        textwrap.dedent(
            """
            name: test_model
            tables:
              - name: cdp.ops.batches
                description: Batches table
                synonyms: [batches, batch]
                dimensions:
                  - name: status
                    description: Batch status
                    values: [active, inactive, draft]
            verified_queries: []
            """
        )
    )

    store = SemanticModelStore(model_path)
    semantic = store.retrieve("Show me the percentage of batches that are active vs inactive vs draft")
    resolver = ValueResolver(store)
    grounding = resolver.resolve(
        question="Show me the percentage of batches that are active vs inactive vs draft",
        content_awareness=[],
        preferred_tables=["cdp.ops.batches"],
        instruction_matches=semantic["instruction_matches"],
        focus=None,
    )
    planner = DeterministicPlanner(store)
    plan = planner.plan(
        question="Show me the percentage of batches that are active vs inactive vs draft",
        glossary_matches=semantic["glossary_matches"],
        selected_tables=["cdp.ops.batches"],
        preferred_tables=semantic["preferred_tables"],
        resolved_filters=[item.to_dict() for item in grounding.filters],
        instruction_matches=semantic["instruction_matches"],
        om_table_candidates=[],
        metabase_evidence=[],
        join_paths=[],
    )

    assert plan is not None
    assert plan.intent == "FILTER_BREAKDOWN_PERCENTAGE"
    sql = plan.compiled_sql()
    assert "SELECT cdp.ops.batches.status, ROUND(100.0 * COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (), 0), 2) AS status_percentage" in sql
    assert "WHERE cdp.ops.batches.status IN ('active', 'inactive', 'draft')" in sql
    assert "GROUP BY cdp.ops.batches.status" in sql


def test_deterministic_planner_builds_grouped_period_growth_sql(tmp_path: Path):
    model_path = tmp_path / "semantic_model.yaml"
    model_path.write_text(
        textwrap.dedent(
            """
            name: test_model
            tables:
              - name: acme.sales.orders
                description: Orders fact table
                synonyms: [orders, revenue]
                time_dimensions:
                  - name: created_at
                    description: Creation timestamp
                metrics:
                  - name: total_revenue
                    description: Total revenue
                    sql: SUM(amount)
              - name: acme.crm.centers
                description: Center dimension
                synonyms: [centers, center]
                dimensions:
                  - name: center_name
                    description: Center name
            relationships:
              - left_table: acme.sales.orders
                right_table: acme.crm.centers
                join_columns:
                  left: center_id
                  right: center_id
                cast_required: false
            verified_queries: []
            """
        )
    )

    store = SemanticModelStore(model_path)
    semantic = store.retrieve("Revenue growth by center this month vs last month")
    policy = JoinPolicy(store)
    selected_tables, join_paths = policy.connect_tables(
        ["acme.sales.orders", "acme.crm.centers"]
    )
    planner = DeterministicPlanner(store)
    plan = planner.plan(
        question="Revenue growth by center this month vs last month",
        glossary_matches=semantic["glossary_matches"],
        selected_tables=selected_tables,
        preferred_tables=semantic["preferred_tables"],
        resolved_filters=[],
        instruction_matches=semantic["instruction_matches"],
        om_table_candidates=[],
        metabase_evidence=[],
        join_paths=join_paths,
    )

    assert plan is not None
    assert plan.intent == "GROUPED_PERIOD_GROWTH"
    assert plan.path_type == "DETERMINISTIC_MULTI_TABLE"
    sql = plan.compiled_sql()
    assert "JOIN acme.crm.centers ON acme.sales.orders.center_id = acme.crm.centers.center_id" in sql
    assert "SELECT acme.crm.centers.center_name" in sql
    assert "GROUP BY acme.crm.centers.center_name" in sql
    assert "DATE_TRUNC('month', CURRENT_DATE)" in sql
    assert "DATE_ADD('month', -1, DATE_TRUNC('month', CURRENT_DATE))" in sql
    assert "AS total_revenue_last_month_to_this_month_growth_pct" in sql


def test_deterministic_planner_builds_filter_breakdown_aggregate_sql(tmp_path: Path):
    model_path = tmp_path / "semantic_model.yaml"
    model_path.write_text(
        textwrap.dedent(
            """
            name: test_model
            tables:
              - name: cdp.sales.orders
                description: Orders fact table
                synonyms: [orders, revenue]
                dimensions:
                  - name: status
                    description: Order status
                    values: [active, inactive, cancelled]
                metrics:
                  - name: total_revenue
                    description: Total revenue
                    sql: SUM(amount)
            business_rules:
              - term: current_month
                definition: Current month only
                sql_fragment: "cdp.sales.orders.created_at >= DATE_TRUNC('month', CURRENT_DATE)"
                synonyms: [this month]
            verified_queries: []
            """
        )
    )

    store = SemanticModelStore(model_path)
    semantic = store.retrieve("Show revenue for active vs inactive orders this month")
    resolver = ValueResolver(store)
    grounding = resolver.resolve(
        question="Show revenue for active vs inactive orders this month",
        content_awareness=[],
        preferred_tables=semantic["preferred_tables"],
        instruction_matches=semantic["instruction_matches"],
        focus=None,
    )
    planner = DeterministicPlanner(store)
    plan = planner.plan(
        question="Show revenue for active vs inactive orders this month",
        glossary_matches=semantic["glossary_matches"],
        selected_tables=["cdp.sales.orders"],
        preferred_tables=semantic["preferred_tables"],
        resolved_filters=[item.to_dict() for item in grounding.filters],
        instruction_matches=semantic["instruction_matches"],
        om_table_candidates=[],
        metabase_evidence=[],
        join_paths=[],
    )

    assert plan is not None
    assert plan.intent == "FILTER_BREAKDOWN_AGG"
    sql = plan.compiled_sql()
    assert "SELECT cdp.sales.orders.status, SUM(amount) AS total_revenue" in sql
    assert "WHERE cdp.sales.orders.created_at >= DATE_TRUNC('month', CURRENT_DATE)" in sql
    assert "cdp.sales.orders.status IN ('active', 'inactive')" in sql
    assert "GROUP BY cdp.sales.orders.status" in sql
    assert "ORDER BY total_revenue DESC" in sql


def test_deterministic_planner_builds_filter_breakdown_count_sql(tmp_path: Path):
    model_path = tmp_path / "semantic_model.yaml"
    model_path.write_text(
        textwrap.dedent(
            """
            name: test_model
            tables:
              - name: cdp.ops.batches
                description: Batches table
                synonyms: [batches, batch]
                dimensions:
                  - name: status
                    description: Batch status
                    values: [active, inactive, draft]
            verified_queries: []
            """
        )
    )

    store = SemanticModelStore(model_path)
    semantic = store.retrieve("How many batches are in active vs inactive vs draft status")
    resolver = ValueResolver(store)
    grounding = resolver.resolve(
        question="How many batches are in active vs inactive vs draft status",
        content_awareness=[],
        preferred_tables=semantic["preferred_tables"],
        instruction_matches=semantic["instruction_matches"],
        focus=None,
    )
    planner = DeterministicPlanner(store)
    plan = planner.plan(
        question="How many batches are in active vs inactive vs draft status",
        glossary_matches=semantic["glossary_matches"],
        selected_tables=["cdp.ops.batches"],
        preferred_tables=semantic["preferred_tables"],
        resolved_filters=[item.to_dict() for item in grounding.filters],
        instruction_matches=semantic["instruction_matches"],
        om_table_candidates=[],
        metabase_evidence=[],
        join_paths=[],
    )

    assert plan is not None
    assert plan.intent == "FILTER_BREAKDOWN_COUNT"
    sql = plan.compiled_sql()
    assert "SELECT cdp.ops.batches.status, COUNT(*) AS status_count" in sql
    assert "WHERE cdp.ops.batches.status IN ('active', 'inactive', 'draft')" in sql
    assert "GROUP BY cdp.ops.batches.status" in sql
    assert "ORDER BY status_count DESC" in sql


def test_pipeline_returns_ambiguous_when_validation_rejects_sql(monkeypatch):
    import sys
    import types

    fake_prom = types.SimpleNamespace(
        CollectorRegistry=lambda *args, **kwargs: object(),
        Counter=lambda *args, **kwargs: object(),
        Gauge=lambda *args, **kwargs: object(),
        Histogram=lambda *args, **kwargs: object(),
        Summary=lambda *args, **kwargs: object(),
        generate_latest=lambda *args, **kwargs: b"",
        CONTENT_TYPE_LATEST="text/plain",
    )
    monkeypatch.setitem(sys.modules, "prometheus_client", fake_prom)

    import src.raven.pipeline as pipeline_module
    from src.raven.pipeline import Pipeline
    from src.raven.router.classifier import Difficulty

    class _DummyMetrics:
        def query_started(self):
            return None

        def record_cache_hit(self):
            return None

        def record_cache_miss(self):
            return None

        def query_completed(self, **kwargs):
            return None

        def observe_stage(self, *args, **kwargs):
            return None

        def record_stage_error(self, *args, **kwargs):
            return None

    monkeypatch.setattr(pipeline_module, "METRICS", _DummyMetrics())

    class _NoExecuteTrino:
        def execute(self, sql):
            raise AssertionError("execution should not run after validation rejection")

        def explain(self, sql):
            return "ok"

    class _DummyPgVector:
        pass

    pipeline = Pipeline(
        trino=_NoExecuteTrino(),
        pgvector=_DummyPgVector(),
        openai=_DummyOpenAI(),
    )

    async def _resolve_question(question, conversation_id):
        return {"resolved_question": question, "is_followup": False}

    async def _router(ctx):
        ctx.difficulty = Difficulty.COMPLEX

    async def _retrieval(ctx):
        return None

    async def _schema(ctx):
        ctx.selected_tables = ["acme.sales.orders"]
        ctx.pruned_schema = "TABLE: acme.sales.orders\n  - amount (double)"

    async def _planning(ctx):
        ctx.query_plan = {
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
            "confidence": "MEDIUM",
        }

    async def _generation(ctx):
        ctx.sql_candidates = [
            "SELECT SUM(amount) AS total_revenue FROM acme.sales.orders",
            "SELECT acme.crm.centers.center_name FROM acme.crm.centers",
        ]

    async def _validation(ctx):
        ctx.selected_sql = ""
        ctx.confidence = "LOW"
        ctx.validation_issues = ["missing_table:acme.crm.centers"]

    monkeypatch.setattr(pipeline.conversation, "resolve_question", _resolve_question)
    monkeypatch.setattr(pipeline, "_stage_router", _router)
    monkeypatch.setattr(pipeline, "_stage_retrieval", _retrieval)
    monkeypatch.setattr(pipeline, "_stage_schema", _schema)
    monkeypatch.setattr(pipeline, "_stage_planning", _planning)
    monkeypatch.setattr(pipeline, "_stage_generation", _generation)
    monkeypatch.setattr(pipeline, "_stage_validation", _validation)

    result = asyncio.run(pipeline.generate("top 5 centers by revenue"))

    assert result["status"] == "ambiguous"
    assert "validate the sql or result" in result["message"].lower()
    assert result["validation_issues"] == ["missing_table:acme.crm.centers"]


def test_pipeline_returns_ambiguous_when_execution_judge_rejects_result(monkeypatch):
    import sys
    import types

    fake_prom = types.SimpleNamespace(
        CollectorRegistry=lambda *args, **kwargs: object(),
        Counter=lambda *args, **kwargs: object(),
        Gauge=lambda *args, **kwargs: object(),
        Histogram=lambda *args, **kwargs: object(),
        Summary=lambda *args, **kwargs: object(),
        generate_latest=lambda *args, **kwargs: b"",
        CONTENT_TYPE_LATEST="text/plain",
    )
    monkeypatch.setitem(sys.modules, "prometheus_client", fake_prom)

    import src.raven.pipeline as pipeline_module
    from src.raven.pipeline import Pipeline
    from src.raven.router.classifier import Difficulty

    class _DummyMetrics:
        def query_started(self):
            return None

        def record_cache_hit(self):
            return None

        def record_cache_miss(self):
            return None

        def query_completed(self, **kwargs):
            return None

        def observe_stage(self, *args, **kwargs):
            return None

        def record_stage_error(self, *args, **kwargs):
            return None

    monkeypatch.setattr(pipeline_module, "METRICS", _DummyMetrics())

    class _FakeTrino:
        def execute(self, sql):
            return pd.DataFrame({"total_revenue": [100.0, 200.0]})

        def explain(self, sql):
            return "ok"

    class _DummyPgVector:
        pass

    pipeline = Pipeline(
        trino=_FakeTrino(),
        pgvector=_DummyPgVector(),
        openai=_DummyOpenAI(),
    )

    async def _resolve_question(question, conversation_id):
        return {"resolved_question": question, "is_followup": False}

    async def _router(ctx):
        ctx.difficulty = Difficulty.SIMPLE

    async def _retrieval(ctx):
        return None

    async def _schema(ctx):
        ctx.selected_tables = ["acme.sales.orders"]
        ctx.pruned_schema = "TABLE: acme.sales.orders\n  - amount (double)"

    async def _planning(ctx):
        ctx.query_plan = {
            "intent": "KPI",
            "table": "acme.sales.orders",
            "source_tables": ["acme.sales.orders"],
            "metric_name": "total_revenue",
            "metric_sql": "SUM(amount)",
            "confidence": "HIGH",
            "compiled_sql": "SELECT SUM(amount) AS total_revenue FROM acme.sales.orders",
        }

    async def _render(*args, **kwargs):
        raise AssertionError("render should not run after execution judge rejection")

    monkeypatch.setattr(pipeline.conversation, "resolve_question", _resolve_question)
    monkeypatch.setattr(pipeline, "_stage_router", _router)
    monkeypatch.setattr(pipeline, "_stage_retrieval", _retrieval)
    monkeypatch.setattr(pipeline, "_stage_schema", _schema)
    monkeypatch.setattr(pipeline, "_stage_planning", _planning)
    monkeypatch.setattr(pipeline.renderer, "render", _render)

    result = asyncio.run(pipeline.generate("what is the total revenue"))

    assert result["status"] == "ambiguous"
    assert "validate the sql or result" in result["message"].lower()
    assert "unexpected_row_count:2" in result["validation_issues"]


def test_pipeline_returns_metadata_lookup_response_for_table_question(monkeypatch):
    import sys
    import types

    fake_prom = types.SimpleNamespace(
        CollectorRegistry=lambda *args, **kwargs: object(),
        Counter=lambda *args, **kwargs: object(),
        Gauge=lambda *args, **kwargs: object(),
        Histogram=lambda *args, **kwargs: object(),
        Summary=lambda *args, **kwargs: object(),
        generate_latest=lambda *args, **kwargs: b"",
        CONTENT_TYPE_LATEST="text/plain",
    )
    monkeypatch.setitem(sys.modules, "prometheus_client", fake_prom)

    import src.raven.pipeline as pipeline_module
    from src.raven.pipeline import Pipeline
    from src.raven.router.classifier import Difficulty

    class _DummyMetrics:
        def query_started(self):
            return None

        def record_cache_hit(self):
            return None

        def record_cache_miss(self):
            return None

        def query_completed(self, **kwargs):
            return None

        def observe_stage(self, *args, **kwargs):
            return None

        def record_stage_error(self, *args, **kwargs):
            return None

    monkeypatch.setattr(pipeline_module, "METRICS", _DummyMetrics())

    class _NoExecuteTrino:
        def execute(self, sql):
            raise AssertionError("metadata lookup should not execute SQL")

        def explain(self, sql):
            return "ok"

    class _DummyPgVector:
        pass

    pipeline = Pipeline(
        trino=_NoExecuteTrino(),
        pgvector=_DummyPgVector(),
        openai=_DummyOpenAI(),
    )
    pipeline.schema_selector.set_column_catalog(
        {
            "monitoring.trino.query_logs": [],
            "analytics.orders": [],
        }
    )

    async def _resolve_question(question, conversation_id):
        return {"resolved_question": question, "is_followup": False}

    async def _router(ctx):
        ctx.difficulty = Difficulty.SIMPLE

    async def _retrieval(ctx):
        ctx.om_table_candidates = [
            {
                "fqn": "monitoring.trino.query_logs",
                "name": "query_logs",
                "description": "Trino query execution log table",
                "domain": "platform",
                "score": 0.93,
            }
        ]
        ctx.doc_snippets = [
            {
                "title": "Trino Logging",
                "table": "monitoring.trino.query_logs",
                "content": "Contains Trino query history.",
                "similarity": 0.81,
                "trust_level": "reviewed",
                "related_tables": ["monitoring.trino.query_logs"],
            }
        ]

    async def _schema(ctx):
        raise AssertionError("schema selection should be skipped for metadata lookup")

    async def _planning(ctx):
        raise AssertionError("planning should be skipped for metadata lookup")

    async def _generation(ctx):
        raise AssertionError("generation should be skipped for metadata lookup")

    monkeypatch.setattr(pipeline.conversation, "resolve_question", _resolve_question)
    monkeypatch.setattr(pipeline, "_stage_router", _router)
    monkeypatch.setattr(pipeline, "_stage_retrieval", _retrieval)
    monkeypatch.setattr(pipeline, "_stage_schema", _schema)
    monkeypatch.setattr(pipeline, "_stage_planning", _planning)
    monkeypatch.setattr(pipeline, "_stage_generation", _generation)

    result = asyncio.run(
        pipeline.generate("what table can give me trino query logs of today?")
    )

    assert result["status"] == "success"
    assert result["debug"]["query_plan"]["intent"] == "METADATA_LOOKUP"
    assert result["sql"] == "-- metadata lookup request; no SQL executed"
    assert result["data"][0]["table_name"] == "monitoring.trino.query_logs"
    assert "metadata lookup question" in result["summary"].lower()


def test_metadata_lookup_prefers_lexically_matching_table_over_irrelevant_similarity(monkeypatch):
    import sys
    import types

    fake_prom = types.SimpleNamespace(
        CollectorRegistry=lambda *args, **kwargs: object(),
        Counter=lambda *args, **kwargs: object(),
        Gauge=lambda *args, **kwargs: object(),
        Histogram=lambda *args, **kwargs: object(),
        Summary=lambda *args, **kwargs: object(),
        generate_latest=lambda *args, **kwargs: b"",
        CONTENT_TYPE_LATEST="text/plain",
    )
    monkeypatch.setitem(sys.modules, "prometheus_client", fake_prom)

    import src.raven.pipeline as pipeline_module
    from src.raven.pipeline import Pipeline
    from src.raven.router.classifier import Difficulty

    class _DummyMetrics:
        def query_started(self):
            return None

        def record_cache_hit(self):
            return None

        def record_cache_miss(self):
            return None

        def query_completed(self, **kwargs):
            return None

        def observe_stage(self, *args, **kwargs):
            return None

        def record_stage_error(self, *args, **kwargs):
            return None

    monkeypatch.setattr(pipeline_module, "METRICS", _DummyMetrics())

    class _NoExecuteTrino:
        def execute(self, sql):
            raise AssertionError("metadata lookup should not execute SQL")

        def explain(self, sql):
            return "ok"

    class _DummyPgVector:
        pass

    pipeline = Pipeline(
        trino=_NoExecuteTrino(),
        pgvector=_DummyPgVector(),
        openai=_DummyOpenAI(),
    )
    pipeline.schema_selector.set_column_catalog(
        {
            "cdp.trino_logs.trino_queries": [],
            "cdp.cdp_revenue.gold_batches": [],
            "cdp.cdp_revenue.gold_batch_plans": [],
        }
    )

    async def _resolve_question(question, conversation_id):
        return {"resolved_question": question, "is_followup": False}

    async def _router(ctx):
        ctx.difficulty = Difficulty.SIMPLE

    async def _retrieval(ctx):
        ctx.om_table_candidates = [
            {
                "fqn": "cdp.cdp_revenue.gold_batches",
                "name": "gold_batches",
                "description": "Batch analytics table",
                "domain": "revenue",
                "score": 0.95,
            },
            {
                "fqn": "cdp.trino_logs.trino_queries",
                "name": "trino_queries",
                "description": "Trino query log history and execution metadata",
                "domain": "platform",
                "score": 0.72,
            },
        ]
        ctx.doc_snippets = [
            {
                "title": "Trino query logs",
                "table": "cdp.trino_logs.trino_queries",
                "content": "Contains Trino query logs data and execution details.",
                "similarity": 0.65,
                "trust_level": "reviewed",
                "related_tables": ["cdp.trino_logs.trino_queries"],
            }
        ]

    async def _schema(ctx):
        raise AssertionError("schema selection should be skipped for metadata lookup")

    async def _planning(ctx):
        raise AssertionError("planning should be skipped for metadata lookup")

    async def _generation(ctx):
        raise AssertionError("generation should be skipped for metadata lookup")

    monkeypatch.setattr(pipeline.conversation, "resolve_question", _resolve_question)
    monkeypatch.setattr(pipeline, "_stage_router", _router)
    monkeypatch.setattr(pipeline, "_stage_retrieval", _retrieval)
    monkeypatch.setattr(pipeline, "_stage_schema", _schema)
    monkeypatch.setattr(pipeline, "_stage_planning", _planning)
    monkeypatch.setattr(pipeline, "_stage_generation", _generation)

    result = asyncio.run(
        pipeline.generate("which table can give me trino query logs data?")
    )

    assert result["status"] == "success"
    assert result["data"][0]["table_name"] == "cdp.trino_logs.trino_queries"
    assert result["debug"]["query_plan"]["intent"] == "METADATA_LOOKUP"
