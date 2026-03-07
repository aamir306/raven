from __future__ import annotations

from pathlib import Path
import textwrap

from src.raven.focus import FocusContext
from src.raven.semantic_assets import SemanticModelStore


def test_query_family_matches_top_k_verified_query(tmp_path: Path):
    model_path = tmp_path / "semantic_model.yaml"
    model_path.write_text(
        textwrap.dedent(
            """
            name: test_model
            tables:
              - name: acme.sales.orders
                description: Orders
                metrics:
                  - name: total_revenue
                    description: Revenue
                    sql: SUM(amount)
            verified_queries:
              - question: "Top 10 orders by revenue"
                sql: "SELECT order_id, SUM(amount) AS total_revenue FROM acme.sales.orders GROUP BY order_id ORDER BY total_revenue DESC LIMIT 10"
                category: revenue
            """
        )
    )

    store = SemanticModelStore(model_path)
    result = store.retrieve("Top 5 orders by revenue")

    family = result["query_family_match"]
    assert family is not None
    assert family["source"] == "semantic_model"
    assert "LIMIT 5" in family["sql"]
    assert family["slots"]["limit"] == 5


def test_query_family_matches_time_window_verified_query(tmp_path: Path):
    model_path = tmp_path / "semantic_model.yaml"
    model_path.write_text(
        textwrap.dedent(
            """
            name: test_model
            tables:
              - name: acme.sales.orders
                description: Orders
                time_dimensions:
                  - name: created_at
                    description: Created time
                metrics:
                  - name: total_revenue
                    description: Revenue
                    sql: SUM(amount)
            verified_queries:
              - question: "Daily revenue for the last 30 days"
                sql: "SELECT DATE(created_at) AS order_date, SUM(amount) AS daily_revenue FROM acme.sales.orders WHERE created_at >= CURRENT_DATE - INTERVAL '30' DAY GROUP BY DATE(created_at) ORDER BY order_date"
                category: time_series
            """
        )
    )

    store = SemanticModelStore(model_path)
    result = store.retrieve("Daily revenue for the last 7 days")

    family = result["query_family_match"]
    assert family is not None
    assert "INTERVAL '7' DAY" in family["sql"]
    assert family["slots"]["interval"] == {"value": 7, "unit": "day"}


def test_query_family_matches_metabase_card(tmp_path: Path):
    model_path = tmp_path / "semantic_model.yaml"
    model_path.write_text(
        textwrap.dedent(
            """
            name: test_model
            tables:
              - name: acme.sales.orders
                description: Orders
                metrics:
                  - name: total_revenue
                    description: Revenue
                    sql: SUM(amount)
            verified_queries: []
            """
        )
    )

    focus = FocusContext(
        type="dashboard",
        name="Revenue Dashboard",
        source_id="dash-1",
        tables=["acme.sales.orders"],
        dashboard_cards=[
            {
                "card_id": 9,
                "name": "Top 10 orders by revenue",
                "sql": "SELECT order_id, SUM(amount) AS total_revenue FROM acme.sales.orders GROUP BY order_id ORDER BY total_revenue DESC LIMIT 10",
                "tables": ["acme.sales.orders"],
                "display": "bar",
            }
        ],
    )

    store = SemanticModelStore(model_path)
    result = store.retrieve("Top 3 orders by revenue", focus=focus)

    family = result["query_family_match"]
    assert family is not None
    assert family["source"] == "metabase"
    assert "LIMIT 3" in family["sql"]


def test_query_family_compiles_bottom_k_order_direction(tmp_path: Path):
    model_path = tmp_path / "semantic_model.yaml"
    model_path.write_text(
        textwrap.dedent(
            """
            name: test_model
            tables:
              - name: acme.sales.orders
                description: Orders
                metrics:
                  - name: total_revenue
                    description: Revenue
                    sql: SUM(amount)
            verified_queries:
              - question: "Top 10 centers by revenue"
                sql: "SELECT center_name, SUM(amount) AS total_revenue FROM acme.sales.orders GROUP BY center_name ORDER BY total_revenue DESC LIMIT 10"
                category: revenue
            """
        )
    )

    store = SemanticModelStore(model_path)
    result = store.retrieve("Bottom 5 centers by revenue")

    family = result["query_family_match"]
    assert family is not None
    assert "ORDER BY total_revenue ASC" in family["sql"]
    assert "LIMIT 5" in family["sql"]
    assert family["slots"]["order_direction"] == "ASC"


def test_query_family_compiles_time_grain_change(tmp_path: Path):
    model_path = tmp_path / "semantic_model.yaml"
    model_path.write_text(
        textwrap.dedent(
            """
            name: test_model
            tables:
              - name: acme.sales.orders
                description: Orders
                time_dimensions:
                  - name: created_at
                    description: Created time
                metrics:
                  - name: total_revenue
                    description: Revenue
                    sql: SUM(amount)
            verified_queries:
              - question: "Daily revenue for the last 30 days"
                sql: "SELECT DATE(created_at) AS order_date, SUM(amount) AS daily_revenue FROM acme.sales.orders WHERE created_at >= CURRENT_DATE - INTERVAL '30' DAY GROUP BY DATE(created_at) ORDER BY order_date"
                category: time_series
            """
        )
    )

    store = SemanticModelStore(model_path)
    result = store.retrieve("Weekly revenue for the last 30 days")

    family = result["query_family_match"]
    assert family is not None
    assert "DATE_TRUNC('week', created_at)" in family["sql"]
    assert family["slots"]["time_grain"] == "week"


def test_query_family_compiles_safe_dimension_swap(tmp_path: Path):
    model_path = tmp_path / "semantic_model.yaml"
    model_path.write_text(
        textwrap.dedent(
            """
            name: test_model
            tables:
              - name: acme.sales.orders
                description: Orders
                dimensions:
                  - name: center_name
                    description: Center name
                  - name: region_name
                    description: Region name
                metrics:
                  - name: total_revenue
                    description: Revenue
                    sql: SUM(amount)
            verified_queries:
              - question: "Top 10 centers by revenue"
                sql: "SELECT center_name, SUM(amount) AS total_revenue FROM acme.sales.orders GROUP BY center_name ORDER BY total_revenue DESC LIMIT 10"
                category: revenue
            """
        )
    )

    store = SemanticModelStore(model_path)
    semantic = store.retrieve("Top 10 regions by revenue")
    family = store.match_query_family(
        question="Top 10 regions by revenue",
        verified_queries=semantic["verified_queries"],
        metabase_evidence=[],
        glossary_matches=semantic["glossary_matches"],
    )

    assert family is not None
    assert "SELECT region_name" in family["sql"]
    assert "GROUP BY region_name" in family["sql"]
    assert "center_name" not in family["sql"]
    assert family["dimension_replacements"]


def test_query_family_rejects_dimension_swap_when_dimension_is_filter(tmp_path: Path):
    model_path = tmp_path / "semantic_model.yaml"
    model_path.write_text(
        textwrap.dedent(
            """
            name: test_model
            tables:
              - name: acme.sales.orders
                description: Orders
                dimensions:
                  - name: center_name
                    description: Center name
                  - name: region_name
                    description: Region name
                metrics:
                  - name: total_revenue
                    description: Revenue
                    sql: SUM(amount)
            verified_queries:
              - question: "Top centers for Kota by revenue"
                sql: "SELECT center_name, SUM(amount) AS total_revenue FROM acme.sales.orders WHERE center_name = 'Kota' GROUP BY center_name ORDER BY total_revenue DESC LIMIT 10"
                category: revenue
            """
        )
    )

    store = SemanticModelStore(model_path)
    semantic = store.retrieve("Top regions for Kota by revenue")
    family = store.match_query_family(
        question="Top regions for Kota by revenue",
        verified_queries=semantic["verified_queries"],
        metabase_evidence=[],
        glossary_matches=semantic["glossary_matches"],
    )

    assert family is None


def test_query_family_compiles_join_aware_dimension_swap(tmp_path: Path):
    model_path = tmp_path / "semantic_model.yaml"
    model_path.write_text(
        textwrap.dedent(
            """
            name: test_model
            tables:
              - name: acme.sales.orders
                description: Orders
                metrics:
                  - name: total_revenue
                    description: Revenue
                    sql: SUM(amount)
              - name: acme.crm.centers
                description: Centers
                dimensions:
                  - name: center_name
                    description: Center name
              - name: acme.geo.regions
                description: Regions
                dimensions:
                  - name: region_name
                    description: Region name
            relationships:
              - left_table: acme.sales.orders
                right_table: acme.crm.centers
                join_columns:
                  left: center_id
                  right: center_id
                cast_required: false
              - left_table: acme.sales.orders
                right_table: acme.geo.regions
                join_columns:
                  left: region_id
                  right: region_id
                cast_required: false
            verified_queries:
              - question: "Top 10 centers by revenue"
                sql: "SELECT acme.crm.centers.center_name, SUM(amount) AS total_revenue FROM acme.sales.orders JOIN acme.crm.centers ON acme.sales.orders.center_id = acme.crm.centers.center_id GROUP BY acme.crm.centers.center_name ORDER BY total_revenue DESC LIMIT 10"
                category: revenue
            """
        )
    )

    store = SemanticModelStore(model_path)
    semantic = store.retrieve("Top 10 regions by revenue")
    family = store.match_query_family(
        question="Top 10 regions by revenue",
        verified_queries=semantic["verified_queries"],
        metabase_evidence=[],
        glossary_matches=semantic["glossary_matches"],
    )

    assert family is not None
    assert "JOIN acme.geo.regions ON acme.sales.orders.region_id = acme.geo.regions.region_id" in family["sql"]
    assert "acme.geo.regions.region_name" in family["sql"]
    assert "acme.crm.centers.center_name" not in family["sql"]
    assert family["join_replacements"]


def test_query_family_compiles_safe_metric_swap(tmp_path: Path):
    model_path = tmp_path / "semantic_model.yaml"
    model_path.write_text(
        textwrap.dedent(
            """
            name: test_model
            tables:
              - name: acme.sales.orders
                description: Orders
                dimensions:
                  - name: center_name
                    description: Center name
                metrics:
                  - name: total_revenue
                    description: Revenue
                    sql: SUM(amount)
                  - name: total_orders
                    description: Order count
                    sql: COUNT(*)
            verified_queries:
              - question: "Top 10 centers by revenue"
                sql: "SELECT center_name, SUM(amount) AS total_revenue FROM acme.sales.orders GROUP BY center_name ORDER BY total_revenue DESC LIMIT 10"
                category: revenue
            """
        )
    )

    store = SemanticModelStore(model_path)
    semantic = store.retrieve("Top 10 centers by order count")
    family = store.match_query_family(
        question="Top 10 centers by order count",
        verified_queries=semantic["verified_queries"],
        metabase_evidence=[],
        glossary_matches=semantic["glossary_matches"],
    )

    assert family is not None
    assert "COUNT(*) AS total_orders" in family["sql"]
    assert "ORDER BY total_orders DESC" in family["sql"]
    assert "SUM(amount) AS total_revenue" not in family["sql"]
    assert family["metric_replacements"]


def test_query_family_matches_grounded_filter_value(tmp_path: Path):
    model_path = tmp_path / "semantic_model.yaml"
    model_path.write_text(
        textwrap.dedent(
            """
            name: test_model
            tables:
              - name: acme.sales.orders
                description: Orders
                dimensions:
                  - name: center_name
                    description: Center
                    values: [Kota, Noida]
                metrics:
                  - name: total_revenue
                    description: Revenue
                    sql: SUM(amount)
            verified_queries:
              - question: "Revenue for Kota center"
                sql: "SELECT SUM(amount) AS total_revenue FROM acme.sales.orders WHERE center_name = 'Kota'"
                category: revenue
            """
        )
    )

    store = SemanticModelStore(model_path)
    family = store.match_query_family(
        question="Revenue for Noida center",
        verified_queries=store.retrieve("Revenue for Noida center")["verified_queries"],
        metabase_evidence=[],
        resolved_filters=[
            {
                "table": "acme.sales.orders",
                "column": "center_name",
                "operator": "=",
                "value": "Noida",
                "matched_text": "Noida",
            }
        ],
    )

    assert family is not None
    assert "center_name = 'Noida'" in family["sql"]
    assert family["filter_replacements"]


def test_query_family_prefers_grounded_metabase_card_value(tmp_path: Path):
    model_path = tmp_path / "semantic_model.yaml"
    model_path.write_text(
        textwrap.dedent(
            """
            name: test_model
            tables:
              - name: acme.sales.orders
                description: Orders
                metrics:
                  - name: total_revenue
                    description: Revenue
                    sql: SUM(amount)
            verified_queries: []
            """
        )
    )

    focus = FocusContext(
        type="dashboard",
        name="Revenue Dashboard",
        source_id="dash-1",
        tables=["acme.sales.orders"],
        dashboard_cards=[
            {
                "card_id": 10,
                "name": "Revenue for Kota center",
                "sql": "SELECT SUM(amount) AS total_revenue FROM acme.sales.orders WHERE center_name = 'Kota'",
                "tables": ["acme.sales.orders"],
                "display": "scalar",
            }
        ],
    )

    store = SemanticModelStore(model_path)
    semantic = store.retrieve("Revenue for Noida center", focus=focus)
    family = store.match_query_family(
        question="Revenue for Noida center",
        verified_queries=semantic["verified_queries"],
        metabase_evidence=semantic["metabase_evidence"],
        resolved_filters=[
            {
                "table": "acme.sales.orders",
                "column": "center_name",
                "operator": "=",
                "value": "Noida",
                "matched_text": "Noida",
            }
        ],
    )

    assert family is not None
    assert family["source"] == "metabase"
    assert "center_name = 'Noida'" in family["sql"]
