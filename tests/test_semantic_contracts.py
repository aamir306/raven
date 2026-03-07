from __future__ import annotations

from pathlib import Path
import textwrap

import pytest

from src.raven.contracts import ContractRegistry, SemanticContractValidationError
from src.raven.semantic_assets import SemanticModelStore


def test_contract_registry_loads_split_domain_pack(tmp_path: Path):
    pack_dir = tmp_path / "domain_pack"
    (pack_dir / "contracts").mkdir(parents=True)
    (pack_dir / "instructions").mkdir()
    (pack_dir / "verified_queries").mkdir()

    (pack_dir / "manifest.yaml").write_text(
        textwrap.dedent(
            """
            name: acme_pack
            description: Example pack
            metadata:
              owner: analytics
            """
        )
    )
    (pack_dir / "contracts" / "revenue.yaml").write_text(
        textwrap.dedent(
            """
            tables:
              - name: acme.sales.orders
                description: Orders
                synonyms: [orders]
                dimensions:
                  - name: status
                    description: Order status
                metrics:
                  - name: total_revenue
                    description: Revenue
                    sql: SUM(amount)
            relationships:
              - left_table: acme.sales.orders
                right_table: acme.crm.centers
                join_columns:
                  left: center_id
                  right: center_id
            """
        )
    )
    (pack_dir / "contracts" / "dimensions.yaml").write_text(
        textwrap.dedent(
            """
            tables:
              - name: acme.crm.centers
                description: Centers
                synonyms: [centers]
                dimensions:
                  - name: center_name
                    description: Center name
            """
        )
    )
    (pack_dir / "instructions" / "business_rules.yaml").write_text(
        textwrap.dedent(
            """
            business_rules:
              - term: completed_orders
                definition: Use completed orders only
                sql_fragment: "status = 'completed'"
            """
        )
    )
    (pack_dir / "verified_queries" / "queries.yaml").write_text(
        textwrap.dedent(
            """
            verified_queries:
              - question: What is total revenue?
                sql: SELECT SUM(amount) FROM acme.sales.orders
            """
        )
    )

    bundle = ContractRegistry(pack_dir).load()

    assert bundle.name == "acme_pack"
    assert len(bundle.tables) == 2
    assert len(bundle.relationships) == 1
    assert len(bundle.business_rules) == 1
    assert len(bundle.verified_queries) == 1


def test_semantic_store_accepts_split_domain_pack(tmp_path: Path):
    pack_dir = tmp_path / "domain_pack"
    (pack_dir / "contracts").mkdir(parents=True)
    (pack_dir / "instructions").mkdir()

    (pack_dir / "contracts" / "core.yaml").write_text(
        textwrap.dedent(
            """
            tables:
              - name: acme.billing.invoices
                description: Invoices
                synonyms: [invoices, billing]
                dimensions:
                  - name: invoice_status
                    description: Invoice status
                metrics:
                  - name: invoice_total
                    description: Total invoiced amount
                    sql: SUM(amount)
            """
        )
    )
    (pack_dir / "instructions" / "rules.yaml").write_text(
        textwrap.dedent(
            """
            business_rules:
              - term: paid_only
                definition: Only paid invoices
                sql_fragment: "invoice_status = 'paid'"
            """
        )
    )

    store = SemanticModelStore(pack_dir)
    result = store.retrieve("show invoice total for paid invoices")

    assert any(match["field_name"] == "invoice_total" for match in result["glossary_matches"])
    assert any(rule["term"] == "paid_only" for rule in result["instruction_matches"])
    assert "invoice" in store.data_keywords()


def test_semantic_store_rejects_invalid_contract_bundle(tmp_path: Path):
    pack_dir = tmp_path / "invalid_pack"
    (pack_dir / "contracts").mkdir(parents=True)
    (pack_dir / "contracts" / "broken.yaml").write_text(
        textwrap.dedent(
            """
            tables:
              - name: acme.sales.orders
                description: Orders
                metrics:
                  - name: total_revenue
                    description: Revenue
                    sql: SUM(amount)
            relationships:
              - left_table: acme.sales.orders
                right_table: acme.crm.centers
                join_columns:
                  left: center_id
            """
        )
    )

    with pytest.raises(SemanticContractValidationError):
        SemanticModelStore(pack_dir)
