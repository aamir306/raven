from __future__ import annotations

import asyncio
import textwrap
from pathlib import Path

from src.raven.metabase.query_family_sync import MetabaseQueryFamilySync
from src.raven.query_families.registry import QueryFamilyRegistry
from src.raven.semantic_assets import SemanticModelStore


class _FakeOpenAI:
    async def batch_embed(self, texts: list[str]) -> list[list[float]]:
        return [[float(i + 1)] * 3 for i, _ in enumerate(texts)]


class _FakePgVector:
    def __init__(self) -> None:
        self.deleted: list[tuple[str, str]] = []
        self.inserted: list[tuple[str, list[dict]]] = []

    def delete_by_source(self, *, table: str | None = None, source: str = "", table_name: str | None = None) -> int:
        self.deleted.append((table or table_name or "", source))
        return 0

    def batch_insert(self, table: str, items: list[dict]) -> int:
        self.inserted.append((table, items))
        return len(items)


def test_metabase_sync_replaces_scope_and_persists_registry(tmp_path: Path):
    registry = QueryFamilyRegistry()
    path = tmp_path / "query_family_registry.json"
    syncer = MetabaseQueryFamilySync(registry, registry_path=path)

    cards_v1 = [
        {
            "card_id": 11,
            "name": "Top 10 orders by revenue",
            "sql": "SELECT order_id, SUM(amount) AS total_revenue FROM acme.sales.orders GROUP BY order_id ORDER BY total_revenue DESC LIMIT 10",
            "tables": ["acme.sales.orders"],
            "display": "bar",
        }
    ]
    cards_v2 = [
        {
            "card_id": 12,
            "name": "Daily revenue",
            "sql": "SELECT DATE(created_at) AS order_date, SUM(amount) AS total_revenue FROM acme.sales.orders GROUP BY DATE(created_at)",
            "tables": ["acme.sales.orders"],
            "display": "line",
        }
    ]

    result_v1 = asyncio.run(
        syncer.sync_cards(
            cards_v1,
            scope_type="dashboard",
            scope_id=99,
            scope_name="Revenue Dashboard",
        )
    )
    result_v2 = asyncio.run(
        syncer.sync_cards(
            cards_v2,
            scope_type="dashboard",
            scope_id=99,
            scope_name="Revenue Dashboard",
        )
    )

    assert result_v1["synced_count"] == 1
    assert result_v2["synced_count"] == 1
    assert registry.size == 1
    assert path.exists()
    exported = registry.export_assets(source_prefix="metabase_sync")
    assert len(exported) == 1
    assert exported[0]["question"] == "Daily revenue"
    assert exported[0]["metadata"]["scope_key"] == "dashboard:99"


def test_metabase_sync_can_persist_embeddings(tmp_path: Path):
    registry = QueryFamilyRegistry()
    pgvector = _FakePgVector()
    syncer = MetabaseQueryFamilySync(
        registry,
        registry_path=tmp_path / "registry.json",
        pgvector=pgvector,
        openai=_FakeOpenAI(),
    )

    cards = [
        {
            "card_id": 21,
            "name": "Revenue by center",
            "sql": "SELECT center_name, SUM(amount) AS total_revenue FROM acme.sales.orders GROUP BY center_name",
            "tables": ["acme.sales.orders"],
            "display": "bar",
        }
    ]

    result = asyncio.run(
        syncer.sync_cards(
            cards,
            scope_type="collection",
            scope_id=7,
            scope_name="Ops KPIs",
            persist_embeddings=True,
        )
    )

    assert result["embedded_count"] == 1
    assert pgvector.deleted == [("question_embeddings", "metabase_sync:collection:7")]
    assert len(pgvector.inserted) == 1
    table, items = pgvector.inserted[0]
    assert table == "question_embeddings"
    assert items[0]["source"] == "metabase_sync:collection:7"
    assert items[0]["metadata"]["scope_key"] == "collection:7"


def test_semantic_store_uses_synced_metabase_families_without_live_focus(tmp_path: Path):
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

    store = SemanticModelStore(model_path)
    store.set_external_query_families(
        [
            {
                "question": "Top 10 orders by revenue",
                "sql": "SELECT order_id, SUM(amount) AS total_revenue FROM acme.sales.orders GROUP BY order_id ORDER BY total_revenue DESC LIMIT 10",
                "tables_used": ["acme.sales.orders"],
                "source": "metabase_sync",
                "metadata": {"scope_key": "dashboard:1", "scope_name": "Revenue Dashboard"},
            }
        ]
    )

    result = store.retrieve("Top 5 orders by revenue")
    family = result["query_family_match"]

    assert family is not None
    assert family["source"] == "metabase_sync"
    assert "LIMIT 5" in family["sql"]
