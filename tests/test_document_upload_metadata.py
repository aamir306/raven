from __future__ import annotations

import asyncio
import importlib
from io import BytesIO
from pathlib import Path

from fastapi import UploadFile

from src.raven.retrieval.doc_retriever import DocRetriever


class _DummyOpenAI:
    async def batch_embed(self, texts: list[str]) -> list[list[float]]:
        return [[0.1, 0.2, 0.3] for _ in texts]


class _DummyCursor:
    def __init__(self, rows: list[dict]):
        self._rows = rows
        self._fetchone = [0]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql: str, params: tuple[str]):
        if sql.startswith("SELECT COUNT(*) FROM doc_embeddings"):
            source_file = params[0]
            self._fetchone = [
                sum(1 for row in self._rows if row["source_file"] == source_file)
            ]
            return
        if sql.startswith("DELETE FROM doc_embeddings"):
            source_file = params[0]
            self._rows[:] = [
                row for row in self._rows if row["source_file"] != source_file
            ]
            return
        raise AssertionError(f"Unexpected SQL in test: {sql}")

    def fetchone(self):
        return self._fetchone


class _DummyConn:
    def __init__(self, rows: list[dict]):
        self._rows = rows

    def cursor(self, *args, **kwargs):
        return _DummyCursor(self._rows)

    def commit(self):
        return None


class _DummyPool:
    def __init__(self, rows: list[dict]):
        self._rows = rows

    def getconn(self):
        return _DummyConn(self._rows)

    def putconn(self, conn):
        return None


class _DummyPgVector:
    def __init__(self):
        self.rows: list[dict] = []
        self._pool = _DummyPool(self.rows)

    def insert(self, **kwargs):
        self.rows.append(kwargs)


class _DummyPipeline:
    def __init__(self):
        self.openai = _DummyOpenAI()
        self.pgvector = _DummyPgVector()


class _DummyDocStore:
    def __init__(self, rows: list[dict]):
        self._rows = rows

    async def async_search(self, **kwargs):
        return list(self._rows)

    def search(self, **kwargs):
        return list(self._rows)


def test_upload_doc_persists_structured_metadata(tmp_path: Path, monkeypatch):
    routes_module = importlib.import_module("web.routes.__init__")
    focus_module = importlib.import_module("src.raven.focus")

    monkeypatch.setattr(routes_module, "UPLOAD_DIR", tmp_path / "uploads")
    monkeypatch.setattr(focus_module, "FOCUS_DIR", tmp_path / "focus_documents")
    monkeypatch.setattr(
        focus_module,
        "SUGGESTIONS_FILE",
        (tmp_path / "focus_documents" / "_suggestions.json"),
    )

    pipeline = _DummyPipeline()
    upload = UploadFile(
        filename="revenue_prd.txt",
        file=BytesIO(b"1. Objective\nRevenue means booked orders only."),
    )

    response = asyncio.run(
        routes_module.upload_doc(
            file=upload,
            title="Revenue PRD",
            description="Defines canonical revenue handling.",
            doc_kind="prd",
            domain="revenue",
            owner="finance",
            trust_level="canonical",
            related_tables="analytics.orders, marts.revenue_daily",
            related_metrics="net_revenue, order_count",
            tags="finance, approved",
            version="v2",
            effective_date="2026-03-07",
            deprecated=False,
            pipeline=pipeline,
        )
    )

    assert response.status == "indexed"
    assert response.title == "Revenue PRD"
    assert response.doc_kind == "prd"
    assert response.related_tables == ["analytics.orders", "marts.revenue_daily"]
    assert response.related_metrics == ["net_revenue", "order_count"]
    assert response.tags == ["finance", "approved"]
    assert response.focus_document_id

    stored = pipeline.pgvector.rows[0]
    assert stored["metadata"]["title"] == "Revenue PRD"
    assert stored["metadata"]["trust_level"] == "canonical"
    assert stored["metadata"]["related_tables"] == [
        "analytics.orders",
        "marts.revenue_daily",
    ]
    assert stored["metadata"]["related_metrics"] == ["net_revenue", "order_count"]

    store = focus_module.FocusStore()
    docs = store.list_documents()
    assert len(docs) == 1
    doc = docs[0]
    assert doc["name"] == "Revenue PRD"
    assert doc["doc_kind"] == "prd"
    assert doc["domain"] == "revenue"
    assert doc["owner"] == "finance"
    assert doc["trust_level"] == "canonical"
    assert doc["tables"] == ["analytics.orders", "marts.revenue_daily"]
    assert doc["related_metrics"] == ["net_revenue", "order_count"]
    assert doc["tags"] == ["finance", "approved"]
    assert doc["source_filename"] == "revenue_prd.txt"
    assert doc["version"] == "v2"
    assert doc["effective_date"] == "2026-03-07"


def test_uploaded_docs_listing_includes_structured_metadata(tmp_path: Path, monkeypatch):
    routes_module = importlib.import_module("web.routes.__init__")
    focus_module = importlib.import_module("src.raven.focus")

    monkeypatch.setattr(routes_module, "UPLOAD_DIR", tmp_path / "uploads")
    monkeypatch.setattr(focus_module, "FOCUS_DIR", tmp_path / "focus_documents")
    monkeypatch.setattr(
        focus_module,
        "SUGGESTIONS_FILE",
        (tmp_path / "focus_documents" / "_suggestions.json"),
    )

    pipeline = _DummyPipeline()
    upload = UploadFile(
        filename="status_rules.txt",
        file=BytesIO(b"1. Rule\nCancelled orders should be excluded."),
    )
    asyncio.run(
        routes_module.upload_doc(
            file=upload,
            title="Order Status Rules",
            description="Order lifecycle rules.",
            doc_kind="business_rule",
            domain="operations",
            owner="ops",
            trust_level="reviewed",
            related_tables="analytics.orders",
            related_metrics="cancel_rate",
            tags="ops, status",
            version="2026-Q1",
            effective_date="2026-03-01",
            deprecated=False,
            pipeline=pipeline,
        )
    )

    result = asyncio.run(routes_module.list_uploaded_docs(pipeline=pipeline))
    assert len(result["documents"]) == 1
    doc = result["documents"][0]
    assert doc["title"] == "Order Status Rules"
    assert doc["doc_kind"] == "business_rule"
    assert doc["domain"] == "operations"
    assert doc["owner"] == "ops"
    assert doc["trust_level"] == "reviewed"
    assert doc["related_tables"] == ["analytics.orders"]
    assert doc["related_metrics"] == ["cancel_rate"]
    assert doc["tags"] == ["ops", "status"]
    assert doc["version"] == "2026-Q1"
    assert doc["effective_date"] == "2026-03-01"


def test_doc_retriever_prefers_canonical_docs_and_penalizes_deprecated():
    retriever = DocRetriever(
        _DummyDocStore(
            [
                {
                    "similarity": 0.79,
                    "source_file": "docs/reference.txt",
                    "content": "Reference note",
                    "metadata": {"title": "Reference", "trust_level": "reference"},
                },
                {
                    "similarity": 0.77,
                    "source_file": "docs/canonical.txt",
                    "content": "Canonical note",
                    "metadata": {"title": "Canonical", "trust_level": "canonical"},
                },
                {
                    "similarity": 0.83,
                    "source_file": "docs/deprecated.txt",
                    "content": "Deprecated note",
                    "metadata": {
                        "title": "Deprecated",
                        "trust_level": "canonical",
                        "deprecated": True,
                    },
                },
            ]
        )
    )

    results = asyncio.run(retriever.search([0.0] * 3, top_k=3, min_similarity=0.0))

    assert [item["title"] for item in results] == [
        "Canonical",
        "Reference",
        "Deprecated",
    ]
    assert results[0]["similarity"] > results[1]["similarity"] > results[2]["similarity"]
