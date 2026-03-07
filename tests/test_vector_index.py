"""Tests for the ANN-aware VectorIndex module."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np
import pytest

from src.raven.retrieval.vector_index import (
    DEFAULT_REDUCED_DIM,
    DimReducer,
    OpenAIEmbedder,
    SearchResult,
    VectorIndex,
    _tokenize,
    bm25_score,
)


# ── DimReducer tests ──────────────────────────────────────────


class TestDimReducer:
    def test_truncates_to_target(self):
        reducer = DimReducer(target_dim=4)
        emb = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]
        result = reducer.reduce(emb)
        assert len(result) == 4

    def test_l2_normalized(self):
        reducer = DimReducer(target_dim=3)
        emb = [3.0, 4.0, 0.0, 99.0]
        result = reducer.reduce(emb)
        norm = sum(x**2 for x in result) ** 0.5
        assert abs(norm - 1.0) < 1e-5

    def test_batch_reduce(self):
        reducer = DimReducer(target_dim=2)
        embeddings = [
            [1.0, 0.0, 99.0],
            [0.0, 1.0, 99.0],
        ]
        results = reducer.reduce_batch(embeddings)
        assert len(results) == 2
        assert len(results[0]) == 2
        # Each should be L2-normalized
        for r in results:
            norm = sum(x**2 for x in r) ** 0.5
            assert abs(norm - 1.0) < 1e-5

    def test_zero_vector_handled(self):
        """Zero vector should not cause division by zero."""
        reducer = DimReducer(target_dim=3)
        emb = [0.0, 0.0, 0.0, 0.0]
        result = reducer.reduce(emb)
        assert len(result) == 3
        assert all(x == 0.0 for x in result)

    def test_default_dim(self):
        reducer = DimReducer()
        assert reducer.target_dim == DEFAULT_REDUCED_DIM


# ── BM25 scoring tests ────────────────────────────────────────


class TestBM25:
    def test_exact_match_high_score(self):
        query = _tokenize("total revenue")
        doc = _tokenize("SELECT total revenue FROM sales")
        score = bm25_score(query, doc)
        assert score > 0

    def test_no_overlap_zero(self):
        query = _tokenize("total revenue")
        doc = _tokenize("employee headcount summary")
        score = bm25_score(query, doc)
        assert score == 0.0

    def test_empty_query_zero(self):
        assert bm25_score([], ["some", "tokens"]) == 0.0

    def test_empty_doc_zero(self):
        assert bm25_score(["some", "tokens"], []) == 0.0

    def test_repeated_terms_boost(self):
        query = _tokenize("revenue")
        doc_once = _tokenize("revenue report")
        doc_twice = _tokenize("revenue revenue report analysis")
        score_once = bm25_score(query, doc_once)
        score_twice = bm25_score(query, doc_twice)
        # More occurrences should give higher (or equal) score due to tf saturation
        assert score_twice >= score_once


# ── Tokenizer tests ───────────────────────────────────────────


class TestTokenize:
    def test_basic(self):
        assert _tokenize("Hello, World!") == ["hello", "world"]

    def test_numbers(self):
        assert _tokenize("top 10 tables") == ["top", "10", "tables"]

    def test_empty(self):
        assert _tokenize("") == []


# ── SearchResult tests ────────────────────────────────────────


class TestSearchResult:
    def test_table_from_metadata(self):
        sr = SearchResult(id=1, text="x", metadata={"table_name": "gold.sales"})
        assert sr.table == "gold.sales"

    def test_source_from_metadata(self):
        sr = SearchResult(id=1, text="x", metadata={"source": "metabase"})
        assert sr.source == "metabase"

    def test_defaults(self):
        sr = SearchResult(id=1, text="x")
        assert sr.table == ""
        assert sr.source == ""
        assert sr.hybrid_score == 0.0


# ── VectorIndex tests (with mocked pgvector + embedder) ──────


class TestVectorIndex:
    @pytest.fixture
    def mock_embedder(self):
        embedder = MagicMock()
        embedder.dim = 8
        embedder.embed = AsyncMock(return_value=[1.0] * 8)
        embedder.embed_batch = AsyncMock(return_value=[[1.0] * 8, [0.5] * 8])
        return embedder

    @pytest.fixture
    def mock_pgvector(self):
        store = MagicMock()
        store.batch_insert = MagicMock(return_value=2)
        store.async_search = AsyncMock(return_value=[
            {
                "id": 1,
                "description": "total revenue by region",
                "similarity": 0.92,
                "table_name": "gold.finance.orders",
            },
            {
                "id": 2,
                "description": "employee headcount summary",
                "similarity": 0.71,
                "table_name": "gold.hr.employees",
            },
        ])
        return store

    @pytest.fixture
    def index(self, mock_pgvector, mock_embedder):
        return VectorIndex(
            pgvector=mock_pgvector,
            embedder=mock_embedder,
            reduced_dim=4,
        )

    @pytest.mark.asyncio
    async def test_upsert(self, index, mock_pgvector, mock_embedder):
        count = await index.upsert(
            "schema_embeddings",
            ["revenue table", "employee table"],
            metadata=[{"table_name": "t1"}, {"table_name": "t2"}],
        )
        assert count == 2
        mock_embedder.embed_batch.assert_called_once()
        mock_pgvector.batch_insert.assert_called_once()

    @pytest.mark.asyncio
    async def test_hybrid_search(self, index, mock_pgvector, mock_embedder):
        results = await index.hybrid_search("total revenue", table="schema_embeddings", top_k=5)
        assert len(results) >= 1
        # First result should have higher hybrid score (both vector + keyword match)
        assert results[0].vector_score == 0.92
        assert results[0].keyword_score > 0  # "total revenue" matches "total revenue by region"

    @pytest.mark.asyncio
    async def test_hybrid_search_ranking(self, index, mock_pgvector, mock_embedder):
        results = await index.hybrid_search("total revenue", table="schema_embeddings")
        # Results should be sorted by hybrid_score descending
        for i in range(len(results) - 1):
            assert results[i].hybrid_score >= results[i + 1].hybrid_score

    @pytest.mark.asyncio
    async def test_vector_search(self, index, mock_pgvector, mock_embedder):
        results = await index.vector_search("revenue", table="schema_embeddings")
        assert len(results) == 2
        assert results[0].vector_score == 0.92

    @pytest.mark.asyncio
    async def test_no_embedder_raises(self, mock_pgvector):
        index = VectorIndex(pgvector=mock_pgvector, embedder=None)
        with pytest.raises(ValueError, match="No embedder"):
            await index.upsert("schema_embeddings", ["text"])
        with pytest.raises(ValueError, match="No embedder"):
            await index.hybrid_search("query")

    @pytest.mark.asyncio
    async def test_empty_results(self, mock_pgvector, mock_embedder):
        mock_pgvector.async_search = AsyncMock(return_value=[])
        index = VectorIndex(pgvector=mock_pgvector, embedder=mock_embedder, reduced_dim=4)
        results = await index.hybrid_search("obscure query")
        assert results == []

    @pytest.mark.asyncio
    async def test_min_score_filter(self, index, mock_pgvector, mock_embedder):
        # With a very high min_score, most results should be filtered out
        results = await index.hybrid_search("x", min_score=0.99)
        assert len(results) == 0 or all(r.hybrid_score >= 0.99 for r in results)


# ── OpenAIEmbedder tests ─────────────────────────────────────


class TestOpenAIEmbedder:
    @pytest.mark.asyncio
    async def test_embed_delegates(self):
        client = MagicMock()
        client.embed = AsyncMock(return_value=[0.1] * 3072)
        embedder = OpenAIEmbedder(client)
        result = await embedder.embed("test")
        assert len(result) == 3072
        client.embed.assert_called_once_with("test")

    def test_dim(self):
        embedder = OpenAIEmbedder(MagicMock())
        assert embedder.dim == 3072

    @pytest.mark.asyncio
    async def test_embed_batch(self):
        client = MagicMock()
        client.embed = AsyncMock(return_value=[0.1] * 3072)
        embedder = OpenAIEmbedder(client)
        results = await embedder.embed_batch(["a", "b"])
        assert len(results) == 2
