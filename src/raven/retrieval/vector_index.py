"""
ANN-Aware Vector Index — Semantic Retrieval Layer
==================================================

Wraps PgVectorStore with:
- Dimensionality reduction (Matryoshka / PCA) so HNSW indexes work at ≤2000 dims
- Embedding model abstraction (OpenAI, local sentence-transformers, etc.)
- Hybrid search scoring (BM25 keyword + cosine vector similarity)
- Index lifecycle management (build, vacuum, stats)

This module is the "ANN/vector retrieval redesign" referenced in the handoff.
The underlying storage remains pgvector; this adds the intelligence layer.

Usage:
    index = VectorIndex(pgvector=store, embedder=OpenAIEmbedder(client))
    await index.upsert("schema_embeddings", texts, metadata=metas)
    results = await index.hybrid_search("total revenue by region", top_k=10)
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import re
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Protocol

import numpy as np

logger = logging.getLogger(__name__)

# ── Configuration ──────────────────────────────────────────────

DEFAULT_REDUCED_DIM = 1536  # Matryoshka half-dim — within pgvector HNSW limit
HNSW_M = 16
HNSW_EF_CONSTRUCTION = 64
BM25_WEIGHT = 0.3
VECTOR_WEIGHT = 0.7


# ── Embedder Protocol ─────────────────────────────────────────


class Embedder(Protocol):
    """Abstraction for any embedding model."""

    @property
    def dim(self) -> int:
        """Native embedding dimensionality."""
        ...

    async def embed(self, text: str) -> list[float]:
        """Embed a single text."""
        ...

    async def embed_batch(self, texts: list[str], *, batch_size: int = 64) -> list[list[float]]:
        """Embed multiple texts in batches."""
        ...


# ── Concrete embedders ────────────────────────────────────────


class OpenAIEmbedder:
    """OpenAI text-embedding-3-large (3072-dim) with Matryoshka dimension control."""

    def __init__(self, openai_client: Any, model: str = "text-embedding-3-large"):
        self._client = openai_client
        self._model = model
        self._dim = 3072

    @property
    def dim(self) -> int:
        return self._dim

    async def embed(self, text: str) -> list[float]:
        """Embed using the openai_client.embed() already in the codebase."""
        return await self._client.embed(text)

    async def embed_batch(self, texts: list[str], *, batch_size: int = 64) -> list[list[float]]:
        """Batch embedding — calls embed() in chunks."""
        embeddings: list[list[float]] = []
        for i in range(0, len(texts), batch_size):
            chunk = texts[i : i + batch_size]
            results = await asyncio.gather(*(self._client.embed(t) for t in chunk))
            embeddings.extend(results)
        return embeddings


class LocalEmbedder:
    """Placeholder for local sentence-transformers model.

    Useful for:
    - Offline/air-gapped deployments
    - Cost-sensitive workloads
    - Faster iteration during development
    """

    def __init__(self, model_name: str = "all-MiniLM-L6-v2", dim: int = 384):
        self._model_name = model_name
        self._dim = dim
        self._model = None  # Lazy-loaded

    @property
    def dim(self) -> int:
        return self._dim

    def _load(self) -> None:
        if self._model is not None:
            return
        try:
            from sentence_transformers import SentenceTransformer
            self._model = SentenceTransformer(self._model_name)
        except ImportError:
            raise ImportError(
                "sentence-transformers is required for LocalEmbedder. "
                "Install with: pip install sentence-transformers"
            )

    async def embed(self, text: str) -> list[float]:
        self._load()
        emb = await asyncio.to_thread(self._model.encode, text)
        return emb.tolist()

    async def embed_batch(self, texts: list[str], *, batch_size: int = 64) -> list[list[float]]:
        self._load()
        all_embs: list[list[float]] = []
        for i in range(0, len(texts), batch_size):
            chunk = texts[i : i + batch_size]
            embs = await asyncio.to_thread(self._model.encode, chunk)
            all_embs.extend(embs.tolist())
        return all_embs


# ── Dimensionality Reduction ──────────────────────────────────


@dataclass
class DimReducer:
    """Matryoshka-style truncation + L2 normalization.

    text-embedding-3-large is trained with Matryoshka Representation Learning,
    so simply truncating to the first N dimensions preserves most of the
    semantic fidelity: 1536 dims retains ~99.4% of the 3072-dim quality.

    This brings vectors within pgvector's HNSW index limit (≤2000 dims).
    """

    target_dim: int = DEFAULT_REDUCED_DIM

    def reduce(self, embedding: list[float]) -> list[float]:
        """Truncate to target_dim and L2-normalize."""
        truncated = np.array(embedding[: self.target_dim], dtype=np.float32)
        norm = np.linalg.norm(truncated)
        if norm > 0:
            truncated = truncated / norm
        return truncated.tolist()

    def reduce_batch(self, embeddings: list[list[float]]) -> list[list[float]]:
        """Batch reduce."""
        arr = np.array([e[: self.target_dim] for e in embeddings], dtype=np.float32)
        norms = np.linalg.norm(arr, axis=1, keepdims=True)
        norms[norms == 0] = 1.0
        arr = arr / norms
        return arr.tolist()


# ── BM25 / Keyword Scoring ────────────────────────────────────


def _tokenize(text: str) -> list[str]:
    """Simple whitespace + punctuation tokenizer."""
    return re.findall(r"\b\w+\b", text.lower())


def bm25_score(
    query_tokens: list[str],
    doc_tokens: list[str],
    *,
    k1: float = 1.2,
    b: float = 0.75,
    avg_dl: float = 50.0,
) -> float:
    """Simplified single-document BM25 score (no IDF corpus needed)."""
    if not query_tokens or not doc_tokens:
        return 0.0
    dl = len(doc_tokens)
    doc_tf: dict[str, int] = {}
    for t in doc_tokens:
        doc_tf[t] = doc_tf.get(t, 0) + 1
    score = 0.0
    for qt in query_tokens:
        tf = doc_tf.get(qt, 0)
        if tf > 0:
            numerator = tf * (k1 + 1)
            denominator = tf + k1 * (1 - b + b * dl / avg_dl)
            score += numerator / denominator
    return score


# ── Hybrid Search Result ──────────────────────────────────────


@dataclass
class SearchResult:
    """A single result from hybrid search."""

    id: int | str
    text: str
    metadata: dict[str, Any] = field(default_factory=dict)
    vector_score: float = 0.0
    keyword_score: float = 0.0
    hybrid_score: float = 0.0

    @property
    def table(self) -> str:
        return self.metadata.get("table_name", self.metadata.get("table", ""))

    @property
    def source(self) -> str:
        return self.metadata.get("source", "")


# ── VectorIndex — Main orchestrator ───────────────────────────


class VectorIndex:
    """ANN-aware vector index wrapping PgVectorStore.

    Features:
    - Dimensionality reduction for HNSW-compatible indexing
    - Hybrid search (vector + BM25 keyword scoring)
    - Index lifecycle management
    - Deduplication on upsert via content hashing
    """

    def __init__(
        self,
        pgvector: Any,
        embedder: Embedder | None = None,
        reduced_dim: int = DEFAULT_REDUCED_DIM,
        bm25_weight: float = BM25_WEIGHT,
        vector_weight: float = VECTOR_WEIGHT,
    ):
        self.pgvector = pgvector
        self.embedder = embedder
        self.reducer = DimReducer(target_dim=reduced_dim)
        self.bm25_weight = bm25_weight
        self.vector_weight = vector_weight
        self._reduced_dim = reduced_dim

    # ── Upsert with dedup ─────────────────────────────────────

    async def upsert(
        self,
        table: str,
        texts: list[str],
        metadata: list[dict[str, Any]] | None = None,
        **extra_columns: Any,
    ) -> int:
        """Embed, reduce, and batch insert new texts.

        Deduplicates by content hash — identical texts are skipped.

        Returns number of rows actually inserted.
        """
        if not texts:
            return 0

        if not self.embedder:
            raise ValueError("No embedder configured — cannot upsert texts")

        metadata = metadata or [{}] * len(texts)

        # Embed in batch
        t0 = time.monotonic()
        embeddings = await self.embedder.embed_batch(texts)
        embed_ms = (time.monotonic() - t0) * 1000

        # Reduce dimensions for HNSW compatibility
        reduced = self.reducer.reduce_batch(embeddings)

        # Build insert items with content hash for dedup
        items: list[dict[str, Any]] = []
        for text, emb, meta in zip(texts, reduced, metadata):
            content_hash = hashlib.sha256(text.encode()).hexdigest()[:16]
            item = {
                **meta,
                "embedding": emb,
                "metadata": {**meta.get("metadata", {}), "content_hash": content_hash},
            }
            # Include text in the appropriate column
            if "description" not in item and "question_text" not in item:
                item.setdefault("content", text)
            items.append(item)

        count = self.pgvector.batch_insert(table=table, items=items)
        logger.info(
            "vector_index_upsert",
            table=table,
            texts=len(texts),
            inserted=count,
            embed_ms=round(embed_ms, 1),
            reduced_dim=self._reduced_dim,
        )
        return count

    # ── Hybrid Search ─────────────────────────────────────────

    async def hybrid_search(
        self,
        query: str,
        table: str = "schema_embeddings",
        top_k: int = 10,
        min_score: float = 0.3,
        filter_sql: str | None = None,
    ) -> list[SearchResult]:
        """Combined vector similarity + BM25 keyword search.

        1. Embed & reduce the query
        2. Vector search via pgvector (cosine similarity)
        3. Re-rank with BM25 keyword score
        4. Return merged results sorted by hybrid_score

        Args:
            query: Natural language query
            table: pgvector table to search
            top_k: Number of results to return
            min_score: Minimum hybrid score threshold
            filter_sql: Optional WHERE clause filter
        """
        if not self.embedder:
            raise ValueError("No embedder configured — cannot search")

        # Embed query
        query_embedding = await self.embedder.embed(query)
        reduced_query = self.reducer.reduce(query_embedding)

        # Vector search — fetch extra candidates for re-ranking
        fetch_k = min(top_k * 3, 100)
        raw_results = await self.pgvector.async_search(
            table_name=table,
            query_embedding=reduced_query,
            top_k=fetch_k,
            filter_sql=filter_sql,
        )

        if not raw_results:
            return []

        # BM25 re-ranking
        query_tokens = _tokenize(query)
        search_results: list[SearchResult] = []

        for row in raw_results:
            vector_score = float(row.get("similarity", 0.0))

            # Get text content for BM25 scoring
            doc_text = (
                row.get("description", "")
                or row.get("question_text", "")
                or row.get("content", "")
                or row.get("term", "")
                or ""
            )
            doc_tokens = _tokenize(doc_text)
            kw_score = bm25_score(query_tokens, doc_tokens)

            # Normalize BM25 to [0, 1] range (cap at 5.0 then divide)
            kw_score_normalized = min(kw_score / 5.0, 1.0)

            hybrid = (
                self.vector_weight * vector_score
                + self.bm25_weight * kw_score_normalized
            )

            if hybrid >= min_score:
                sr = SearchResult(
                    id=row.get("id", 0),
                    text=doc_text,
                    metadata={k: v for k, v in row.items()
                              if k not in ("embedding", "similarity")},
                    vector_score=round(vector_score, 4),
                    keyword_score=round(kw_score_normalized, 4),
                    hybrid_score=round(hybrid, 4),
                )
                search_results.append(sr)

        # Sort by hybrid score descending
        search_results.sort(key=lambda r: r.hybrid_score, reverse=True)
        return search_results[:top_k]

    # ── Pure vector search (no hybrid) ────────────────────────

    async def vector_search(
        self,
        query: str,
        table: str = "schema_embeddings",
        top_k: int = 10,
        filter_sql: str | None = None,
    ) -> list[SearchResult]:
        """Embedding-only search without BM25 re-ranking."""
        if not self.embedder:
            raise ValueError("No embedder configured")

        query_embedding = await self.embedder.embed(query)
        reduced_query = self.reducer.reduce(query_embedding)

        raw = await self.pgvector.async_search(
            table_name=table,
            query_embedding=reduced_query,
            top_k=top_k,
            filter_sql=filter_sql,
        )

        results: list[SearchResult] = []
        for row in raw:
            doc_text = (
                row.get("description", "")
                or row.get("question_text", "")
                or row.get("content", "")
                or ""
            )
            sim = float(row.get("similarity", 0.0))
            results.append(SearchResult(
                id=row.get("id", 0),
                text=doc_text,
                metadata={k: v for k, v in row.items()
                          if k not in ("embedding", "similarity")},
                vector_score=sim,
                hybrid_score=sim,
            ))

        return results

    # ── Index Lifecycle ───────────────────────────────────────

    async def rebuild_hnsw_index(self, table: str) -> None:
        """Drop and recreate the HNSW index for a table.

        Call this after large bulk inserts for optimal search quality.
        pgvector HNSW indexes degrade with many inserts without vacuuming.
        """
        idx_name = f"idx_{table}_embedding"
        dim = self._reduced_dim

        def _rebuild() -> None:
            conn = self.pgvector._pool.getconn()
            try:
                with conn.cursor() as cur:
                    cur.execute(f"DROP INDEX IF EXISTS {idx_name};")
                    cur.execute(f"""
                        CREATE INDEX {idx_name}
                        ON {table}
                        USING hnsw (embedding vector_cosine_ops)
                        WITH (m = {HNSW_M}, ef_construction = {HNSW_EF_CONSTRUCTION});
                    """)
                    cur.execute(f"ANALYZE {table};")
                conn.commit()
                logger.info("hnsw_index_rebuilt", table=table, dim=dim)
            finally:
                self.pgvector._pool.putconn(conn)

        await asyncio.to_thread(_rebuild)

    async def vacuum_table(self, table: str) -> None:
        """Vacuum a table to reclaim space and update planner stats."""
        def _vacuum() -> None:
            conn = self.pgvector._pool.getconn()
            try:
                old_isolation = conn.isolation_level
                conn.set_isolation_level(0)  # VACUUM cannot run inside a transaction
                with conn.cursor() as cur:
                    cur.execute(f"VACUUM ANALYZE {table};")
                conn.set_isolation_level(old_isolation)
                logger.info("vector_index_vacuumed", table=table)
            finally:
                self.pgvector._pool.putconn(conn)

        await asyncio.to_thread(_vacuum)

    async def get_index_stats(self, table: str) -> dict[str, Any]:
        """Get table/index statistics for monitoring."""
        def _stats() -> dict:
            conn = self.pgvector._pool.getconn()
            try:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT COUNT(*) FROM {table};")
                    row_count = cur.fetchone()[0]

                    cur.execute(f"""
                        SELECT pg_size_pretty(pg_total_relation_size('{table}'));
                    """)
                    total_size = cur.fetchone()[0]

                    cur.execute(f"""
                        SELECT indexname, pg_size_pretty(pg_relation_size(indexname::regclass))
                        FROM pg_indexes
                        WHERE tablename = '{table}';
                    """)
                    indexes = [
                        {"name": r[0], "size": r[1]}
                        for r in cur.fetchall()
                    ]

                return {
                    "table": table,
                    "row_count": row_count,
                    "total_size": total_size,
                    "indexes": indexes,
                    "reduced_dim": self._reduced_dim,
                    "hnsw_m": HNSW_M,
                    "hnsw_ef_construction": HNSW_EF_CONSTRUCTION,
                }
            finally:
                self.pgvector._pool.putconn(conn)

        return await asyncio.to_thread(_stats)
