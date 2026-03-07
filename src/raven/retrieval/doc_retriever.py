"""
Doc Retriever — Stage 2.5
==========================
Searches pgvector for documentation snippets (wiki, README,
data-dictionary pages, dbt docs) that are relevant to the
user question.  Returns source-tagged text chunks.
"""

from __future__ import annotations

import logging
from typing import Any

from ..connectors.pgvector_store import PgVectorStore

logger = logging.getLogger(__name__)

DEFAULT_TOP_K = 3
DOC_TABLE = "doc_embeddings"
TRUST_SCORE_BONUS = {
    "reference": 0.0,
    "reviewed": 0.03,
    "canonical": 0.06,
}


class DocRetriever:
    """Retrieve relevant documentation chunks from pgvector."""

    def __init__(self, pgvector: PgVectorStore):
        self.pgvector = pgvector

    async def search(
        self,
        question_embedding: list[float],
        top_k: int = DEFAULT_TOP_K,
        min_similarity: float = 0.50,
    ) -> list[dict]:
        """
        Find documentation snippets related to the user question.

        Args:
            question_embedding: 3072-dim embedding of the user question.
            top_k: Maximum results to return.
            min_similarity: Cosine-similarity floor.

        Returns:
            [
                {
                    "source": "wiki/orders.md",
                    "table": "gold.finance.orders",
                    "content": "The orders table contains...",
                    "similarity": 0.82,
                },
                ...
            ]
        """
        raw_results = await self.pgvector.async_search(
            table_name=DOC_TABLE,
            query_embedding=question_embedding,
            top_k=max(top_k * 2, top_k + 2),
        )

        results: list[dict] = []
        for r in raw_results:
            sim = self._score_result(r)
            if sim < min_similarity:
                continue
            results.append(self._format_result(r, sim))

        results.sort(key=lambda item: item.get("similarity", 0.0), reverse=True)
        results = results[:top_k]

        logger.debug("Doc retrieval: returned %d snippets", len(results))
        return results

    async def search_for_tables(
        self,
        table_names: list[str],
        top_k: int = 5,
    ) -> list[dict]:
        """
        Retrieve documentation snippets specifically about given tables.

        Used after Schema Selection to enrich the generation prompt with
        table-specific usage notes, caveats, and join guidance.
        """
        raw_results = self.pgvector.search(
            table_name=DOC_TABLE,
            query_embedding=[0.0] * 3072,  # placeholder — metadata filter does the real filtering
            top_k=top_k,
            metadata_filter={"table": table_names},
        )

        results: list[dict] = []
        for r in raw_results:
            results.append(self._format_result(r, self._score_result(r)))
        return results

    @staticmethod
    def _score_result(result: dict) -> float:
        base_similarity = result.get("similarity", 0.0)
        meta = result.get("metadata") or {}
        trust_level = str(meta.get("trust_level", "reference")).lower()
        adjusted = base_similarity + TRUST_SCORE_BONUS.get(trust_level, 0.0)
        if meta.get("deprecated"):
            adjusted -= 0.12
        return max(0.0, min(1.0, adjusted))

    @staticmethod
    def _format_result(result: dict, similarity: float) -> dict:
        meta = result.get("metadata") or {}
        related_tables = meta.get("related_tables") or []
        return {
            "source": result.get("source_file") or meta.get("source", ""),
            "title": meta.get("title", ""),
            "table": result.get("table_ref") or meta.get("table", "") or (related_tables[0] if related_tables else ""),
            "content": result.get("content") or meta.get("content", ""),
            "similarity": similarity,
            "doc_kind": meta.get("doc_kind", meta.get("file_type", "")),
            "domain": meta.get("domain", ""),
            "owner": meta.get("owner", ""),
            "trust_level": meta.get("trust_level", "reference"),
            "related_tables": related_tables,
            "related_metrics": meta.get("related_metrics") or [],
            "tags": meta.get("tags") or [],
            "deprecated": bool(meta.get("deprecated", False)),
        }
