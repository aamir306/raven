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
            question_embedding: 1536-dim embedding of the user question.
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
        raw_results = self.pgvector.search(
            table_name=DOC_TABLE,
            query_embedding=question_embedding,
            top_k=top_k,
        )

        results: list[dict] = []
        for r in raw_results:
            sim = r.get("similarity", 0.0)
            if sim < min_similarity:
                continue
            meta = r.get("metadata", {})
            results.append({
                "source": meta.get("source", ""),
                "table": meta.get("table", ""),
                "content": meta.get("content", ""),
                "similarity": sim,
            })

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
            query_embedding=[0.0] * 1536,  # placeholder
            top_k=top_k,
            metadata_filter={"table": table_names},
        )

        results: list[dict] = []
        for r in raw_results:
            meta = r.get("metadata", {})
            results.append({
                "source": meta.get("source", ""),
                "table": meta.get("table", ""),
                "content": meta.get("content", ""),
                "similarity": r.get("similarity", 0.0),
            })
        return results
