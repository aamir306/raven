"""
Few-Shot Retriever — Stage 2.3
===============================
Searches pgvector for semantically similar past Q-SQL pairs.
Returns top-k examples used by the Few-Shot generator and
for cross-referencing table usage.
"""

from __future__ import annotations

import logging
from typing import Any

from ..connectors.pgvector_store import PgVectorStore

logger = logging.getLogger(__name__)

DEFAULT_TOP_K = 3
EMBEDDING_TABLE = "question_embeddings"


class FewShotRetriever:
    """Retrieve similar Q-SQL pairs from the validated history store."""

    def __init__(self, pgvector: PgVectorStore):
        self.pgvector = pgvector

    async def search(
        self,
        question_embedding: list[float],
        top_k: int = DEFAULT_TOP_K,
        min_similarity: float = 0.55,
    ) -> list[dict]:
        """
        Find the most similar past (question, SQL) pairs.

        Args:
            question_embedding: 3072-dim embedding of the user question.
            top_k: Maximum number of results.
            min_similarity: Cosine-similarity floor.

        Returns:
            [
                {
                    "question": "What was total revenue last month?",
                    "sql": "SELECT SUM(revenue) FROM ...",
                    "tables_used": ["gold.finance.orders"],
                    "similarity": 0.91,
                },
                ...
            ]
        """
        raw_results = self.pgvector.search(
            table_name=EMBEDDING_TABLE,
            query_embedding=question_embedding,
            top_k=top_k,
        )

        results: list[dict] = []
        for r in raw_results:
            sim = r.get("similarity", 0.0)
            if sim < min_similarity:
                continue
            meta = r.get("metadata") or {}
            results.append({
                "question": r.get("question_text") or meta.get("question_text", ""),
                "sql": r.get("sql_query") or meta.get("sql_query", ""),
                "tables_used": meta.get("tables_used", []),
                "similarity": sim,
            })

        logger.debug(
            "FewShot retrieval: %d/%d results above %.2f threshold",
            len(results), len(raw_results), min_similarity,
        )
        return results

    async def search_by_tables(
        self,
        table_names: list[str],
        top_k: int = 5,
    ) -> list[dict]:
        """
        Retrieve past queries targeting the same tables (metadata filter).

        Useful for augmenting few-shot examples when the user's question
        results in low vector-similarity hits.
        """
        # pgvector metadata-filter search (if supported by store)
        raw_results = self.pgvector.search(
            table_name=EMBEDDING_TABLE,
            query_embedding=[0.0] * 3072,  # placeholder — metadata filter does the real filtering
            top_k=top_k,
            metadata_filter={"tables_used": table_names},
        )

        results: list[dict] = []
        for r in raw_results:
            meta = r.get("metadata") or {}
            results.append({
                "question": r.get("question_text") or meta.get("question_text", ""),
                "sql": r.get("sql_query") or meta.get("sql_query", ""),
                "tables_used": meta.get("tables_used", []),
                "similarity": r.get("similarity", 0.0),
            })
        return results
