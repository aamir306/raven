"""
Glossary Retriever — Stage 2.4
===============================
Searches pgvector for semantic-model / glossary entries relevant
to the user question.  Returns term definitions and canonical SQL
fragments (e.g., "churn_rate → 1 - (active / total)").
"""

from __future__ import annotations

import logging
from typing import Any

from ..connectors.pgvector_store import PgVectorStore

logger = logging.getLogger(__name__)

DEFAULT_TOP_K = 5
GLOSSARY_TABLE = "glossary_embeddings"


class GlossaryRetriever:
    """Retrieve glossary / semantic-model definitions from pgvector."""

    def __init__(self, pgvector: PgVectorStore):
        self.pgvector = pgvector

    async def search(
        self,
        question_embedding: list[float],
        metrics: list[str] | None = None,
        top_k: int = DEFAULT_TOP_K,
        min_similarity: float = 0.50,
    ) -> list[dict]:
        """
        Find glossary entries matching the user question semantics.

        Args:
            question_embedding: 1536-dim embedding of the user question.
            metrics: Metric names extracted by KeywordExtractor (boost scoring).
            top_k: Maximum number of results.
            min_similarity: Cosine-similarity floor.

        Returns:
            [
                {
                    "term": "churn_rate",
                    "definition": "Percentage of customers who cancelled in period.",
                    "sql_fragment": "1 - (COUNT(DISTINCT active_id) / NULLIF(COUNT(DISTINCT customer_id), 0))",
                    "synonyms": ["attrition", "customer_loss"],
                    "similarity": 0.88,
                },
                ...
            ]
        """
        raw_results = self.pgvector.search(
            table_name=GLOSSARY_TABLE,
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
                "term": meta.get("term", ""),
                "definition": meta.get("definition", ""),
                "sql_fragment": meta.get("sql_fragment", ""),
                "synonyms": meta.get("synonyms", []),
                "similarity": sim,
            })

        # Boost entries whose term matches an extracted metric name
        if metrics:
            metric_set = {m.lower() for m in metrics}
            for entry in results:
                if entry["term"].lower() in metric_set:
                    entry["similarity"] = min(entry["similarity"] + 0.10, 1.0)
            results.sort(key=lambda e: e["similarity"], reverse=True)

        logger.debug(
            "Glossary retrieval: %d results (metrics boost applied: %s)",
            len(results),
            bool(metrics),
        )
        return results
