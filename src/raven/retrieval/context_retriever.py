"""
Stage 2: Context Retriever
==========================
Five parallel retrieval operations:
  1. Keyword & entity extraction (LLM)
  2. Entity matching via MinHash LSH (local)
  3. Similar Q-SQL pairs (pgvector)
  4. Semantic model / glossary lookup (pgvector)
  5. Documentation retrieval (pgvector)
  + Content Awareness metadata for matched columns
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any

from ..connectors.openai_client import OpenAIClient
from ..connectors.pgvector_store import PgVectorStore

logger = logging.getLogger(__name__)

PROMPT_PATH = Path(__file__).resolve().parents[3] / "prompts" / "ir_keyword_extract.txt"


class ContextRetriever:
    """Retrieve context bundle for the pipeline."""

    def __init__(self, openai: OpenAIClient, pgvector: PgVectorStore):
        self.openai = openai
        self.pgvector = pgvector
        self._prompt_template = PROMPT_PATH.read_text()
        self._lsh_index = None  # Loaded during preprocessing

    async def retrieve(self, question: str, difficulty: Any = None) -> dict:
        """
        Run all retrieval operations in parallel and return context bundle.

        Returns dict with keys:
            keywords, time_range, entity_matches, similar_queries,
            glossary_matches, doc_snippets, content_awareness
        """
        # Step 1: Extract keywords (LLM call)
        keywords_task = self._extract_keywords(question)

        # Step 2-5: Run in parallel after we have keywords
        keyword_result = await keywords_task

        keywords = keyword_result.get("keywords", [])
        time_range = keyword_result.get("time_range")
        metrics = keyword_result.get("metrics", [])

        # Embed question for vector search
        question_embedding = await self.openai.embed(question)

        # Run parallel retrieval
        entity_task = self._match_entities(keywords)
        similar_task = self._search_similar_queries(question_embedding)
        glossary_task = self._search_glossary(question_embedding, metrics)
        docs_task = self._search_docs(question_embedding)

        entity_matches, similar_queries, glossary_matches, doc_snippets = await asyncio.gather(
            entity_task, similar_task, glossary_task, docs_task
        )

        # Content Awareness for matched columns
        content_awareness = await self._get_content_awareness(entity_matches)

        return {
            "keywords": keywords,
            "time_range": time_range,
            "entity_matches": entity_matches,
            "similar_queries": similar_queries,
            "glossary_matches": glossary_matches,
            "doc_snippets": doc_snippets,
            "content_awareness": content_awareness,
        }

    async def _extract_keywords(self, question: str) -> dict:
        """Use LLM to extract keywords, time range, metrics, entities."""
        prompt = self._prompt_template.replace("{user_question}", question)
        response = await self.openai.complete(prompt=prompt, stage_name="ir_keyword_extract")

        result: dict[str, Any] = {"keywords": [], "time_range": None, "metrics": [], "entities": []}
        for line in response.strip().split("\n"):
            line = line.strip()
            if line.startswith("KEYWORDS:"):
                result["keywords"] = [k.strip() for k in line.split(":", 1)[1].split(",") if k.strip()]
            elif line.startswith("TIME_RANGE:"):
                val = line.split(":", 1)[1].strip()
                result["time_range"] = None if val.upper() == "NONE" else val
            elif line.startswith("METRICS:"):
                val = line.split(":", 1)[1].strip()
                result["metrics"] = [] if val.upper() == "NONE" else [m.strip() for m in val.split(",")]
            elif line.startswith("ENTITIES:"):
                val = line.split(":", 1)[1].strip()
                result["entities"] = [] if val.upper() == "NONE" else [e.strip() for e in val.split(",")]

        return result

    async def _match_entities(self, keywords: list[str]) -> list[dict]:
        """Match keywords against MinHash LSH index (local, no API call)."""
        if not self._lsh_index or not keywords:
            return []

        matches = []
        for keyword in keywords:
            # LSH matching runs locally — never sends data to API
            lsh_results = self._lsh_index.query(keyword)
            for result in lsh_results:
                matches.append({
                    "keyword": keyword,
                    "table": result["table"],
                    "column": result["column"],
                    "matched_value": result.get("value"),
                    "similarity": result.get("similarity", 0.0),
                })
        return matches

    async def _search_similar_queries(self, embedding: list[float]) -> list[dict]:
        """Search pgvector for similar past Q-SQL pairs."""
        results = self.pgvector.search(
            table_name="question_embeddings",
            query_embedding=embedding,
            top_k=3,
        )
        return [
            {
                "question": r.get("metadata", {}).get("question_text", ""),
                "sql": r.get("metadata", {}).get("sql_query", ""),
                "similarity": r.get("similarity", 0.0),
            }
            for r in results
        ]

    async def _search_glossary(self, embedding: list[float], metrics: list[str]) -> list[dict]:
        """Search semantic model / glossary for matching terms."""
        results = self.pgvector.search(
            table_name="glossary_embeddings",
            query_embedding=embedding,
            top_k=5,
        )
        return [
            {
                "term": r.get("metadata", {}).get("term", ""),
                "definition": r.get("metadata", {}).get("definition", ""),
                "sql_fragment": r.get("metadata", {}).get("sql_fragment", ""),
                "synonyms": r.get("metadata", {}).get("synonyms", []),
                "similarity": r.get("similarity", 0.0),
            }
            for r in results
        ]

    async def _search_docs(self, embedding: list[float]) -> list[dict]:
        """Search documentation index."""
        results = self.pgvector.search(
            table_name="doc_embeddings",
            query_embedding=embedding,
            top_k=3,
        )
        return [
            {
                "source": r.get("metadata", {}).get("source", ""),
                "table": r.get("metadata", {}).get("table", ""),
                "content": r.get("metadata", {}).get("content", ""),
                "similarity": r.get("similarity", 0.0),
            }
            for r in results
        ]

    async def _get_content_awareness(self, entity_matches: list[dict]) -> list[dict]:
        """Retrieve Content Awareness metadata for matched columns."""
        # Content Awareness is loaded during preprocessing and stored in pgvector
        # or as local JSON. For now, return entity matches with metadata placeholder.
        awareness = []
        for match in entity_matches:
            awareness.append({
                "table": match.get("table", ""),
                "column": match.get("column", ""),
                "data_type": match.get("data_type", ""),
                "format_pattern": match.get("format_pattern", ""),
                "distinct_count": match.get("distinct_count"),
                "null_pct": match.get("null_pct"),
                "notes": match.get("notes", ""),
            })
        return awareness

    def set_lsh_index(self, index: Any) -> None:
        """Set the MinHash LSH index (loaded during preprocessing)."""
        self._lsh_index = index
