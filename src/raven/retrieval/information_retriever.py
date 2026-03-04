"""
Information Retriever — Stage 2 Orchestrator
=============================================
Coordinates six parallel retrieval sub-modules:
  2.1  KeywordExtractor   – LLM-based keyword / entity / time extraction
  2.2  LSHMatcher          – Local MinHash entity matching
  2.3  FewShotRetriever    – Similar Q-SQL pairs (pgvector)
  2.4  GlossaryRetriever   – Semantic model / glossary (pgvector)
  2.5  DocRetriever         – Documentation chunks (pgvector)
  2.6  ContentAwareness     – Column-level metadata (local JSON)

All vector searches run in parallel after keyword extraction.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from ..connectors.openai_client import OpenAIClient
from ..connectors.pgvector_store import PgVectorStore
from .keyword_extractor import KeywordExtractor
from .lsh_matcher import LSHMatcher
from .fewshot_retriever import FewShotRetriever
from .glossary_retriever import GlossaryRetriever
from .doc_retriever import DocRetriever
from .content_awareness import ContentAwareness

logger = logging.getLogger(__name__)


class InformationRetriever:
    """Stage 2 orchestrator — assemble a full context bundle for downstream stages."""

    def __init__(
        self,
        openai: OpenAIClient,
        pgvector: PgVectorStore,
        lsh_index: Any = None,
        lsh_metadata: dict | None = None,
        content_awareness_path: str | None = None,
    ):
        self.openai = openai
        self.pgvector = pgvector

        # Sub-modules
        self.keyword_extractor = KeywordExtractor(openai)
        self.lsh_matcher = LSHMatcher(lsh_index, lsh_metadata)
        self.fewshot_retriever = FewShotRetriever(pgvector)
        self.glossary_retriever = GlossaryRetriever(pgvector)
        self.doc_retriever = DocRetriever(pgvector)
        self.content_awareness = ContentAwareness(content_awareness_path)

    async def retrieve(self, question: str, difficulty: Any = None) -> dict:
        """
        Run all retrieval operations and return a context bundle.

        Flow:
            1. Extract keywords/entities/time (LLM call)
            2. Embed question (OpenAI)
            3. Run 4 parallel searches + 1 local LSH match
            4. Enrich matches with Content Awareness metadata

        Returns:
            {
                "keywords": [...],
                "time_range": "..." | None,
                "entity_matches": [...],
                "similar_queries": [...],
                "glossary_matches": [...],
                "doc_snippets": [...],
                "content_awareness": [...],
            }
        """
        # ── Step 1: Keyword extraction (LLM) ──────────────────────────
        kw_result = await self.keyword_extractor.extract(question)

        keywords = kw_result.get("keywords", [])
        time_range = kw_result.get("time_range")
        metrics = kw_result.get("metrics", [])
        entities = kw_result.get("entities", [])

        # Combine keywords + entities for LSH matching
        lsh_terms = list(set(keywords + entities))

        # ── Step 2: Embed question ─────────────────────────────────────
        question_embedding = await self.openai.embed(question)

        # ── Step 3: Parallel retrieval ─────────────────────────────────
        (
            entity_matches,
            similar_queries,
            glossary_matches,
            doc_snippets,
        ) = await asyncio.gather(
            self.lsh_matcher.match(lsh_terms),
            self.fewshot_retriever.search(question_embedding),
            self.glossary_retriever.search(question_embedding, metrics=metrics),
            self.doc_retriever.search(question_embedding),
        )

        # ── Step 4: Content Awareness enrichment ───────────────────────
        awareness = await self.content_awareness.lookup(entity_matches)

        logger.info(
            "Retrieval complete: %d entities, %d fewshot, %d glossary, %d docs",
            len(entity_matches),
            len(similar_queries),
            len(glossary_matches),
            len(doc_snippets),
        )

        return {
            "keywords": keywords,
            "time_range": time_range,
            "entity_matches": entity_matches,
            "similar_queries": similar_queries,
            "glossary_matches": glossary_matches,
            "doc_snippets": doc_snippets,
            "content_awareness": awareness,
        }

    # ── Hot-swap helpers (called after preprocessing refresh) ──────────

    def set_lsh_index(self, lsh_index: Any, metadata: dict | None = None) -> None:
        """Set or replace the LSH index."""
        self.lsh_matcher.set_index(lsh_index, metadata or {})

    def reload_content_awareness(self, path: str | None = None) -> None:
        """Reload the content awareness artifact."""
        self.content_awareness.reload(path)
