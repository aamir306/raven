"""
Information Retriever — Stage 2 Orchestrator
=============================================
Coordinates retrieval sub-modules + OpenMetadata MCP integration:
  2.1  KeywordExtractor     – LLM-based keyword / entity / time extraction
  2.2  LSHMatcher           – Local MinHash entity matching
  2.3  FewShotRetriever     – Similar Q-SQL pairs (pgvector)
  2.4  GlossaryRetriever    – Semantic model / glossary (pgvector) + OM glossary
  2.5  DocRetriever         – Documentation chunks (pgvector)
  2.6  ContentAwareness     – Column-level metadata (local JSON → OM profiles)
  2.7  OM SemanticSearch    – Vector similarity search (OpenMetadata MCP)
  2.8  OM GlossarySearch    – Live glossary from OpenMetadata MCP

When OpenMetadata is available, OM results supplement/replace local sources.
Falls back to local-only mode when OM is unreachable.
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
        om_client: Any | None = None,
        lsh_index: Any = None,
        lsh_metadata: dict | None = None,
        content_awareness_path: str | None = None,
    ):
        self.openai = openai
        self.pgvector = pgvector
        self.om_client = om_client  # OpenMetadataMCPClient (optional)

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
            3. Run parallel searches (local + OM if available)
            4. Enrich matches with Content Awareness / OM profiles

        Returns:
            {
                "keywords": [...],
                "time_range": "..." | None,
                "entity_matches": [...],
                "similar_queries": [...],
                "glossary_matches": [...],
                "doc_snippets": [...],
                "content_awareness": [...],
                "om_table_candidates": [...],   # NEW: OM semantic search results
                "quality_warnings": [...],       # NEW: OM quality warnings
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

        # ── Step 3: Parallel retrieval (local + OpenMetadata) ──────────
        om_available = await self._check_om_available()

        # Build task list — always include local tasks
        tasks = [
            self.lsh_matcher.match(lsh_terms),                                # 0: entity_matches
            self.fewshot_retriever.search(question_embedding),                # 1: similar_queries
            self.glossary_retriever.search(question_embedding, metrics=metrics),  # 2: glossary_matches
            self.doc_retriever.search(question_embedding),                    # 3: doc_snippets
        ]

        # Add OM tasks when available
        if om_available:
            tasks.append(self.om_client.semantic_search(question, limit=20))  # 4: om_tables
            keyword_str = " ".join(keywords) if keywords else question
            tasks.append(self.om_client.search_glossary(keyword_str, limit=10))  # 5: om_glossary
        else:
            tasks.append(self._empty_list())  # 4: placeholder
            tasks.append(self._empty_list())  # 5: placeholder

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Unpack results, handling exceptions gracefully
        entity_matches = self._safe_result(results[0], [])
        similar_queries = self._safe_result(results[1], [])
        glossary_matches = self._safe_result(results[2], [])
        doc_snippets = self._safe_result(results[3], [])
        om_table_candidates = self._safe_result(results[4], [])
        om_glossary_terms = self._safe_result(results[5], [])

        # ── Step 3b: Merge OM glossary with local glossary ─────────────
        if om_glossary_terms:
            glossary_matches = self._merge_glossary(glossary_matches, om_glossary_terms)

        # ── Step 4: Content Awareness enrichment ───────────────────────
        # If OM is available, use OM table profiles; otherwise fall back to local JSON
        if om_available and om_table_candidates:
            awareness = await self._om_content_awareness(om_table_candidates, entity_matches)
        else:
            awareness = await self.content_awareness.lookup(entity_matches)

        logger.info(
            "Retrieval complete: %d entities, %d fewshot, %d glossary, %d docs, "
            "%d OM tables, %d OM glossary",
            len(entity_matches),
            len(similar_queries),
            len(glossary_matches),
            len(doc_snippets),
            len(om_table_candidates),
            len(om_glossary_terms),
        )

        return {
            "keywords": keywords,
            "time_range": time_range,
            "entity_matches": entity_matches,
            "similar_queries": similar_queries,
            "glossary_matches": glossary_matches,
            "doc_snippets": doc_snippets,
            "content_awareness": awareness,
            "om_table_candidates": [
                self._normalize_om_table(t) for t in om_table_candidates
            ] if om_table_candidates else [],
        }

    # ── OpenMetadata integration helpers ───────────────────────────────

    async def _check_om_available(self) -> bool:
        """Check if OpenMetadata client is configured and available."""
        if not self.om_client:
            return False
        try:
            return await self.om_client.is_available()
        except Exception:
            return False

    @staticmethod
    async def _empty_list() -> list:
        """Async placeholder that returns empty list."""
        return []

    @staticmethod
    def _safe_result(result: Any, default: Any) -> Any:
        """Extract result from asyncio.gather, returning default on exception."""
        if isinstance(result, BaseException):
            logger.debug("Retrieval task failed: %s", result)
            return default
        return result

    def _merge_glossary(self, local: list[dict], om_terms: list) -> list[dict]:
        """Merge OpenMetadata glossary terms with local pgvector glossary results."""
        merged = list(local)
        seen_terms = {g.get("term", "").lower() for g in local}

        for term in om_terms:
            # OM terms are OMGlossaryTerm objects or dicts
            if hasattr(term, "name"):
                name = term.name
                definition = term.description
                sql_frag = term.sql_fragment
                synonyms = term.synonyms
                score = term.score
            else:
                name = term.get("name", "")
                definition = term.get("description", "")
                sql_frag = term.get("sql_fragment", "")
                synonyms = term.get("synonyms", [])
                score = term.get("score", 0.5)

            if name.lower() not in seen_terms:
                merged.append({
                    "term": name,
                    "definition": definition,
                    "sql_fragment": sql_frag,
                    "synonyms": synonyms,
                    "similarity": score,
                    "source": "openmetadata",
                })
                seen_terms.add(name.lower())

        # Re-sort by similarity
        merged.sort(key=lambda g: g.get("similarity", 0), reverse=True)
        return merged

    async def _om_content_awareness(self, om_tables: list, entity_matches: list[dict]) -> list[dict]:
        """Build content awareness from OM table profiles instead of local JSON."""
        if not self.om_client:
            return await self.content_awareness.lookup(entity_matches)

        # Collect FQNs from OM table candidates
        fqns = []
        for t in om_tables:
            fqn = t.fqn if hasattr(t, "fqn") else t.get("fqn", "")
            if fqn:
                fqns.append(fqn)

        if not fqns:
            return await self.content_awareness.lookup(entity_matches)

        # Get profiles from OM (parallel, limited to top 15)
        try:
            profiles = await self.om_client.get_tables_with_profiles(fqns[:15])
        except Exception:
            logger.debug("OM profile fetch failed, falling back to local", exc_info=True)
            return await self.content_awareness.lookup(entity_matches)

        awareness: list[dict] = []
        for profile_data in profiles:
            table_fqn = profile_data.get("table", "")
            profile = profile_data.get("profile", {})
            if not profile:
                continue

            # Extract column-level profile data
            for col_profile in profile.get("columnProfile", []):
                awareness.append({
                    "table": table_fqn,
                    "column": col_profile.get("name", ""),
                    "data_type": col_profile.get("dataType", ""),
                    "format_pattern": "",
                    "distinct_count": col_profile.get("distinctCount"),
                    "null_pct": col_profile.get("nullProportion", col_profile.get("nullCount")),
                    "sample_values": col_profile.get("sampleValues", []),
                    "notes": "",
                    "source": "openmetadata",
                })

        # Supplement with local awareness for any entity matches not covered by OM
        if entity_matches:
            om_covered = {(a["table"], a["column"]) for a in awareness}
            uncovered = [
                m for m in entity_matches
                if (m.get("table", ""), m.get("column", "")) not in om_covered
            ]
            if uncovered:
                local_awareness = await self.content_awareness.lookup(uncovered)
                awareness.extend(local_awareness)

        return awareness

    @staticmethod
    def _normalize_om_table(t: Any) -> dict:
        """Normalize OM table result to dict for downstream consumption."""
        if hasattr(t, "fqn"):
            return {
                "fqn": t.fqn,
                "name": t.name,
                "description": t.description,
                "domain": t.domain,
                "tags": t.tags,
                "columns": t.columns,
                "quality_status": t.quality_status,
                "score": t.score,
            }
        return t if isinstance(t, dict) else {}

    # ── Hot-swap helpers (called after preprocessing refresh) ──────────

    def set_lsh_index(self, lsh_index: Any, metadata: dict | None = None) -> None:
        """Set or replace the LSH index."""
        self.lsh_matcher.set_index(lsh_index, metadata or {})

    def set_om_client(self, om_client: Any) -> None:
        """Set or replace the OpenMetadata MCP client."""
        self.om_client = om_client

    def reload_content_awareness(self, path: str | None = None) -> None:
        """Reload the content awareness artifact."""
        self.content_awareness.reload(path)
