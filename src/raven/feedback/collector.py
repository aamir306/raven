"""
Stage 8: Feedback Collector
============================
- Log every query (question, SQL, difficulty, confidence, timing) → query_log table
- Accept thumbs-up / thumbs-down from users
- Thumbs-up → auto-embed (question, SQL) and add to few-shot index
- Thumbs-down → queue for correction review with optional corrected SQL
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from ..connectors.openai_client import OpenAIClient
from ..connectors.pgvector_store import PgVectorStore

logger = logging.getLogger(__name__)


class FeedbackCollector:
    """Collect and store user feedback for continuous improvement."""

    def __init__(self, pgvector: PgVectorStore, openai: OpenAIClient | None = None,
                 om_client: Any = None):
        self.pgvector = pgvector
        self.openai = openai  # Needed for embedding thumbs-up pairs
        self.om_client = om_client  # OpenMetadataMCPClient for write-back

    async def log_query(
        self,
        question: str,
        sql: str,
        difficulty: str,
        confidence: str,
        row_count: int,
        conversation_id: str | None = None,
    ) -> str:
        """Log a pipeline query execution to query_log table. Returns query_id."""
        query_id = str(uuid.uuid4())

        try:
            await asyncio.to_thread(
                self.pgvector.log_query,
                query_id=query_id,
                question=question,
                sql_text=sql,
                difficulty=difficulty,
                confidence=confidence,
                row_count=row_count,
                conversation_id=conversation_id,
            )
            logger.debug("Logged query %s: %s", query_id, question[:60])
        except Exception:
            logger.warning("Failed to log query %s (non-critical)", query_id, exc_info=True)

        return query_id

    async def submit_feedback(
        self,
        query_id: str,
        feedback: str,  # "thumbs_up" or "thumbs_down"
        correction_sql: str | None = None,
        correction_notes: str | None = None,
    ) -> dict:
        """
        Process user feedback.

        - thumbs_up: Auto-embed (question, SQL) and add to few-shot index.
        - thumbs_down: Store correction for human review.
        """
        result = {"query_id": query_id, "feedback": feedback, "action": ""}

        # Update query_log with feedback
        try:
            updated = await asyncio.to_thread(
                self.pgvector.update_feedback,
                query_id=query_id,
                feedback=feedback,
                correction_sql=correction_sql,
                correction_notes=correction_notes,
            )
            if not updated:
                result["action"] = "query_not_found"
                return result
        except Exception:
            logger.warning("Failed to update feedback for %s", query_id, exc_info=True)
            result["action"] = "update_failed"
            return result

        if feedback == "thumbs_up":
            # Auto-add to few-shot index
            result["action"] = await self._add_to_fewshot(query_id)

        elif feedback == "thumbs_down":
            if correction_sql:
                result["action"] = "correction_queued"
                logger.info(
                    "Thumbs down + correction for query %s — queuing for review",
                    query_id,
                )
            else:
                result["action"] = "flagged_for_review"
                logger.info("Thumbs down for query %s — flagged for review", query_id)

        return result

    async def _add_to_fewshot(self, query_id: str) -> str:
        """Embed the (question, SQL) pair and insert into question_embeddings."""
        if not self.openai:
            logger.warning("Cannot add to few-shot index — no OpenAI client configured")
            return "skipped_no_openai"

        try:
            # Fetch the original query
            query = await asyncio.to_thread(self.pgvector.get_query, query_id)
            if not query:
                return "query_not_found"

            question = query["question"]
            sql_text = query.get("sql_text", "")

            # Embed the question
            embedding = await self.openai.embed(question)

            # Insert into question_embeddings with source=feedback
            await asyncio.to_thread(
                self.pgvector.insert,
                table="question_embeddings",
                text=question,
                embedding=embedding,
                question_text=question,
                sql_query=sql_text,
                source="feedback_thumbs_up",
                metadata={
                    "query_id": query_id,
                    "source": "feedback",
                    "difficulty": query.get("difficulty", ""),
                },
            )
            logger.info("Added thumbs-up query %s to few-shot index", query_id)

            # ── OM Write-Back: push verified query to Knowledge Center ──
            if self.om_client:
                try:
                    tables = query.get("tables_used", [])
                    if not tables and sql_text:
                        # Extract tables from SQL as fallback
                        import re
                        tables = re.findall(
                            r'(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_.]*)',
                            sql_text, re.IGNORECASE
                        )
                    await self.om_client.on_thumbs_up(
                        question=question,
                        sql=sql_text,
                        tables=tables,
                    )
                    logger.info("Pushed verified query %s to OpenMetadata Knowledge Center", query_id)
                except Exception:
                    logger.debug("OM Knowledge Center write-back failed (non-critical)", exc_info=True)

            return "added_to_fewshot_index"

        except Exception:
            logger.warning("Failed to add query %s to few-shot index", query_id, exc_info=True)
            return "fewshot_insert_failed"

    async def push_glossary_term(self, term: str, definition: str,
                                 sql_fragment: str = "") -> dict:
        """Push a new business term to OpenMetadata glossary (write-back)."""
        result = {"term": term, "action": "skipped"}
        if not self.om_client:
            return result
        try:
            resp = await self.om_client.create_glossary_term(
                glossary="raven-business-terms",
                name=term,
                description=definition,
                sql_fragment=sql_fragment,
            )
            if resp:
                result["action"] = "created_in_openmetadata"
                logger.info("Pushed glossary term '%s' to OpenMetadata", term)
        except Exception:
            logger.debug("OM glossary write-back failed for '%s'", term, exc_info=True)
        return result

    async def push_relationship(self, from_table: str, to_table: str,
                                join_column: str) -> dict:
        """Push a discovered table relationship to OpenMetadata lineage."""
        result = {"from": from_table, "to": to_table, "action": "skipped"}
        if not self.om_client:
            return result
        try:
            resp = await self.om_client.on_relationship_discovered(
                from_table=from_table,
                to_table=to_table,
                join_column=join_column,
            )
            if resp:
                result["action"] = "created_lineage_in_openmetadata"
                logger.info("Pushed lineage edge %s → %s to OpenMetadata", from_table, to_table)
        except Exception:
            logger.debug("OM lineage write-back failed", exc_info=True)
        return result

    async def get_pending_corrections(self, limit: int = 50) -> list[dict]:
        """Get queries flagged for correction review."""
        try:
            return await asyncio.to_thread(
                self.pgvector.get_pending_corrections, limit
            )
        except Exception:
            logger.warning("Failed to get pending corrections", exc_info=True)
            return []

    async def approve_correction(
        self,
        query_id: str,
        corrected_sql: str,
    ) -> dict:
        """
        Approve a correction: update the query_log and add corrected pair to few-shot index.
        """
        try:
            # Update feedback status
            await asyncio.to_thread(
                self.pgvector.update_feedback,
                query_id=query_id,
                feedback="correction_approved",
                correction_sql=corrected_sql,
            )

            # Fetch the original query for the question text
            query = await asyncio.to_thread(self.pgvector.get_query, query_id)
            if query and self.openai:
                question = query["question"]
                embedding = await self.openai.embed(question)
                await asyncio.to_thread(
                    self.pgvector.insert,
                    table="question_embeddings",
                    text=question,
                    embedding=embedding,
                    question_text=question,
                    sql_query=corrected_sql,
                    source="feedback_correction",
                    metadata={
                        "query_id": query_id,
                        "source": "correction",
                        "original_sql": query.get("sql_text", ""),
                    },
                )

            logger.info("Correction approved for query %s", query_id)
            return {"query_id": query_id, "action": "correction_approved"}

        except Exception:
            logger.warning("Failed to approve correction for %s", query_id, exc_info=True)
            return {"query_id": query_id, "action": "approval_failed"}
