"""
Conversation Manager
====================
Manages multi-turn conversation context for follow-up questions.

When a user asks "What about last month?", the manager resolves this
using previous Q&A pairs from the same conversation_id.

Conversation history is stored in the query_log table (pgvector)
and injected into the pipeline context for downstream stages.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from .connectors.openai_client import OpenAIClient
from .connectors.pgvector_store import PgVectorStore

logger = logging.getLogger(__name__)

# Max previous turns to include in context
_MAX_HISTORY = 5

# System prompt for question rewriting
_REWRITE_SYSTEM = """You are a question rewriter for a data analytics system.
Given a conversation history and a follow-up question, rewrite the question
to be self-contained (understandable without the history).

Rules:
- If the question is already self-contained, return it unchanged.
- Resolve pronouns ("it", "that", "those") using the conversation context.
- Resolve time references ("last month", "same period") using the previous query's context.
- Preserve the user's intent exactly — don't add or remove requirements.
- Output ONLY the rewritten question, nothing else."""


class ConversationManager:
    """Manages multi-turn conversation context and question rewriting."""

    def __init__(self, openai: OpenAIClient, pgvector: PgVectorStore):
        self.openai = openai
        self.pgvector = pgvector

    async def resolve_question(
        self,
        question: str,
        conversation_id: str | None,
    ) -> dict[str, Any]:
        """
        Resolve a potentially ambiguous follow-up question using conversation history.

        Returns:
            {
                "resolved_question": str,  # self-contained version
                "original_question": str,
                "is_followup": bool,
                "history": list[dict],  # previous Q&A pairs
            }
        """
        if not conversation_id:
            return {
                "resolved_question": question,
                "original_question": question,
                "is_followup": False,
                "history": [],
            }

        # Fetch conversation history from query_log
        try:
            history = await asyncio.to_thread(
                self.pgvector.get_conversation_history,
                conversation_id,
                _MAX_HISTORY,
            )
        except Exception:
            logger.warning("Failed to fetch conversation history", exc_info=True)
            history = []

        if not history:
            return {
                "resolved_question": question,
                "original_question": question,
                "is_followup": False,
                "history": [],
            }

        # Check if this looks like a follow-up question
        is_followup = self._looks_like_followup(question)

        if not is_followup:
            return {
                "resolved_question": question,
                "original_question": question,
                "is_followup": False,
                "history": history,
            }

        # Rewrite the question to be self-contained
        resolved = await self._rewrite_question(question, history)

        logger.info(
            "Resolved follow-up: '%s' → '%s' (history_len=%d)",
            question[:60], resolved[:60], len(history),
        )

        return {
            "resolved_question": resolved,
            "original_question": question,
            "is_followup": True,
            "history": history,
        }

    @staticmethod
    def _looks_like_followup(question: str) -> bool:
        """Heuristic check if a question is likely a follow-up."""
        q = question.lower().strip()

        # Pronouns / references
        followup_signals = [
            "what about", "how about", "and for", "same but",
            "same for", "now show", "break it down", "break that",
            "compare that", "what if", "instead of",
            "last month", "last year", "previous", "next",
            " it ", " that ", " those ", " these ", " them ",
            "the same", "drill down", "filter by",
        ]
        for signal in followup_signals:
            if signal in q:
                return True

        # Very short questions are likely follow-ups
        if len(q.split()) <= 4 and not q.startswith(("how many", "what is", "show me", "list")):
            return True

        return False

    async def _rewrite_question(
        self,
        question: str,
        history: list[dict],
    ) -> str:
        """Use LLM to rewrite a follow-up question to be self-contained."""
        # Build conversation context
        history_text = ""
        for i, h in enumerate(history[-3:], 1):  # Last 3 turns max
            q = h.get("question", "")
            sql = h.get("sql_text", "")
            history_text += f"\nTurn {i}:\n  Question: {q}\n  SQL: {sql[:200]}\n"

        prompt = (
            f"Conversation history:{history_text}\n"
            f"Follow-up question: {question}\n\n"
            f"Rewritten self-contained question:"
        )

        try:
            resolved = await self.openai.complete(
                prompt=prompt,
                stage_name="conversation_rewrite",
                system_prompt=_REWRITE_SYSTEM,
                max_tokens=200,
                temperature=0,
            )
            return resolved.strip() or question
        except Exception:
            logger.warning("Question rewrite failed, using original", exc_info=True)
            return question
