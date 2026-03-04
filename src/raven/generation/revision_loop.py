"""
Revision Loop — Stage 5.5
===========================
Validates generated SQL on Trino via EXPLAIN, classifies errors
using the 36-subtype taxonomy, and asks the LLM for a targeted fix.
Up to 2 retries per candidate.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from ..connectors.openai_client import OpenAIClient
from ..connectors.trino_connector import TrinoConnector
from ..safety.query_validator import validate_read_only
from .trino_dialect import TrinoDialect
from .divide_and_conquer import extract_sql

logger = logging.getLogger(__name__)

PROMPT_PATH = Path(__file__).resolve().parents[3] / "prompts" / "gen_revision.txt"
MAX_RETRIES = 2


class RevisionLoop:
    """Validate SQL on Trino and apply taxonomy-guided revisions."""

    def __init__(
        self,
        openai: OpenAIClient,
        trino: TrinoConnector,
        dialect: TrinoDialect | None = None,
    ):
        self.openai = openai
        self.trino = trino
        self.dialect = dialect or TrinoDialect()
        self._revision_prompt = PROMPT_PATH.read_text()

    async def validate_and_revise(
        self,
        sql: str,
        question: str,
        pruned_schema: str,
    ) -> str | None:
        """
        Validate SQL on Trino.  If EXPLAIN fails, classify the error
        and ask LLM for a targeted fix (up to MAX_RETRIES times).

        Args:
            sql: SQL candidate to validate.
            question: User question (for revision context).
            pruned_schema: Schema string (for revision context).

        Returns:
            Validated (possibly revised) SQL, or None if hopeless.
        """
        if not sql or not validate_read_only(sql):
            logger.warning("SQL failed read-only validation, skipping")
            return None

        current_sql = sql

        for attempt in range(MAX_RETRIES + 1):
            try:
                await asyncio.to_thread(self.trino.explain, current_sql)
                if attempt > 0:
                    logger.info("SQL valid after %d revision(s)", attempt)
                return current_sql  # ✓ Valid
            except Exception as e:
                error_msg = str(e)
                if attempt >= MAX_RETRIES:
                    logger.warning(
                        "SQL failed after %d retries: %s",
                        MAX_RETRIES, error_msg[:100],
                    )
                    return current_sql  # Return anyway — validator may still pick it

                # Classify → revise
                cat, sub, desc = self.dialect.classify_error(error_msg)
                logger.info(
                    "Revision attempt %d: %s/%s — %s",
                    attempt + 1, cat, sub, error_msg[:80],
                )
                current_sql = await self._revise(
                    current_sql, question, pruned_schema,
                    cat, sub, desc, error_msg,
                )

        return current_sql

    async def validate_batch(
        self,
        candidates: list[str],
        question: str,
        pruned_schema: str,
    ) -> list[str]:
        """
        Validate and revise a batch of candidates (parallel).

        Returns only candidates that either pass EXPLAIN or have
        been revised.  Empty candidates are dropped.
        """
        tasks = [
            self.validate_and_revise(sql, question, pruned_schema)
            for sql in candidates
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        validated: list[str] = []
        for r in results:
            if isinstance(r, str) and r.strip():
                validated.append(r)

        if not validated and candidates:
            # If all revision attempts failed, keep the first raw candidate
            validated.append(candidates[0])

        logger.info(
            "Revision loop: %d/%d candidates validated",
            len(validated), len(candidates),
        )
        return validated

    # ── Internal ───────────────────────────────────────────────────────

    async def _revise(
        self,
        failed_sql: str,
        question: str,
        pruned_schema: str,
        error_category: str,
        error_subtype: str,
        error_description: str,
        error_message: str,
    ) -> str:
        """Use LLM to fix a failed SQL query with taxonomy-guided context."""
        prompt = (
            self._revision_prompt
            .replace("{user_question}", question)
            .replace("{pruned_schema}", pruned_schema)
            .replace("{failed_sql}", failed_sql)
            .replace("{error_category}", error_category)
            .replace("{error_subtype}", error_subtype)
            .replace("{error_description}", error_description)
            .replace("{error_message}", error_message)
            .replace("{trino_dialect_rules}", self.dialect.rules_text)
        )

        response = await self.openai.complete(
            prompt=prompt, stage_name="gen_revision",
        )
        return extract_sql(response)
