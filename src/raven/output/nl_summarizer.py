"""
NL Summarizer — Stage 7.4
===========================
Generates a natural-language summary of query results.
Uses numeric summaries and column metadata (never raw row data)
to produce concise explanations.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from ..connectors.openai_client import OpenAIClient

logger = logging.getLogger(__name__)

PROMPT_PATH = Path(__file__).resolve().parents[3] / "prompts" / "out_nl_summary.txt"


class NLSummarizer:
    """Generate human-readable summary of SQL query results."""

    def __init__(self, openai: OpenAIClient):
        self.openai = openai
        self._prompt_template = PROMPT_PATH.read_text()

    async def summarize(
        self,
        question: str,
        sql: str,
        df: Any,
    ) -> str:
        """
        Generate a NL summary of query results.

        Args:
            question: User question.
            sql: Executed SQL.
            df: Result DataFrame.

        Returns:
            Human-readable summary string.
        """
        if df is None or len(df) == 0:
            return "The query returned no results."

        row_count = len(df)
        column_names = ", ".join(df.columns.tolist())

        # Numeric summaries (safe to send — aggregated, not raw)
        numeric_cols = df.select_dtypes(include=["number"])
        numeric_summaries = ""
        if not numeric_cols.empty:
            parts: list[str] = []
            for col in numeric_cols.columns:
                parts.append(
                    f"{col}: min={numeric_cols[col].min()}, "
                    f"max={numeric_cols[col].max()}, "
                    f"mean={numeric_cols[col].mean():.2f}"
                )
            numeric_summaries = "; ".join(parts)

        prompt = (
            self._prompt_template
            .replace("{user_question}", question)
            .replace("{sql}", sql)
            .replace("{row_count}", str(row_count))
            .replace("{column_names}", column_names)
            .replace("{numeric_summaries}", numeric_summaries or "No numeric columns")
        )

        response = await self.openai.complete(
            prompt=prompt, stage_name="out_summary",
        )

        summary = response.strip()
        logger.debug("NL summary: %d chars", len(summary))
        return summary
