"""
Stage 7: Output Renderer
=========================
- Auto-detect chart type from query results
- Generate NL summary of results
- Format data for API response
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from ..connectors.openai_client import OpenAIClient

logger = logging.getLogger(__name__)

PROMPTS_DIR = Path(__file__).resolve().parents[3] / "prompts"


class OutputRenderer:
    """Render query results with chart detection and NL summary."""

    def __init__(self, openai: OpenAIClient):
        self.openai = openai
        self._chart_prompt = (PROMPTS_DIR / "out_chart_detect.txt").read_text()
        self._summary_prompt = (PROMPTS_DIR / "out_nl_summary.txt").read_text()

    async def render(self, question: str, sql: str, df: Any) -> dict:
        """
        Render output: detect chart type + generate NL summary.

        Returns dict: {chart_type, chart_config, summary}
        """
        import asyncio

        row_count = len(df)
        column_info = ", ".join(f"{col} ({df[col].dtype})" for col in df.columns)
        column_names = ", ".join(df.columns.tolist())

        # Sample summary (first 3 rows, sanitized)
        sample_df = df.head(3)
        sample_summary = sample_df.to_string(index=False, max_colwidth=30)

        # Numeric summaries
        numeric_cols = df.select_dtypes(include=["number"])
        numeric_summaries = ""
        if not numeric_cols.empty:
            summaries = []
            for col in numeric_cols.columns:
                summaries.append(
                    f"{col}: min={numeric_cols[col].min()}, "
                    f"max={numeric_cols[col].max()}, "
                    f"mean={numeric_cols[col].mean():.2f}"
                )
            numeric_summaries = "; ".join(summaries)

        # Run chart detection and summary in parallel
        chart_task = self._detect_chart(sql, column_info, row_count, sample_summary)
        summary_task = self._generate_summary(
            question, sql, row_count, column_names, numeric_summaries,
        )

        chart_result, summary = await asyncio.gather(chart_task, summary_task)

        return {
            "chart_type": chart_result.get("type", "TABLE"),
            "chart_config": chart_result,
            "summary": summary,
        }

    async def _detect_chart(
        self,
        sql: str,
        column_info: str,
        row_count: int,
        sample_summary: str,
    ) -> dict:
        """Detect best chart type for results."""
        prompt = (
            self._chart_prompt
            .replace("{sql}", sql)
            .replace("{column_info}", column_info)
            .replace("{row_count}", str(row_count))
            .replace("{sample_summary}", sample_summary)
        )

        response = await self.openai.complete(prompt=prompt, stage_name="out_chart")

        # Parse response
        result = {"type": "TABLE", "x_axis": None, "y_axis": None, "title": ""}
        for line in response.strip().split("\n"):
            line = line.strip()
            if line.startswith("CHART_TYPE:"):
                result["type"] = line.split(":", 1)[1].strip()
            elif line.startswith("X_AXIS:"):
                val = line.split(":", 1)[1].strip()
                result["x_axis"] = None if val.upper() == "NONE" else val
            elif line.startswith("Y_AXIS:"):
                val = line.split(":", 1)[1].strip()
                result["y_axis"] = None if val.upper() == "NONE" else val
            elif line.startswith("TITLE:"):
                result["title"] = line.split(":", 1)[1].strip()

        return result

    async def _generate_summary(
        self,
        question: str,
        sql: str,
        row_count: int,
        column_names: str,
        numeric_summaries: str,
    ) -> str:
        """Generate NL summary of query results."""
        prompt = (
            self._summary_prompt
            .replace("{user_question}", question)
            .replace("{sql}", sql)
            .replace("{row_count}", str(row_count))
            .replace("{column_names}", column_names)
            .replace("{numeric_summaries}", numeric_summaries or "No numeric columns")
        )

        response = await self.openai.complete(prompt=prompt, stage_name="out_summary")
        return response.strip()
