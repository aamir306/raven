"""
Stage 7: Output Renderer — Orchestrator
=========================================
Coordinates four output sub-modules:
  7.1  QueryExecutor  – Execute SQL on Trino
  7.2  ChartDetector  – Detect chart type
  7.3  ChartGenerator – Generate Vega-Lite chart config
  7.4  NLSummarizer   – Generate natural-language summary
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from ..connectors.openai_client import OpenAIClient
from ..connectors.trino_connector import TrinoConnector
from .query_executor import QueryExecutor
from .chart_detector import ChartDetector
from .chart_generator import ChartGenerator
from .nl_summarizer import NLSummarizer

logger = logging.getLogger(__name__)


class OutputRenderer:
    """Stage 7 orchestrator — execute + chart + summarize."""

    def __init__(self, openai: OpenAIClient, trino: TrinoConnector | None = None):
        self.openai = openai
        self.trino = trino

        # Sub-modules
        self.executor = QueryExecutor(trino) if trino else None
        self.chart_detector = ChartDetector(openai)
        self.chart_generator = ChartGenerator()
        self.nl_summarizer = NLSummarizer(openai)

    async def render(self, question: str, sql: str, df: Any) -> dict:
        """
        Render output: detect chart type + generate config + NL summary.

        Args:
            question: User question.
            sql: Executed SQL.
            df: Result DataFrame (already executed).

        Returns:
            {
                "chart_type": "BAR",
                "chart_config": {...},
                "summary": "Your revenue increased by 15%...",
            }
        """
        row_count = len(df) if df is not None else 0

        if df is None or row_count == 0:
            return {
                "chart_type": "TABLE",
                "chart_config": {"type": "TABLE", "title": "No data"},
                "summary": "The query returned no results.",
            }

        # Build metadata for chart detection
        column_info = ", ".join(f"{col} ({df[col].dtype})" for col in df.columns)
        sample_df = df.head(3)
        sample_summary = sample_df.to_string(index=False, max_colwidth=30)

        # Run chart detection and NL summary in parallel
        chart_result, summary = await asyncio.gather(
            self.chart_detector.detect(sql, column_info, row_count, sample_summary),
            self.nl_summarizer.summarize(question, sql, df),
        )

        # Generate chart config
        chart_config = await self.chart_generator.generate(
            chart_type=chart_result.get("type", "TABLE"),
            df=df,
            x_axis=chart_result.get("x_axis"),
            y_axis=chart_result.get("y_axis"),
            title=chart_result.get("title", ""),
        )

        return {
            "chart_type": chart_result.get("type", "TABLE"),
            "chart_config": chart_config,
            "summary": summary,
        }

    async def execute_and_render(self, question: str, sql: str) -> dict:
        """
        Execute SQL and render results in one call.

        Used when the pipeline delegates execution to Stage 7.
        """
        if not self.executor:
            return {
                "chart_type": "TABLE",
                "chart_config": {},
                "summary": "No Trino executor configured.",
                "df": None,
                "row_count": 0,
            }

        exec_result = await self.executor.execute(sql)
        df = exec_result.get("df")
        row_count = exec_result.get("row_count", 0)

        render_result = await self.render(question, sql, df)
        render_result["df"] = df
        render_result["row_count"] = row_count
        render_result["execution_error"] = exec_result.get("error", "")
        return render_result
