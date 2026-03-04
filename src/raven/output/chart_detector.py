"""
Chart Detector — Stage 7.2
============================
Detects the most appropriate chart type for query results
based on column types, row count, and query structure.

Supported chart types: TABLE, BAR, LINE, PIE, SCATTER, HEATMAP, KPI.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any

from ..connectors.openai_client import OpenAIClient

logger = logging.getLogger(__name__)

PROMPT_PATH = Path(__file__).resolve().parents[3] / "prompts" / "out_chart_detect.txt"


class ChartDetector:
    """Detect optimal chart type from query results metadata."""

    def __init__(self, openai: OpenAIClient):
        self.openai = openai
        self._prompt_template = PROMPT_PATH.read_text()

    async def detect(
        self,
        sql: str,
        column_info: str,
        row_count: int,
        sample_summary: str,
    ) -> dict:
        """
        Detect the best chart type for query results.

        Args:
            sql: The executed SQL.
            column_info: Column names and types (e.g., "revenue (double), month (varchar)").
            row_count: Number of result rows.
            sample_summary: Text summary of first few rows.

        Returns:
            {
                "type": "BAR" | "LINE" | "PIE" | "TABLE" | "KPI" | ...,
                "x_axis": "month" | None,
                "y_axis": "revenue" | None,
                "title": "Monthly Revenue",
            }
        """
        # Quick heuristics for obvious cases
        if row_count == 0:
            return {"type": "TABLE", "x_axis": None, "y_axis": None, "title": "No data"}
        if row_count == 1 and "," not in column_info:
            return {"type": "KPI", "x_axis": None, "y_axis": None, "title": ""}

        prompt = (
            self._prompt_template
            .replace("{sql}", sql)
            .replace("{column_info}", column_info)
            .replace("{row_count}", str(row_count))
            .replace("{sample_summary}", sample_summary)
        )

        response = await self.openai.complete(prompt=prompt, stage_name="out_chart")
        result = self._parse_response(response)
        logger.debug("Chart detection: %s", result.get("type"))
        return result

    @staticmethod
    def _parse_response(response: str) -> dict:
        """Parse LLM chart detection response."""
        result = {"type": "TABLE", "x_axis": None, "y_axis": None, "title": ""}
        for line in response.strip().split("\n"):
            line = line.strip()
            if line.startswith("CHART_TYPE:"):
                result["type"] = line.split(":", 1)[1].strip().upper()
            elif line.startswith("X_AXIS:"):
                val = line.split(":", 1)[1].strip()
                result["x_axis"] = None if val.upper() == "NONE" else val
            elif line.startswith("Y_AXIS:"):
                val = line.split(":", 1)[1].strip()
                result["y_axis"] = None if val.upper() == "NONE" else val
            elif line.startswith("TITLE:"):
                result["title"] = line.split(":", 1)[1].strip()
        return result
