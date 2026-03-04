"""
Keyword Extractor — Stage 2.1
==============================
Uses LLM to extract structured information from user questions:
  - Keywords (table/column-relevant terms)
  - Time range (date bounds for filtering)
  - Metrics (KPI / measure names)
  - Entities (specific values like customer names, product codes)
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from ..connectors.openai_client import OpenAIClient

logger = logging.getLogger(__name__)

PROMPT_PATH = Path(__file__).resolve().parents[3] / "prompts" / "ir_keyword_extract.txt"


class KeywordExtractor:
    """Extract keywords, entities, time ranges, and metrics from natural language."""

    def __init__(self, openai: OpenAIClient):
        self.openai = openai
        self._prompt_template = PROMPT_PATH.read_text()

    async def extract(self, question: str) -> dict[str, Any]:
        """
        Extract structured components from a user question.

        Returns:
            {
                "keywords": ["revenue", "monthly", ...],
                "time_range": "last 30 days" | None,
                "metrics": ["revenue", "profit_margin"],
                "entities": ["ACME Corp", "SKU-1234"],
            }
        """
        prompt = self._prompt_template.replace("{user_question}", question)
        response = await self.openai.complete(
            prompt=prompt, stage_name="ir_keyword_extract",
        )
        return self._parse_response(response)

    @staticmethod
    def _parse_response(response: str) -> dict[str, Any]:
        """Parse the structured LLM response into a dict."""
        result: dict[str, Any] = {
            "keywords": [],
            "time_range": None,
            "metrics": [],
            "entities": [],
        }

        for line in response.strip().split("\n"):
            line = line.strip()
            if line.startswith("KEYWORDS:"):
                raw = line.split(":", 1)[1].strip()
                result["keywords"] = [
                    k.strip() for k in raw.split(",") if k.strip()
                ]
            elif line.startswith("TIME_RANGE:"):
                val = line.split(":", 1)[1].strip()
                result["time_range"] = None if val.upper() == "NONE" else val
            elif line.startswith("METRICS:"):
                val = line.split(":", 1)[1].strip()
                result["metrics"] = (
                    [] if val.upper() == "NONE"
                    else [m.strip() for m in val.split(",") if m.strip()]
                )
            elif line.startswith("ENTITIES:"):
                val = line.split(":", 1)[1].strip()
                result["entities"] = (
                    [] if val.upper() == "NONE"
                    else [e.strip() for e in val.split(",") if e.strip()]
                )

        logger.debug(
            "Extracted %d keywords, %d metrics, %d entities, time_range=%s",
            len(result["keywords"]),
            len(result["metrics"]),
            len(result["entities"]),
            result["time_range"],
        )
        return result
