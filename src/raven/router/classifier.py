"""
Stage 1: Difficulty Router
==========================
Classifies questions as SIMPLE / COMPLEX / AMBIGUOUS using GPT-4o-mini.
- SIMPLE  → fast path (1 candidate, skip probes + validation)
- COMPLEX → full path (probes + 3 candidates + pairwise selection)
- AMBIGUOUS → ask clarifying question
"""

from __future__ import annotations

import logging
from enum import Enum
from pathlib import Path

from ..connectors.openai_client import OpenAIClient

logger = logging.getLogger(__name__)

PROMPT_PATH = Path(__file__).resolve().parents[3] / "prompts" / "router_classify.txt"


class Difficulty(Enum):
    SIMPLE = "SIMPLE"
    COMPLEX = "COMPLEX"
    AMBIGUOUS = "AMBIGUOUS"


class DifficultyClassifier:
    """Classify user question difficulty for pipeline routing."""

    def __init__(self, openai: OpenAIClient):
        self.openai = openai
        self._prompt_template = PROMPT_PATH.read_text()

    async def classify(self, question: str) -> Difficulty:
        """Classify a question into SIMPLE, COMPLEX, or AMBIGUOUS."""
        prompt = self._prompt_template.replace("{user_question}", question)

        response = await self.openai.complete(
            prompt=prompt,
            stage_name="router",
        )

        label = response.strip().upper()

        # Parse — allow minor variations
        for d in Difficulty:
            if d.value in label:
                return d

        logger.warning("Router returned unexpected label '%s', defaulting to COMPLEX", label)
        return Difficulty.COMPLEX
