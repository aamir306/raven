"""
Selection Agent — Stage 6.1
==============================
Pairwise comparison of SQL candidates.
Given N candidates, runs all (N choose 2) pairwise LLM comparisons
and selects the winner by majority vote.

Implements the CHASE-SQL pairwise selection approach.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from ..connectors.openai_client import OpenAIClient

logger = logging.getLogger(__name__)

PROMPT_PATH = Path(__file__).resolve().parents[3] / "prompts" / "val_pairwise_compare.txt"


class SelectionAgent:
    """Pairwise comparison → select the best SQL candidate."""

    def __init__(self, openai: OpenAIClient):
        self.openai = openai
        self._prompt_template = PROMPT_PATH.read_text()

    async def select(
        self,
        candidates: list[str],
        question: str,
        pruned_schema: str,
    ) -> str:
        """
        Run pairwise comparisons and return the winning candidate.

        If only 1 candidate, return it directly.

        Args:
            candidates: List of SQL strings.
            question: User question.
            pruned_schema: Schema used for generation.

        Returns:
            The winning SQL string.
        """
        if len(candidates) < 2:
            return candidates[0]

        # Build all pairs
        scores: dict[int, float] = {i: 0.0 for i in range(len(candidates))}
        pairs = [
            (i, j) for i in range(len(candidates))
            for j in range(i + 1, len(candidates))
        ]

        async def compare(i: int, j: int) -> tuple[int, int, str]:
            prompt = (
                self._prompt_template
                .replace("{user_question}", question)
                .replace("{pruned_schema}", pruned_schema)
                .replace("{sql_a}", candidates[i])
                .replace("{sql_b}", candidates[j])
            )
            response = await self.openai.complete(
                prompt=prompt, stage_name="val_pairwise",
            )
            return i, j, response

        # Run all comparisons in parallel
        results = await asyncio.gather(*[compare(i, j) for i, j in pairs])

        for i, j, response in results:
            upper = response.upper()
            if "WINNER: A" in upper:
                scores[i] += 1
            elif "WINNER: B" in upper:
                scores[j] += 1
            else:
                # Tie or parse failure — half credit each
                scores[i] += 0.5
                scores[j] += 0.5

        winner_idx = max(scores, key=scores.get)  # type: ignore[arg-type]
        logger.info(
            "Pairwise selection: candidate %d won (scores: %s)", winner_idx, scores,
        )
        return candidates[winner_idx]
