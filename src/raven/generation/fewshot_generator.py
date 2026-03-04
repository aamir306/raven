"""
Few-Shot Generator — Stage 5.3
================================
CHASE-SQL Strategy C: Generate SQL by adapting the closest
past Q-SQL pairs retrieved in Stage 2.3.

Best for: repetitive questions or queries with well-established patterns.
"""

from __future__ import annotations

import logging
from pathlib import Path

from ..connectors.openai_client import OpenAIClient
from .divide_and_conquer import extract_sql

logger = logging.getLogger(__name__)

PROMPT_PATH = Path(__file__).resolve().parents[3] / "prompts" / "gen_fewshot.txt"


class FewShotGenerator:
    """Generate SQL by adapting similar past queries."""

    def __init__(self, openai: OpenAIClient):
        self.openai = openai
        self._prompt_template = PROMPT_PATH.read_text()

    async def generate(
        self,
        question: str,
        context: dict,
        stage_name: str = "gen_candidate_c",
    ) -> str:
        """
        Generate a SQL candidate using the Few-Shot strategy.

        The prompt includes up to 3 similar (question, SQL) examples
        from the validated history, letting the LLM adapt/combine them.

        Args:
            question: User question.
            context: Shared context dict (must include 'similar_queries').
            stage_name: LLM call identifier.

        Returns:
            Generated SQL string.
        """
        prompt = (
            self._prompt_template
            .replace("{user_question}", question)
            .replace("{trino_dialect_rules}", context.get("dialect_rules", ""))
            .replace("{pruned_schema}", context.get("pruned_schema", ""))
            .replace("{probe_evidence}", context.get("probe_evidence", ""))
            .replace("{glossary_definitions}", context.get("glossary_defs", ""))
            .replace("{few_shot_examples}", context.get("few_shot", ""))
        )

        # Fill in the up-to-3 example slots
        examples = context.get("similar_queries", [])
        for i in range(3):
            if i < len(examples):
                prompt = prompt.replace(
                    f"{{similar_q{i+1}}}", examples[i].get("question", "N/A"),
                )
                prompt = prompt.replace(
                    f"{{similar_sql{i+1}}}", examples[i].get("sql", "N/A"),
                )
            else:
                prompt = prompt.replace(f"{{similar_q{i+1}}}", "N/A")
                prompt = prompt.replace(f"{{similar_sql{i+1}}}", "N/A")

        response = await self.openai.complete(prompt=prompt, stage_name=stage_name)
        sql = extract_sql(response)
        logger.debug("FewShot generator produced %d chars SQL", len(sql))
        return sql
