"""
Execution Plan CoT Generator — Stage 5.2
==========================================
CHASE-SQL Strategy B: Chain-of-Thought that mirrors a physical
execution plan — scan → filter → join → aggregate → sort.

Best for: standard reporting queries with joins and aggregations.
"""

from __future__ import annotations

import logging
from pathlib import Path

from ..connectors.openai_client import OpenAIClient
from .divide_and_conquer import extract_sql

logger = logging.getLogger(__name__)

PROMPT_PATH = Path(__file__).resolve().parents[3] / "prompts" / "gen_execution_plan.txt"


class ExecutionPlanCoTGenerator:
    """Generate SQL via execution-plan chain-of-thought reasoning."""

    def __init__(self, openai: OpenAIClient):
        self.openai = openai
        self._prompt_template = PROMPT_PATH.read_text()

    async def generate(
        self,
        question: str,
        context: dict,
        stage_name: str = "gen_candidate_b",
    ) -> str:
        """
        Generate a SQL candidate using Execution Plan CoT strategy.

        The LLM is guided to reason step-by-step:
          1. Identify source tables to SCAN
          2. Apply WHERE filters
          3. Define JOIN order and conditions
          4. Specify aggregations (GROUP BY)
          5. Apply HAVING, ORDER BY, LIMIT

        Args:
            question: User question.
            context: Shared context dict.
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

        response = await self.openai.complete(prompt=prompt, stage_name=stage_name)
        sql = extract_sql(response)
        logger.debug("EP-CoT generator produced %d chars SQL", len(sql))
        return sql
