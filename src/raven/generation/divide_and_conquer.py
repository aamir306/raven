"""
Divide-and-Conquer Generator — Stage 5.1
==========================================
CHASE-SQL Strategy A: Decompose the user question into sub-questions,
generate CTEs for each, then compose them into a final SELECT.

Best for: deeply nested or multi-step analytical questions.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

from ..connectors.openai_client import OpenAIClient

logger = logging.getLogger(__name__)

PROMPT_PATH = Path(__file__).resolve().parents[3] / "prompts" / "gen_divide_conquer.txt"


class DivideAndConquerGenerator:
    """Generate SQL via sub-question decomposition into CTEs."""

    def __init__(self, openai: OpenAIClient):
        self.openai = openai
        self._prompt_template = PROMPT_PATH.read_text()

    async def generate(
        self,
        question: str,
        context: dict,
        stage_name: str = "gen_candidate_a",
    ) -> str:
        """
        Generate a SQL candidate using the Divide-and-Conquer strategy.

        Args:
            question: User question.
            context: Shared context dict with keys:
                pruned_schema, probe_evidence, glossary_defs,
                few_shot, dialect_rules.
            stage_name: LLM call identifier for cost tracking.

        Returns:
            Generated SQL string (no trailing semicolons).
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
        logger.debug("DC generator produced %d chars SQL", len(sql))
        return sql


# ── Shared SQL Extraction Utility ──────────────────────────────────────

def extract_sql(response: str) -> str:
    """
    Extract SQL from an LLM response.

    Handles:
      - Markdown ```sql ... ``` code blocks
      - Bare SELECT/WITH statements
      - Fallback: return full response
    """
    # Try markdown code blocks first
    code_block = re.search(
        r"```(?:sql)?\s*\n?(.*?)```", response, re.DOTALL | re.IGNORECASE,
    )
    if code_block:
        return code_block.group(1).strip().rstrip(";")

    # Look for SELECT/WITH at start of line
    lines = response.strip().split("\n")
    sql_lines: list[str] = []
    in_sql = False
    for line in lines:
        stripped = line.strip().upper()
        if stripped.startswith(("SELECT", "WITH", "EXPLAIN")):
            in_sql = True
        if in_sql:
            sql_lines.append(line)

    if sql_lines:
        return "\n".join(sql_lines).strip().rstrip(";")

    # Last resort
    return response.strip().rstrip(";")
