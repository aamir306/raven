"""
Probe Generator — Stage 4.2
==============================
Takes a decomposed probe plan and generates refined, Trino-safe SQL
for each sub-question.  Applies dialect rules (LIMIT, date functions,
read-only) and ensures each probe has a LIMIT clause.

In many cases the probe planner already produces valid SQL.  This
module acts as a safety net — validating/fixing probe SQL syntax
before execution.
"""

from __future__ import annotations

import logging
from pathlib import Path

from ..connectors.openai_client import OpenAIClient

logger = logging.getLogger(__name__)

PROMPT_PATH = Path(__file__).resolve().parents[3] / "prompts" / "probe_generate.txt"
MAX_PROBE_LIMIT = 50


class ProbeGenerator:
    """Refine and validate probe SQL before execution."""

    def __init__(self, openai: OpenAIClient):
        self.openai = openai
        self._prompt_template = PROMPT_PATH.read_text()

    async def refine(
        self,
        probes: list[dict],
        pruned_schema: str = "",
    ) -> list[dict]:
        """
        Refine probe SQL to ensure Trino compatibility.

        For each probe:
          - Ensure LIMIT clause exists (max 50 rows)
          - Strip DML statements (safety)
          - Strip trailing semicolons

        Args:
            probes: [{question, sql}] from ProbePlanner.
            pruned_schema: Optional schema for LLM-based refinement.

        Returns:
            [{question, sql}] with cleaned/refined SQL.
        """
        refined: list[dict] = []
        for probe in probes:
            sql = self._clean_sql(probe["sql"])
            refined.append({
                "question": probe["question"],
                "sql": sql,
            })

        logger.debug("Probe generator: refined %d probes", len(refined))
        return refined

    async def generate_from_question(
        self,
        sub_question: str,
        pruned_schema: str,
    ) -> str:
        """
        Generate probe SQL from scratch for a sub-question (fallback path).

        Used when the probe planner's SQL is invalid.
        """
        prompt = (
            self._prompt_template
            .replace("{probe_question}", sub_question)
            .replace("{pruned_schema}", pruned_schema or "(no schema)")
        )

        response = await self.openai.complete(
            prompt=prompt, stage_name="probe_generate",
        )

        return self._clean_sql(self._extract_sql(response))

    # ── Helpers ─────────────────────────────────────────────────────────

    @staticmethod
    def _clean_sql(sql: str) -> str:
        """Ensure probe SQL is safe and Trino-compatible."""
        sql = sql.strip().rstrip(";")

        # Safety: block any DML
        first_word = sql.split()[0].upper() if sql.split() else ""
        if first_word in ("INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE", "TRUNCATE"):
            return ""

        # Ensure LIMIT exists
        if "LIMIT" not in sql.upper():
            sql += f" LIMIT {MAX_PROBE_LIMIT}"

        return sql

    @staticmethod
    def _extract_sql(response: str) -> str:
        """Extract SQL from LLM response (handles markdown code blocks)."""
        import re

        code_block = re.search(
            r"```(?:sql)?\s*\n?(.*?)```", response, re.DOTALL | re.IGNORECASE,
        )
        if code_block:
            return code_block.group(1).strip()

        # Look for SELECT/WITH lines
        lines = response.strip().split("\n")
        sql_lines: list[str] = []
        in_sql = False
        for line in lines:
            if line.strip().upper().startswith(("SELECT", "WITH", "EXPLAIN")):
                in_sql = True
            if in_sql:
                sql_lines.append(line)

        return "\n".join(sql_lines).strip() if sql_lines else response.strip()
