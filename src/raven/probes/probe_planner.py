"""
Probe Planner — Stage 4.1
===========================
Decomposes a complex user question into 2-5 probe sub-questions.
Each sub-question targets one specific aspect of the data that the
generators will need evidence for (e.g., "What date range exists?",
"What distinct statuses are in the table?").

PExA-inspired: probes run *before* generation so the LLM has
concrete, verified evidence instead of guessing.
"""

from __future__ import annotations

import logging
from pathlib import Path

from ..connectors.openai_client import OpenAIClient

logger = logging.getLogger(__name__)

PROMPT_PATH = Path(__file__).resolve().parents[3] / "prompts" / "probe_decompose.txt"
MAX_PROBES = 5


class ProbePlanner:
    """Decompose a complex question into probe sub-questions with SQL."""

    def __init__(self, openai: OpenAIClient):
        self.openai = openai
        self._prompt_template = PROMPT_PATH.read_text()

    async def plan(
        self,
        question: str,
        selected_tables: list[str],
        pruned_schema: str = "",
    ) -> list[dict]:
        """
        Generate probe sub-questions with executable SQL.

        Args:
            question: The user's complex question.
            selected_tables: Tables selected in Stage 3.
            pruned_schema: Optional pruned schema for more precise probes.

        Returns:
            [
                {"question": "What date range ...?", "sql": "SELECT MIN(...)..."},
                ...
            ]
        """
        tables_summary = ", ".join(selected_tables)
        prompt = (
            self._prompt_template
            .replace("{user_question}", question)
            .replace("{selected_tables_summary}", tables_summary)
        )

        # Add pruned schema if available
        if pruned_schema:
            prompt = prompt.replace("{pruned_schema}", pruned_schema)
        else:
            prompt = prompt.replace("{pruned_schema}", "(schema not yet selected)")

        response = await self.openai.complete(
            prompt=prompt, stage_name="probe_decompose",
        )

        probes = self._parse_probes(response)
        logger.info(
            "Probe planner: generated %d probes for '%s'",
            len(probes), question[:60],
        )
        return probes

    @staticmethod
    def _parse_probes(response: str) -> list[dict]:
        """
        Parse LLM response into structured probe dicts.

        Expected format:
            PROBE 1: What is the date range in the orders table?
            SQL: SELECT MIN(order_date), MAX(order_date) FROM gold.finance.orders
            EXPECTED: date values

            PROBE 2: ...
        """
        probes: list[dict] = []
        current_question: str | None = None
        current_sql_lines: list[str] = []
        in_sql = False

        for line in response.strip().split("\n"):
            stripped = line.strip()
            upper = stripped.upper()

            if upper.startswith("PROBE"):
                # Save previous probe
                if current_question and current_sql_lines:
                    probes.append({
                        "question": current_question,
                        "sql": "\n".join(current_sql_lines).strip(),
                    })
                # Parse new probe question
                parts = stripped.split(":", 1)
                current_question = parts[1].strip() if len(parts) > 1 else stripped
                current_sql_lines = []
                in_sql = False

            elif upper.startswith("SQL:"):
                in_sql = True
                sql_part = stripped[4:].strip()
                if sql_part:
                    current_sql_lines.append(sql_part)

            elif in_sql:
                if upper.startswith(("EXPECTED:", "PROBE", "---")):
                    in_sql = False
                else:
                    current_sql_lines.append(line.rstrip())

        # Save last probe
        if current_question and current_sql_lines:
            probes.append({
                "question": current_question,
                "sql": "\n".join(current_sql_lines).strip(),
            })

        return probes[:MAX_PROBES]
