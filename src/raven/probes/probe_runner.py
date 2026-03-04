"""
Stage 4: Probe Runner (PExA-inspired)
======================================
Decomposes complex questions into sub-questions, generates probe SQL,
executes probes on Trino, and returns evidence for generators.

Only runs for COMPLEX queries.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from ..connectors.openai_client import OpenAIClient
from ..connectors.trino_connector import TrinoConnector

logger = logging.getLogger(__name__)

PROMPTS_DIR = Path(__file__).resolve().parents[3] / "prompts"


class ProbeRunner:
    """Execute test probes before SQL generation."""

    def __init__(self, openai: OpenAIClient, trino: TrinoConnector):
        self.openai = openai
        self.trino = trino
        self._decompose_prompt = (PROMPTS_DIR / "probe_decompose.txt").read_text()
        self._generate_prompt = (PROMPTS_DIR / "probe_generate.txt").read_text()

    async def run_probes(
        self,
        question: str,
        pruned_schema: str,
        selected_tables: list[str],
    ) -> list[dict]:
        """
        Decompose → Generate probe SQL → Execute → Return evidence.

        Returns list of dicts: [{question, sql, result, success}]
        """
        # Step 1: Decompose question into sub-questions
        probes = await self._decompose(question, selected_tables)

        if not probes:
            logger.warning("No probes generated for question: %s", question[:60])
            return []

        # Step 2: Execute probes concurrently (with timeout)
        results = await asyncio.gather(
            *[self._execute_probe(p) for p in probes],
            return_exceptions=True,
        )

        evidence = []
        for probe, result in zip(probes, results):
            if isinstance(result, Exception):
                evidence.append({
                    "question": probe["question"],
                    "sql": probe["sql"],
                    "result": f"Error: {result}",
                    "success": False,
                })
            else:
                evidence.append({
                    "question": probe["question"],
                    "sql": probe["sql"],
                    "result": result,
                    "success": True,
                })

        logger.info(
            "Probes: %d/%d succeeded for '%s'",
            sum(1 for e in evidence if e["success"]),
            len(evidence),
            question[:60],
        )
        return evidence

    async def _decompose(self, question: str, selected_tables: list[str]) -> list[dict]:
        """Use LLM to decompose question into probe sub-questions with SQL."""
        tables_summary = ", ".join(selected_tables)
        prompt = (
            self._decompose_prompt
            .replace("{user_question}", question)
            .replace("{selected_tables_summary}", tables_summary)
        )

        response = await self.openai.complete(prompt=prompt, stage_name="probe_decompose")

        # Parse PROBE N: question\nSQL: query format
        probes = []
        current_question = None
        current_sql_lines = []
        in_sql = False

        for line in response.strip().split("\n"):
            stripped = line.strip()
            if stripped.upper().startswith("PROBE"):
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
            elif stripped.upper().startswith("SQL:"):
                in_sql = True
                sql_part = stripped[4:].strip()
                if sql_part:
                    current_sql_lines.append(sql_part)
            elif in_sql:
                if stripped.upper().startswith("EXPECTED:") or stripped.upper().startswith("PROBE"):
                    in_sql = False
                else:
                    current_sql_lines.append(line.rstrip())

        # Save last probe
        if current_question and current_sql_lines:
            probes.append({
                "question": current_question,
                "sql": "\n".join(current_sql_lines).strip(),
            })

        return probes[:5]  # Max 5 probes

    async def _execute_probe(self, probe: dict) -> str:
        """Execute a single probe query on Trino with timeout."""
        sql = probe["sql"].strip().rstrip(";")

        # Ensure LIMIT exists
        if "LIMIT" not in sql.upper():
            sql += " LIMIT 50"

        try:
            df = await asyncio.wait_for(
                asyncio.to_thread(self.trino.execute, sql),
                timeout=10.0,
            )
            if df is None or df.empty:
                return "Empty result"

            # Summarize results (never send raw data to API)
            row_count = len(df)
            if row_count == 1 and len(df.columns) <= 3:
                # Single value — safe to summarize
                return f"{row_count} row: {df.to_dict('records')[0]}"
            else:
                # Summarize: column names + count + distinct values
                summaries = []
                for col in df.columns:
                    distinct = df[col].nunique()
                    if distinct <= 20:
                        vals = df[col].dropna().unique().tolist()[:20]
                        summaries.append(f"{col}: {distinct} distinct values: {vals}")
                    else:
                        summaries.append(f"{col}: {distinct} distinct values")
                return f"{row_count} rows. " + "; ".join(summaries)

        except asyncio.TimeoutError:
            return "Probe timed out (>10s)"
        except Exception as e:
            return f"Execution error: {e}"
