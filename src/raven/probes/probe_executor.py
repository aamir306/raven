"""
Probe Executor — Stage 4.3
============================
Executes probe SQL on Trino with aggressive timeouts and
converts raw results into safe, structured evidence summaries.

Evidence format is designed to be injected into generation prompts —
it contains counts, distinct values, and ranges, but NEVER raw data
rows (data-policy compliance).
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from ..connectors.trino_connector import TrinoConnector

logger = logging.getLogger(__name__)

PROBE_TIMEOUT_SECONDS = 10.0


class ProbeExecutor:
    """Execute probe queries on Trino and summarize results."""

    def __init__(self, trino: TrinoConnector):
        self.trino = trino

    async def execute_all(self, probes: list[dict]) -> list[dict]:
        """
        Execute all probe queries concurrently with timeout.

        Args:
            probes: [{question, sql}] from ProbePlanner/ProbeGenerator.

        Returns:
            [
                {
                    "question": "What date range...?",
                    "sql": "SELECT MIN(...)...",
                    "result": "1 row: {min_date: 2023-01-01, max_date: 2024-12-31}",
                    "success": True,
                },
                ...
            ]
        """
        results = await asyncio.gather(
            *[self._execute_one(p) for p in probes],
            return_exceptions=True,
        )

        evidence: list[dict] = []
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

        success_count = sum(1 for e in evidence if e["success"])
        logger.info(
            "Probe execution: %d/%d succeeded", success_count, len(evidence),
        )
        return evidence

    async def _execute_one(self, probe: dict) -> str:
        """
        Execute a single probe query with timeout.

        Returns a safe summary string (never raw rows).
        """
        sql = probe["sql"].strip().rstrip(";")

        # Safety: ensure LIMIT
        if "LIMIT" not in sql.upper():
            sql += " LIMIT 50"

        try:
            df = await asyncio.wait_for(
                asyncio.to_thread(self.trino.execute, sql),
                timeout=PROBE_TIMEOUT_SECONDS,
            )

            if df is None or df.empty:
                return "Empty result"

            return self._summarize(df)

        except asyncio.TimeoutError:
            return f"Probe timed out (>{PROBE_TIMEOUT_SECONDS}s)"
        except Exception as e:
            return f"Execution error: {e}"

    @staticmethod
    def _summarize(df: Any) -> str:
        """
        Summarize a DataFrame into a safe evidence string.

        Rules (data policy):
          - Single scalar → return the value
          - Aggregations (≤3 cols, 1 row) → return the dict
          - Otherwise → column names + counts + distinct-value summary
        """
        row_count = len(df)

        # Single-value result
        if row_count == 1 and len(df.columns) <= 3:
            return f"{row_count} row: {df.to_dict('records')[0]}"

        # Multi-row summary
        summaries: list[str] = []
        for col in df.columns:
            distinct = df[col].nunique()
            if distinct <= 20:
                vals = df[col].dropna().unique().tolist()[:20]
                summaries.append(f"{col}: {distinct} distinct values: {vals}")
            else:
                # Summarize numerics with min/max
                if hasattr(df[col], "min") and df[col].dtype.kind in ("i", "f"):
                    summaries.append(
                        f"{col}: {distinct} distinct, "
                        f"range [{df[col].min()} .. {df[col].max()}]"
                    )
                else:
                    summaries.append(f"{col}: {distinct} distinct values")

        return f"{row_count} rows. " + "; ".join(summaries)
