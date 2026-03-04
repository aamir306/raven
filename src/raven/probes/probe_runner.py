"""
Stage 4: Probe Runner — Orchestrator (PExA-inspired)
=====================================================
Coordinates three sub-modules:
  4.1  ProbePlanner   – Decompose question into sub-questions + SQL
  4.2  ProbeGenerator – Refine/validate probe SQL
  4.3  ProbeExecutor  – Execute on Trino with timeouts → evidence

Only runs for COMPLEX queries.
"""

from __future__ import annotations

import logging

from ..connectors.openai_client import OpenAIClient
from ..connectors.trino_connector import TrinoConnector
from .probe_planner import ProbePlanner
from .probe_generator import ProbeGenerator
from .probe_executor import ProbeExecutor

logger = logging.getLogger(__name__)


class ProbeRunner:
    """Stage 4 orchestrator — decompose → generate → execute → evidence."""

    def __init__(self, openai: OpenAIClient, trino: TrinoConnector):
        self.openai = openai
        self.trino = trino

        # Sub-modules
        self.planner = ProbePlanner(openai)
        self.generator = ProbeGenerator(openai)
        self.executor = ProbeExecutor(trino)

    async def run_probes(
        self,
        question: str,
        pruned_schema: str,
        selected_tables: list[str],
    ) -> list[dict]:
        """
        Decompose → Refine → Execute → Return evidence.

        Returns list of dicts: [{question, sql, result, success}]
        """
        # Step 1: Decompose question into sub-questions with SQL
        probes = await self.planner.plan(
            question, selected_tables, pruned_schema,
        )

        if not probes:
            logger.warning("No probes generated for question: %s", question[:60])
            return []

        # Step 2: Refine probe SQL (ensure LIMIT, strip DML, etc.)
        refined_probes = await self.generator.refine(probes, pruned_schema)

        # Step 3: Execute all probes concurrently with timeout
        evidence = await self.executor.execute_all(refined_probes)

        logger.info(
            "Probes: %d/%d succeeded for '%s'",
            sum(1 for e in evidence if e["success"]),
            len(evidence),
            question[:60],
        )
        return evidence
