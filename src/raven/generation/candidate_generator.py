"""
Candidate Generator — Stage 5 Orchestrator
============================================
Coordinates three CHASE-SQL generation strategies:
  5.1  DivideAndConquer    – Decompose → CTEs
  5.2  ExecutionPlanCoT    – Scan → filter → join → agg
  5.3  FewShotGenerator    – Adapt past Q-SQL pairs

Plus:
  5.4  TrinoDialect        – Dialect rules + error classification
  5.5  RevisionLoop        – EXPLAIN-validate + taxonomy-guided revision

SIMPLE queries: 1 candidate (DC via mini).
COMPLEX queries: 3 diverse candidates in parallel (DC, EP, FS via GPT-4o).
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from ..connectors.openai_client import OpenAIClient
from ..connectors.trino_connector import TrinoConnector
from .divide_and_conquer import DivideAndConquerGenerator
from .execution_plan_cot import ExecutionPlanCoTGenerator
from .fewshot_generator import FewShotGenerator
from .trino_dialect import TrinoDialect
from .revision_loop import RevisionLoop

logger = logging.getLogger(__name__)


class CandidateGenerator:
    """Stage 5 orchestrator — multi-strategy SQL generation + revision."""

    def __init__(self, openai: OpenAIClient, trino: TrinoConnector):
        self.openai = openai
        self.trino = trino

        # Generation strategies
        self.dc = DivideAndConquerGenerator(openai)
        self.ep = ExecutionPlanCoTGenerator(openai)
        self.fs = FewShotGenerator(openai)

        # Shared utilities
        self.dialect = TrinoDialect()
        self.revision = RevisionLoop(openai, trino, self.dialect)

    async def generate(
        self,
        question: str,
        difficulty: Any,
        pruned_schema: str,
        probe_evidence: list[dict],
        glossary_matches: list[dict],
        similar_queries: list[dict],
        resolved_values: list[dict] | None = None,
        instruction_matches: list[dict] | None = None,
        query_plan: dict | None = None,
    ) -> list[str]:
        """
        Generate SQL candidates.

        SIMPLE: 1 candidate (DC via GPT-4o-mini).
        COMPLEX: 3 candidates in parallel (DC, EP, FS via GPT-4o).

        All candidates pass through the revision loop.

        Returns:
            List of validated SQL strings.
        """
        context = self._build_context(
            pruned_schema,
            probe_evidence,
            glossary_matches,
            similar_queries,
            resolved_values or [],
            instruction_matches or [],
            query_plan,
        )

        is_simple = hasattr(difficulty, "value") and difficulty.value == "SIMPLE"

        if is_simple:
            sql = await self.dc.generate(
                question, context, stage_name="gen_simple",
            )
            raw_candidates = [sql] if sql else []
        else:
            # 3 diverse candidates — parallel
            tasks = [
                self.dc.generate(question, context, "gen_candidate_a"),
                self.ep.generate(question, context, "gen_candidate_b"),
                self.fs.generate(question, context, "gen_candidate_c"),
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            raw_candidates = [
                r for r in results if isinstance(r, str) and r.strip()
            ]

        # Validate + revise all candidates
        validated = await self.revision.validate_batch(
            raw_candidates, question, pruned_schema,
        )

        if not validated:
            logger.error("All candidates failed for: %s", question[:60])
            if raw_candidates:
                validated = [raw_candidates[0]]

        return validated

    # ── Context Builder ────────────────────────────────────────────────

    def _build_context(
        self,
        pruned_schema: str,
        probe_evidence: list[dict],
        glossary_matches: list[dict],
        similar_queries: list[dict],
        resolved_values: list[dict],
        instruction_matches: list[dict],
        query_plan: dict | None,
    ) -> dict:
        """Assemble the shared context dict used by all generators."""
        # Probe evidence
        evidence_lines: list[str] = []
        for p in probe_evidence:
            if p.get("success"):
                evidence_lines.append(f"Q: {p['question']}\nA: {p['result']}")
        probe_str = (
            "\n\n".join(evidence_lines)
            if evidence_lines
            else "No probe evidence available."
        )

        # Glossary definitions
        glossary_lines: list[str] = []
        for g in glossary_matches:
            line = f"- {g['term']}: {g['definition']}"
            if g.get("sql_fragment"):
                line += f"\n  SQL: {g['sql_fragment']}"
            glossary_lines.append(line)
        glossary_str = "\n".join(glossary_lines) or "No glossary matches."

        # Few-shot examples
        fewshot_lines: list[str] = []
        for q in similar_queries[:3]:
            lines = [
                f"Q: {q.get('question', 'N/A')}",
                f"SQL: {q.get('sql', 'N/A')}",
            ]
            if q.get("notes"):
                lines.append(f"Notes: {q['notes']}")
            if q.get("source"):
                lines.append(f"Source: {q['source']}")
            fewshot_lines.append("\n".join(lines))
        fewshot_str = "\n\n".join(fewshot_lines) or "No similar queries available."

        grounding_lines: list[str] = []
        for item in resolved_values[:8]:
            sql = item.get("sql", "")
            if sql:
                grounding_lines.append(
                    f"- {item.get('matched_text', item.get('column', 'value'))}: {sql}"
                )
        grounding_str = "\n".join(grounding_lines) or "No grounded values."

        instruction_lines: list[str] = []
        for item in instruction_matches[:8]:
            line = f"- {item.get('term', 'rule')}: {item.get('definition', '')}"
            if item.get("sql_fragment"):
                line += f"\n  SQL hint: {item['sql_fragment']}"
            instruction_lines.append(line)
        instruction_str = "\n".join(instruction_lines) or "No instruction matches."

        plan_str = "No deterministic plan available."
        if query_plan:
            plan_str = (
                f"Intent: {query_plan.get('intent', 'unknown')}\n"
                f"Path: {query_plan.get('path_type', 'unknown')}\n"
                f"Planned SQL template:\n{query_plan.get('compiled_sql', '')}"
            )

        return {
            "pruned_schema": pruned_schema or "No schema available.",
            "probe_evidence": probe_str,
            "glossary_defs": glossary_str,
            "few_shot": fewshot_str,
            "grounded_values": grounding_str,
            "instructions": instruction_str,
            "query_plan": plan_str,
            "dialect_rules": self.dialect.rules_text,
            "similar_queries": similar_queries,
        }
