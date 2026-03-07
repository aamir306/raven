"""
Stage 6: Candidate Selector + Validator
========================================
- Pairwise comparison (CHASE-SQL): A vs B, A vs C, B vs C → winner
- Error taxonomy check (SQL-of-Thought): 10 categories, 36 sub-types
- Trino EXPLAIN cost guard
- Partition pruning validation
- Confidence scoring (HIGH / MEDIUM / LOW)

Only runs for COMPLEX queries with multiple candidates.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from ..connectors.openai_client import OpenAIClient
from ..connectors.trino_connector import TrinoConnector
from .confidence_model import ConfidenceModel, ConfidenceSignals
from .cost_guard import CostGuard
from .query_plan_validator import QueryPlanValidator

logger = logging.getLogger(__name__)

PROMPTS_DIR = Path(__file__).resolve().parents[3] / "prompts"
CONFIG_DIR = Path(__file__).resolve().parents[3] / "config"


class CandidateSelector:
    """Select the best SQL candidate from multiple options."""

    _HARD_PLAN_PREFIXES = (
        "missing_table:",
        "missing_join:",
        "missing_group_by",
        "missing_group_column:",
        "missing_time_group_by",
        "missing_time_bucket:",
        "missing_filter:",
        "missing_metric_expression:",
        "missing_limit:",
        "wrong_limit:",
        "missing_order:",
        "missing_time_order",
    )

    _SOFT_PLAN_PREFIXES = (
        "missing_metric_alias:",
        "missing_time_bucket_alias:",
    )

    def __init__(self, openai: OpenAIClient, trino: TrinoConnector):
        self.openai = openai
        self.trino = trino

        self._pairwise_prompt = (PROMPTS_DIR / "val_pairwise_compare.txt").read_text()
        self._taxonomy_prompt = (PROMPTS_DIR / "val_error_taxonomy.txt").read_text()

        self._cost_guard = CostGuard(trino)
        self._confidence_model = ConfidenceModel()
        self._plan_validator = QueryPlanValidator()

    async def select_best(
        self,
        question: str,
        candidates: list[str],
        pruned_schema: str,
        content_awareness: list[dict],
        retrieval_quality: dict | None = None,
        query_plan: dict | None = None,
    ) -> dict:
        """
        Select best SQL from candidates via pairwise comparison + validation.

        Args:
            retrieval_quality: Optional dict with retrieval context signals:
                - entity_match_count: number of entity matches found
                - glossary_match_count: number of glossary matches
                - similar_query_top_sim: similarity of best matching question
                - table_count: number of selected tables
                - probe_count: number of probe results
                - has_few_shot: whether few-shot examples were available

        Returns dict: {sql, confidence, confidence_score, errors_found, explanation}
        """
        retrieval_quality = retrieval_quality or {}

        if len(candidates) == 1:
            # Single candidate — just validate
            errors = await self._taxonomy_check(
                candidates[0], question, pruned_schema, content_awareness,
            )
            plan_check = self._plan_validator.validate(candidates[0], query_plan)
            hard_violations, soft_violations = self._split_plan_violations(plan_check.violations)
            if hard_violations:
                return {
                    "sql": "",
                    "confidence": "LOW",
                    "confidence_score": 0.0,
                    "errors_found": errors,
                    "plan_violations": plan_check.violations,
                    "plan_hard_violations": hard_violations,
                    "plan_soft_violations": soft_violations,
                    "rejected": True,
                    "rejection_reasons": hard_violations,
                }
            confidence, score = self._score_confidence(
                n_candidates=1,
                errors_found=bool(errors),
                cost_ok=True,
                plan_consistent=plan_check.ok,
                hard_plan_violations=len(hard_violations),
                soft_plan_violations=len(soft_violations),
                retrieval_quality=retrieval_quality,
            )
            return {
                "sql": candidates[0],
                "confidence": confidence,
                "confidence_score": score,
                "errors_found": errors,
                "plan_violations": plan_check.violations,
                "plan_hard_violations": hard_violations,
                "plan_soft_violations": soft_violations,
            }

        candidate_pool = list(candidates)
        if query_plan:
            plan_results = [
                self._candidate_plan_summary(candidate, query_plan)
                for candidate in candidates
            ]
            hard_free = [
                item
                for item in plan_results
                if not item["hard_violations"]
            ]
            if hard_free:
                candidate_pool = [
                    item["sql"]
                    for item in sorted(
                        hard_free,
                        key=lambda item: (
                            len(item["soft_violations"]),
                            len(item["plan_violations"]),
                        ),
                    )[: max(1, min(2, len(hard_free)))]
                ]
            else:
                best = min(
                    plan_results,
                    key=lambda item: (
                        len(item["hard_violations"]),
                        len(item["soft_violations"]),
                        len(item["plan_violations"]),
                    ),
                )
                return {
                    "sql": "",
                    "confidence": "LOW",
                    "confidence_score": 0.0,
                    "errors_found": [],
                    "cost_ok": False,
                    "plan_violations": best["plan_violations"],
                    "plan_hard_violations": best["hard_violations"],
                    "plan_soft_violations": best["soft_violations"],
                    "rejected": True,
                    "rejection_reasons": best["hard_violations"],
                }

        # Pairwise comparison for 2+ candidates
        winner = await self._pairwise_select(candidate_pool, question, pruned_schema)

        # Error taxonomy check on winner
        errors = await self._taxonomy_check(
            winner, question, pruned_schema, content_awareness,
        )

        # Cost guard check via EXPLAIN (returns rich dict)
        cost_result = await self._check_cost_guard(winner)
        cost_ok = cost_result.get("passed", True)
        plan_check = self._plan_validator.validate(winner, query_plan)
        hard_violations, soft_violations = self._split_plan_violations(plan_check.violations)
        if hard_violations:
            return {
                "sql": "",
                "confidence": "LOW",
                "confidence_score": 0.0,
                "errors_found": errors,
                "cost_ok": cost_ok,
                "cost_guard": cost_result,
                "plan_violations": plan_check.violations,
                "plan_hard_violations": hard_violations,
                "plan_soft_violations": soft_violations,
                "rejected": True,
                "rejection_reasons": hard_violations,
            }

        # Confidence scoring
        confidence, score = self._score_confidence(
            n_candidates=len(candidate_pool),
            errors_found=bool(errors),
            cost_ok=cost_ok,
            plan_consistent=plan_check.ok,
            hard_plan_violations=len(hard_violations),
            soft_plan_violations=len(soft_violations),
            retrieval_quality=retrieval_quality,
        )

        return {
            "sql": winner,
            "confidence": confidence,
            "confidence_score": score,
            "errors_found": errors,
            "cost_ok": cost_ok,
            "cost_guard": cost_result,
            "plan_violations": plan_check.violations,
            "plan_hard_violations": hard_violations,
            "plan_soft_violations": soft_violations,
        }

    async def _pairwise_select(
        self,
        candidates: list[str],
        question: str,
        pruned_schema: str,
    ) -> str:
        """Run pairwise comparisons and return the winner."""
        if len(candidates) < 2:
            return candidates[0]

        # Run all pairwise comparisons
        scores = {i: 0 for i in range(len(candidates))}
        pairs = []
        for i in range(len(candidates)):
            for j in range(i + 1, len(candidates)):
                pairs.append((i, j))

        async def compare(i: int, j: int) -> tuple[int, int, str]:
            prompt = (
                self._pairwise_prompt
                .replace("{user_question}", question)
                .replace("{pruned_schema}", pruned_schema)
                .replace("{sql_a}", candidates[i])
                .replace("{sql_b}", candidates[j])
            )
            response = await self.openai.complete(prompt=prompt, stage_name="val_pairwise")
            return i, j, response

        results = await asyncio.gather(*[compare(i, j) for i, j in pairs])

        for i, j, response in results:
            upper = response.upper()
            if "WINNER: A" in upper:
                scores[i] += 1
            elif "WINNER: B" in upper:
                scores[j] += 1
            else:
                # Tie or parse failure — give both half credit
                scores[i] += 0.5
                scores[j] += 0.5

        # Winner is the one with highest score
        winner_idx = max(scores, key=scores.get)
        logger.info("Pairwise selection: candidate %d won (scores: %s)", winner_idx, scores)
        return candidates[winner_idx]

    async def _taxonomy_check(
        self,
        sql: str,
        question: str,
        pruned_schema: str,
        content_awareness: list[dict],
    ) -> list[dict]:
        """Check SQL against error taxonomy."""
        awareness_str = "\n".join(
            f"- {a['table']}.{a['column']}: {a.get('data_type', '')} | "
            f"null: {a.get('null_pct', '')}%"
            for a in content_awareness
        ) or "None"

        prompt = (
            self._taxonomy_prompt
            .replace("{user_question}", question)
            .replace("{pruned_schema}", pruned_schema)
            .replace("{content_awareness}", awareness_str)
            .replace("{sql}", sql)
        )

        response = await self.openai.complete(prompt=prompt, stage_name="val_taxonomy")

        # Parse errors
        errors = []
        if "ERRORS_FOUND: true" in response.lower() or "errors_found: true" in response.lower():
            for line in response.split("\n"):
                line = line.strip()
                if line.upper().startswith("ERROR") and ":" in line:
                    parts = line.split("—")
                    errors.append({
                        "category": parts[0].strip() if parts else "",
                        "description": parts[1].strip() if len(parts) > 1 else "",
                        "fix": parts[2].strip() if len(parts) > 2 else "",
                    })

        return errors

    async def _check_cost_guard(self, sql: str) -> dict:
        """Check if SQL passes cost guard via EXPLAIN. Returns full CostGuard result dict."""
        try:
            return await self._cost_guard.check(sql)
        except Exception as e:
            logger.warning("Cost guard check failed: %s", e)
            return {"passed": True, "explain_ok": False, "estimated_scan_gb": 0.0, "reason": str(e)}

    @staticmethod
    def _split_plan_violations(violations: list[str]) -> tuple[list[str], list[str]]:
        hard: list[str] = []
        soft: list[str] = []
        for violation in violations:
            if violation.startswith(CandidateSelector._HARD_PLAN_PREFIXES):
                hard.append(violation)
            elif violation.startswith(CandidateSelector._SOFT_PLAN_PREFIXES):
                soft.append(violation)
            else:
                soft.append(violation)
        return hard, soft

    def _candidate_plan_summary(
        self,
        candidate: str,
        query_plan: dict | None,
    ) -> dict:
        plan_check = self._plan_validator.validate(candidate, query_plan)
        hard_violations, soft_violations = self._split_plan_violations(plan_check.violations)
        return {
            "sql": candidate,
            "plan_violations": plan_check.violations,
            "hard_violations": hard_violations,
            "soft_violations": soft_violations,
            "ok": plan_check.ok,
        }

    @staticmethod
    def _score_confidence(
        n_candidates: int,
        errors_found: bool,
        cost_ok: bool,
        plan_consistent: bool,
        hard_plan_violations: int,
        soft_plan_violations: int,
        retrieval_quality: dict | None = None,
    ) -> tuple[str, float]:
        """Score confidence via ConfidenceModel (backward-compatible wrapper).

        Returns (band, normalised_score).
        """
        model = ConfidenceModel()
        return model.score_from_selector(
            n_candidates=n_candidates,
            errors_found=errors_found,
            cost_ok=cost_ok,
            plan_consistent=plan_consistent,
            hard_plan_violations=hard_plan_violations,
            soft_plan_violations=soft_plan_violations,
            retrieval_quality=retrieval_quality,
        )
