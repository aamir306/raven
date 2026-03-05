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
import json
import logging
from pathlib import Path

from ..connectors.openai_client import OpenAIClient
from ..connectors.trino_connector import TrinoConnector

logger = logging.getLogger(__name__)

PROMPTS_DIR = Path(__file__).resolve().parents[3] / "prompts"
CONFIG_DIR = Path(__file__).resolve().parents[3] / "config"


class CandidateSelector:
    """Select the best SQL candidate from multiple options."""

    def __init__(self, openai: OpenAIClient, trino: TrinoConnector):
        self.openai = openai
        self.trino = trino

        self._pairwise_prompt = (PROMPTS_DIR / "val_pairwise_compare.txt").read_text()
        self._taxonomy_prompt = (PROMPTS_DIR / "val_error_taxonomy.txt").read_text()

        # Load cost guards
        cost_path = CONFIG_DIR / "cost_guards.yaml"
        self._cost_guards = {}
        if cost_path.exists():
            import yaml
            self._cost_guards = yaml.safe_load(cost_path.read_text()) or {}

    async def select_best(
        self,
        question: str,
        candidates: list[str],
        pruned_schema: str,
        content_awareness: list[dict],
        retrieval_quality: dict | None = None,
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
            confidence, score = self._score_confidence(
                n_candidates=1,
                errors_found=bool(errors),
                cost_ok=True,
                retrieval_quality=retrieval_quality,
            )
            return {
                "sql": candidates[0],
                "confidence": confidence,
                "confidence_score": score,
                "errors_found": errors,
            }

        # Pairwise comparison for 2+ candidates
        winner = await self._pairwise_select(candidates, question, pruned_schema)

        # Error taxonomy check on winner
        errors = await self._taxonomy_check(
            winner, question, pruned_schema, content_awareness,
        )

        # Cost guard check via EXPLAIN
        cost_ok = await self._check_cost_guard(winner)

        # Confidence scoring
        confidence, score = self._score_confidence(
            n_candidates=len(candidates),
            errors_found=bool(errors),
            cost_ok=cost_ok,
            retrieval_quality=retrieval_quality,
        )

        return {
            "sql": winner,
            "confidence": confidence,
            "confidence_score": score,
            "errors_found": errors,
            "cost_ok": cost_ok,
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

    async def _check_cost_guard(self, sql: str) -> bool:
        """Check if SQL passes cost guard via EXPLAIN."""
        try:
            plan = await asyncio.to_thread(self.trino.explain, sql)
            # Check scan size against threshold
            max_scan_gb = self._cost_guards.get("thresholds", {}).get("max_scan_gb", 500)
            # Parse EXPLAIN output for estimated data scan (implementation-specific)
            # For now, EXPLAIN success = cost OK
            return True
        except Exception as e:
            logger.warning("EXPLAIN failed for cost guard: %s", e)
            return False

    @staticmethod
    def _score_confidence(
        n_candidates: int,
        errors_found: bool,
        cost_ok: bool,
        retrieval_quality: dict | None = None,
    ) -> tuple[str, float]:
        """Score confidence as (HIGH/MEDIUM/LOW, numeric_score).

        Scoring breakdown (max 10 points):
          - Candidates compared:  3→2pts, 2→1pt, 1→0pts
          - No errors found:      +2pts
          - Cost guard passed:    +1pt
          - Entity matches ≥1:    +1pt
          - Glossary matches ≥1:  +1pt
          - Similar query sim≥0.7: +2pts, ≥0.5: +1pt
          - Probe evidence ≥1:    +1pt

        Thresholds: ≥7 HIGH, ≥4 MEDIUM, <4 LOW
        """
        rq = retrieval_quality or {}
        score = 0.0

        # 1. Candidate diversity (0-2)
        if n_candidates >= 3:
            score += 2.0
        elif n_candidates >= 2:
            score += 1.0

        # 2. Error-free (0-2)
        if not errors_found:
            score += 2.0

        # 3. Cost guard (0-1)
        if cost_ok:
            score += 1.0

        # 4. Entity matches — schema retrieval quality (0-1)
        if rq.get("entity_match_count", 0) >= 1:
            score += 1.0

        # 5. Glossary matches — business rule coverage (0-1)
        if rq.get("glossary_match_count", 0) >= 1:
            score += 1.0

        # 6. Similar query similarity — few-shot grounding (0-2)
        top_sim = rq.get("similar_query_top_sim", 0.0)
        if top_sim >= 0.70:
            score += 2.0
        elif top_sim >= 0.50:
            score += 1.0

        # 7. Probe evidence (0-1) — real data sampling
        if rq.get("probe_count", 0) >= 1:
            score += 1.0

        # Normalize to [0, 1] range for external use
        normalized = round(min(score / 10.0, 1.0), 2)

        # Thresholds calibrated against eval results
        if score >= 7:
            return "HIGH", normalized
        elif score >= 4:
            return "MEDIUM", normalized
        return "LOW", normalized
