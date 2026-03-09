"""
Calibrated Confidence Model
============================
Combines five evidence dimensions into a calibrated confidence score:

  1. **Plan consistency** — query plan validator violations (hard vs soft)
  2. **Cost guard** — EXPLAIN-based scan-size / row-count check
  3. **Execution sanity** — ExecutionJudge post-execution checks
  4. **Retrieval evidence** — entity matches, glossary, similar queries, probes
  5. **Ambiguity** — grounding ambiguities, multiple-match signals

The model produces:
  - ``score``  — float in [0, 1]
  - ``band``   — HIGH / MEDIUM / LOW / ABSTAIN
  - ``detail`` — per-dimension breakdown dict

ABSTAIN band (score < abstain_threshold) signals the pipeline should
decline to answer rather than return likely-wrong SQL.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


# ── Thresholds (can be overridden via config) ──────────────────────────
# Calibrated 2026-03-07 against 20 business-critical production queries.
# Brier=0.2125, ECE=0.05 pre-calibration.

DEFAULT_THRESHOLDS = {
    "high": 0.65,       # was 0.72 — unreachable with typical signal availability
    "medium": 0.42,     # was 0.45 — tightened to separate success/failure clusters
    "abstain": 0.18,    # was 0.22 — lowered to reduce false abstains
}

# Maximum raw score from all dimensions.
# Set to realistic production ceiling (entity/similar-query signals are
# frequently absent; theoretical max is ~14 but production max observed = 9.5).
_MAX_RAW = 12.0


@dataclass(frozen=True)
class ConfidenceSignals:
    """All input signals consumed by the model."""

    # Plan consistency
    hard_plan_violations: int = 0
    soft_plan_violations: int = 0
    plan_consistent: bool = False  # True = no violations at all

    # Cost guard
    cost_guard_passed: bool = True
    cost_guard_explain_ok: bool = True
    estimated_scan_gb: float = 0.0

    # Execution sanity (post-execution, may be None pre-execution)
    execution_judge_passed: bool | None = None
    execution_judge_issues: list[str] = field(default_factory=list)
    row_count: int = 0  # actual result rows (empty = 0)

    # Retrieval evidence
    entity_match_count: int = 0
    glossary_match_count: int = 0
    similar_query_top_sim: float = 0.0
    table_count: int = 0
    probe_count: int = 0
    has_few_shot: bool = False
    has_trusted_query: bool = False
    has_query_family: bool = False

    # Ambiguity
    grounding_ambiguity_count: int = 0
    quality_warning_count: int = 0

    # Candidate diversity
    n_candidates: int = 1
    taxonomy_errors_found: bool = False


@dataclass
class ConfidenceResult:
    """Output of the confidence model."""

    score: float = 0.0              # normalised [0, 1]
    band: str = "LOW"               # HIGH / MEDIUM / LOW / ABSTAIN
    should_abstain: bool = False
    raw_score: float = 0.0          # un-normalised
    detail: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class ConfidenceModel:
    """Calibrated multi-signal confidence scorer."""

    def __init__(self, thresholds: dict[str, float] | None = None):
        t = {**DEFAULT_THRESHOLDS, **(thresholds or {})}
        self._high = float(t["high"])
        self._medium = float(t["medium"])
        self._abstain = float(t["abstain"])

    # ── Public API ──────────────────────────────────────────────────────

    def score(self, signals: ConfidenceSignals) -> ConfidenceResult:
        """Compute calibrated confidence from all available signals."""
        detail: dict[str, Any] = {}

        # ── Dimension 1: Plan consistency (0–3) ────────────────────────
        plan_pts = self._plan_score(signals)
        detail["plan"] = plan_pts

        # ── Dimension 2: Cost guard (0–1.5) ────────────────────────────
        cost_pts = self._cost_score(signals)
        detail["cost"] = cost_pts

        # ── Dimension 3: Execution sanity (0–2) ───────────────────────
        exec_pts = self._execution_score(signals)
        detail["execution"] = exec_pts

        # ── Dimension 4: Retrieval evidence (0–6) ─────────────────────
        retrieval_pts = self._retrieval_score(signals)
        detail["retrieval"] = retrieval_pts

        # ── Dimension 5: Ambiguity penalty (0 to –2) ──────────────────
        ambiguity_pts = self._ambiguity_penalty(signals)
        detail["ambiguity"] = ambiguity_pts

        # ── Dimension 6: Candidate diversity (0–1.5) ──────────────────
        diversity_pts = self._diversity_score(signals)
        detail["diversity"] = diversity_pts

        # ── Dimension 7: Taxonomy errors (0 to –2) ────────────────────
        taxonomy_pts = -2.0 if signals.taxonomy_errors_found else 0.0
        detail["taxonomy"] = taxonomy_pts

        raw = plan_pts + cost_pts + exec_pts + retrieval_pts + ambiguity_pts + diversity_pts + taxonomy_pts
        raw = max(raw, 0.0)
        normalised = round(min(raw / _MAX_RAW, 1.0), 3)

        # ── Hard vetoes ────────────────────────────────────────────────
        if signals.hard_plan_violations > 0:
            normalised = 0.0

        band = self._band(normalised)
        should_abstain = normalised < self._abstain

        return ConfidenceResult(
            score=normalised,
            band=band,
            should_abstain=should_abstain,
            raw_score=round(raw, 2),
            detail=detail,
        )

    # ── Backward-compatible shortcut ───────────────────────────────────

    def score_from_selector(
        self,
        *,
        n_candidates: int = 1,
        errors_found: bool = False,
        cost_ok: bool = True,
        plan_consistent: bool = True,
        hard_plan_violations: int = 0,
        soft_plan_violations: int = 0,
        retrieval_quality: dict[str, Any] | None = None,
    ) -> tuple[str, float]:
        """Drop-in replacement for CandidateSelector._score_confidence.

        Returns (band, normalised_score) to match existing interface.
        """
        rq = retrieval_quality or {}
        signals = ConfidenceSignals(
            hard_plan_violations=hard_plan_violations,
            soft_plan_violations=soft_plan_violations,
            plan_consistent=plan_consistent,
            cost_guard_passed=cost_ok,
            entity_match_count=rq.get("entity_match_count", 0),
            glossary_match_count=rq.get("glossary_match_count", 0),
            similar_query_top_sim=rq.get("similar_query_top_sim", 0.0),
            table_count=rq.get("table_count", 0),
            probe_count=rq.get("probe_count", 0),
            has_few_shot=rq.get("has_few_shot", False),
            n_candidates=n_candidates,
            taxonomy_errors_found=errors_found,
        )
        result = self.score(signals)
        return result.band, result.score

    # ── Pipeline-level scoring (post-execution) ────────────────────────

    def score_pipeline(
        self,
        *,
        ctx_confidence: str = "LOW",
        query_plan: dict | None = None,
        validation_issues: list[str] | None = None,
        execution_judge_passed: bool | None = None,
        execution_judge_issues: list[str] | None = None,
        entity_match_count: int = 0,
        glossary_match_count: int = 0,
        similar_query_top_sim: float = 0.0,
        table_count: int = 0,
        probe_count: int = 0,
        grounding_ambiguity_count: int = 0,
        quality_warning_count: int = 0,
        has_trusted_query: bool = False,
        has_query_family: bool = False,
        cost_guard_result: dict | None = None,
        row_count: int = 0,
    ) -> ConfidenceResult:
        """Full pipeline confidence incorporating execution results."""
        cg = cost_guard_result or {}
        plan = query_plan or {}

        signals = ConfidenceSignals(
            plan_consistent="compiled_sql" in plan and not validation_issues,
            hard_plan_violations=0,
            soft_plan_violations=len(validation_issues or []),
            cost_guard_passed=cg.get("passed", True),
            cost_guard_explain_ok=cg.get("explain_ok", True),
            estimated_scan_gb=cg.get("estimated_scan_gb", 0.0),
            execution_judge_passed=execution_judge_passed,
            execution_judge_issues=list(execution_judge_issues or []),
            row_count=row_count,
            entity_match_count=entity_match_count,
            glossary_match_count=glossary_match_count,
            similar_query_top_sim=similar_query_top_sim,
            table_count=table_count,
            probe_count=probe_count,
            grounding_ambiguity_count=grounding_ambiguity_count,
            quality_warning_count=quality_warning_count,
            has_trusted_query=has_trusted_query,
            has_query_family=has_query_family,
        )
        return self.score(signals)

    # ── Internal dimension scorers ─────────────────────────────────────

    @staticmethod
    def _plan_score(s: ConfidenceSignals) -> float:
        if s.hard_plan_violations > 0:
            return 0.0
        if s.plan_consistent:
            pts = 3.0
        else:
            pts = 1.0
        # Soft violations degrade
        if s.soft_plan_violations >= 3:
            pts -= 1.5
        elif s.soft_plan_violations == 2:
            pts -= 1.0
        elif s.soft_plan_violations == 1:
            pts -= 0.5
        return max(pts, 0.0)

    @staticmethod
    def _cost_score(s: ConfidenceSignals) -> float:
        pts = 0.0
        if s.cost_guard_passed:
            pts += 1.0
        if s.cost_guard_explain_ok:
            pts += 0.5
        return pts

    @staticmethod
    def _execution_score(s: ConfidenceSignals) -> float:
        if s.execution_judge_passed is None:
            return 1.0  # Pre-execution — neutral (was 0.5, raised to avoid
                         # penalising queries before they've been tried)
        if s.execution_judge_passed:
            # Judge passed but empty result → partial credit only.
            # Calibration 2026-03-07: empty results were causing false-HIGH
            # (judge says "structurally fine" but user expected data).
            if s.row_count == 0:
                return 1.0
            return 2.0
        # Issues found — partial credit minus penalties
        issue_count = len(s.execution_judge_issues)
        return max(0.0, 1.0 - 0.5 * issue_count)

    @staticmethod
    def _retrieval_score(s: ConfidenceSignals) -> float:
        pts = 0.0

        # Trusted / family path — strong evidence
        if s.has_trusted_query:
            pts += 3.0
        elif s.has_query_family:
            pts += 2.0

        # Entity matches (0–1)
        if s.entity_match_count >= 2:
            pts += 1.0
        elif s.entity_match_count >= 1:
            pts += 0.5

        # Glossary matches (0–1.5)
        # Calibration: production queries consistently have 8-10 glossary
        # matches — these indicate strong schema grounding and deserve
        # higher credit than the previous 1.0 cap.
        if s.glossary_match_count >= 5:
            pts += 1.5
        elif s.glossary_match_count >= 2:
            pts += 1.0
        elif s.glossary_match_count >= 1:
            pts += 0.5

        # Similar query similarity (0–2)
        if s.similar_query_top_sim >= 0.80:
            pts += 2.0
        elif s.similar_query_top_sim >= 0.60:
            pts += 1.0
        elif s.similar_query_top_sim >= 0.40:
            pts += 0.5

        # Probe evidence (0–1)
        if s.probe_count >= 2:
            pts += 1.0
        elif s.probe_count >= 1:
            pts += 0.5

        return min(pts, 6.0)

    @staticmethod
    def _ambiguity_penalty(s: ConfidenceSignals) -> float:
        pts = 0.0
        if s.grounding_ambiguity_count >= 2:
            pts -= 2.0
        elif s.grounding_ambiguity_count == 1:
            pts -= 1.0
        if s.quality_warning_count >= 2:
            pts -= 0.5
        return pts

    @staticmethod
    def _diversity_score(s: ConfidenceSignals) -> float:
        if s.n_candidates >= 3:
            return 1.5
        if s.n_candidates >= 2:
            return 1.0
        return 0.0

    def _band(self, normalised: float) -> str:
        if normalised >= self._high:
            return "HIGH"
        if normalised >= self._medium:
            return "MEDIUM"
        if normalised >= self._abstain:
            return "LOW"
        return "ABSTAIN"
