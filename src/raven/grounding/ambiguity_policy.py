"""
Ambiguity Policy — Clarification vs Best-Guess
================================================
Decides whether an ambiguous grounding scenario should:

  - **Clarify** — ask the user to disambiguate (returns suggestions)
  - **Pick best** — silently choose the highest-confidence match
  - **Abstain** — decline to answer (too many unknowns)

Policy is configurable via thresholds and can incorporate:
  - Number of disambiguation candidates
  - Confidence gap between top and second candidate
  - Whether the user's focus context narrows the scope
  - Historical resolution patterns (future)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AmbiguityDecision:
    """Result of the ambiguity policy evaluation."""

    action: str  # "clarify" | "pick_best" | "abstain"
    picked_value: dict[str, Any] | None = None  # populated when action == "pick_best"
    suggestions: list[str] = field(default_factory=list)  # populated when action == "clarify"
    reason: str = ""


DEFAULT_THRESHOLDS = {
    # Minimum confidence gap between #1 and #2 to auto-pick without clarifying
    "auto_pick_gap": 0.25,
    # Maximum number of candidates before we flag "too ambiguous"
    "max_candidates_for_pick": 4,
    # Minimum top confidence to allow auto-pick
    "min_top_confidence": 0.70,
    # Maximum ambiguities before abstaining
    "max_ambiguities_for_proceed": 3,
}


class AmbiguityPolicy:
    """Evaluate grounding ambiguities and decide on action."""

    def __init__(self, thresholds: dict[str, float] | None = None):
        self._t = {**DEFAULT_THRESHOLDS, **(thresholds or {})}

    def evaluate(
        self,
        ambiguities: list[dict[str, Any]],
        resolved_filters: list[dict[str, Any]] | None = None,
        focus: Any | None = None,
    ) -> AmbiguityDecision:
        """
        Evaluate all ambiguities and return a single decision.

        Args:
            ambiguities: List of ambiguity dicts from ValueResolver.
            resolved_filters: Already-resolved filters (high confidence).
            focus: Optional FocusContext for scope narrowing.

        Returns:
            AmbiguityDecision with action + detail.
        """
        if not ambiguities:
            return AmbiguityDecision(action="pick_best", reason="no_ambiguities")

        # Too many ambiguities — abstain
        if len(ambiguities) > self._t["max_ambiguities_for_proceed"]:
            return AmbiguityDecision(
                action="abstain",
                reason=f"too_many_ambiguities:{len(ambiguities)}",
                suggestions=self._build_suggestions(ambiguities),
            )

        # Evaluate each ambiguity individually
        value_ambiguities = [a for a in ambiguities if a.get("type") == "value_match"]
        other_ambiguities = [a for a in ambiguities if a.get("type") != "value_match"]

        # Try focus-scoped resolution for value ambiguities
        if focus and value_ambiguities:
            resolved = self._try_focus_resolution(value_ambiguities, focus)
            if resolved:
                return AmbiguityDecision(
                    action="pick_best",
                    picked_value=resolved,
                    reason="focus_scoped_resolution",
                )

        # Single value ambiguity — try auto-pick by candidate quality
        if len(value_ambiguities) == 1:
            candidates = value_ambiguities[0].get("candidates", [])
            if len(candidates) <= self._t["max_candidates_for_pick"]:
                return AmbiguityDecision(
                    action="clarify",
                    suggestions=self._format_value_suggestions(value_ambiguities[0]),
                    reason="single_value_ambiguity",
                )

        # Multiple value ambiguities — always clarify
        if value_ambiguities:
            return AmbiguityDecision(
                action="clarify",
                suggestions=self._build_suggestions(ambiguities),
                reason="multiple_value_ambiguities",
            )

        # Only non-value ambiguities (e.g. metabase filter references) — proceed
        if other_ambiguities and not value_ambiguities:
            return AmbiguityDecision(
                action="pick_best",
                reason="weak_ambiguities_only",
            )

        return AmbiguityDecision(
            action="clarify",
            suggestions=self._build_suggestions(ambiguities),
            reason="general_ambiguity",
        )

    def evaluate_grounding_result(
        self,
        grounding_result: dict[str, Any],
        focus: Any | None = None,
    ) -> AmbiguityDecision:
        """Convenience wrapper that accepts the full GroundingResult dict."""
        return self.evaluate(
            ambiguities=grounding_result.get("ambiguities", []),
            resolved_filters=grounding_result.get("filters", []),
            focus=focus,
        )

    # ── Internal ───────────────────────────────────────────────────────

    @staticmethod
    def _try_focus_resolution(
        value_ambiguities: list[dict[str, Any]],
        focus: Any,
    ) -> dict[str, Any] | None:
        """Try to resolve ambiguity using focus context tables."""
        focus_tables = set(getattr(focus, "tables", []) or [])
        if not focus_tables:
            return None

        for amb in value_ambiguities:
            table = amb.get("table", "")
            if table and table in focus_tables:
                candidates = amb.get("candidates", [])
                if candidates:
                    return {
                        "table": table,
                        "column": amb.get("column", ""),
                        "value": candidates[0],
                        "source": "focus_resolution",
                    }
        return None

    @staticmethod
    def _format_value_suggestions(ambiguity: dict[str, Any]) -> list[str]:
        """Format a value ambiguity into user-friendly suggestions."""
        candidates = ambiguity.get("candidates", [])
        column = ambiguity.get("column", "value")
        table = ambiguity.get("table", "")

        suggestions: list[str] = []
        for candidate in candidates[:5]:
            if table:
                suggestions.append(f"Did you mean '{candidate}' from {table}.{column}?")
            else:
                suggestions.append(f"Did you mean '{candidate}'?")
        return suggestions

    @staticmethod
    def _build_suggestions(ambiguities: list[dict[str, Any]]) -> list[str]:
        """Build consolidated suggestion list from multiple ambiguities."""
        suggestions: list[str] = []
        for amb in ambiguities[:5]:
            amb_type = amb.get("type", "")
            if amb_type == "value_match":
                candidates = amb.get("candidates", [])
                column = amb.get("column", "")
                if candidates:
                    suggestions.append(
                        f"Multiple matches for '{candidates[0]}' in {column} — "
                        f"options: {', '.join(str(c) for c in candidates[:4])}"
                    )
            elif amb_type == "metabase_filter_reference":
                suggestions.append(
                    f"Found a Metabase filter '{amb.get('filter', '')}' — "
                    "please specify the exact value."
                )
        return suggestions
