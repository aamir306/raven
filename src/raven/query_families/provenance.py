"""
Query family provenance tracking.

Records how each query-family match was derived, what template was used,
which slots were substituted, and how confident the compilation is.
Provides an audit trail for every trusted-path SQL answer.
"""

from __future__ import annotations

import time
from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(frozen=True)
class SlotSubstitution:
    """A single slot substitution applied during compilation."""

    slot_type: str  # "limit", "interval", "order_direction", "time_grain", "filter", "metric", "dimension", "join"
    original_value: str  # What was in the template
    new_value: str  # What the user question mapped to
    column_ref: str = ""  # Column reference for filter/dimension slots


@dataclass
class FamilyProvenance:
    """Full provenance record for a query-family match.

    Captures every decision made during matching and compilation,
    so debugging "why did the system produce this SQL?" is answerable
    without re-running the pipeline.
    """

    # ── Identity ──
    family_key: str = ""  # Normalized family key
    template_question: str = ""  # Original verified question
    template_sql: str = ""  # Original verified SQL
    compiled_sql: str = ""  # Final compiled SQL after all substitutions

    # ── Source ──
    source: str = ""  # "semantic_model" | "metabase" | "verified_query"
    source_file: str = ""  # Path to source YAML/JSON if applicable
    source_id: str = ""  # Unique ID within source (e.g. Metabase card ID)

    # ── Tables ──
    tables_used: list[str] = field(default_factory=list)
    tables_from_template: list[str] = field(default_factory=list)

    # ── Match details ──
    similarity_score: float = 0.0
    match_type: str = ""  # "exact", "family_key", "slot_substitution"
    question_normalized: str = ""
    template_normalized: str = ""

    # ── Substitutions ──
    slot_substitutions: list[SlotSubstitution] = field(default_factory=list)
    filter_replacements: list[dict[str, Any]] = field(default_factory=list)
    metric_replacements: list[dict[str, Any]] = field(default_factory=list)
    dimension_replacements: list[dict[str, Any]] = field(default_factory=list)
    join_replacements: list[dict[str, Any]] = field(default_factory=list)

    # ── Confidence ──
    compilation_confidence: float = 1.0  # 1.0 = no substitutions, degrades per substitution
    evidence_strength: float = 0.0  # How strong the supporting evidence is

    # ── Metadata ──
    timestamp: float = field(default_factory=time.time)
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self._compute_compilation_confidence()

    def _compute_compilation_confidence(self) -> None:
        """Confidence degrades proportionally with number of substitutions."""
        total_subs = (
            len(self.slot_substitutions)
            + len(self.filter_replacements)
            + len(self.metric_replacements)
            + len(self.dimension_replacements)
            + len(self.join_replacements)
        )
        # Each substitution costs 0.05 confidence, min 0.40
        self.compilation_confidence = max(0.40, 1.0 - total_subs * 0.05)

        # Evaluate evidence strength
        evidence_score = 0.0
        if self.source == "verified_query":
            evidence_score += 0.4
        elif self.source == "semantic_model":
            evidence_score += 0.3
        elif self.source == "metabase":
            evidence_score += 0.25
        evidence_score += min(self.similarity_score * 0.4, 0.4)
        evidence_score += min(self.compilation_confidence * 0.2, 0.2)
        self.evidence_strength = min(evidence_score, 1.0)

    def to_dict(self) -> dict[str, Any]:
        result = {
            "family_key": self.family_key,
            "template_question": self.template_question,
            "compiled_sql": self.compiled_sql,
            "source": self.source,
            "tables_used": self.tables_used,
            "similarity_score": round(self.similarity_score, 4),
            "match_type": self.match_type,
            "compilation_confidence": round(self.compilation_confidence, 4),
            "evidence_strength": round(self.evidence_strength, 4),
            "substitution_count": len(self.slot_substitutions)
            + len(self.filter_replacements)
            + len(self.metric_replacements)
            + len(self.dimension_replacements)
            + len(self.join_replacements),
        }
        if self.slot_substitutions:
            result["slot_substitutions"] = [asdict(s) for s in self.slot_substitutions]
        if self.filter_replacements:
            result["filter_replacements"] = self.filter_replacements
        if self.metric_replacements:
            result["metric_replacements"] = self.metric_replacements
        if self.dimension_replacements:
            result["dimension_replacements"] = self.dimension_replacements
        if self.join_replacements:
            result["join_replacements"] = self.join_replacements
        if self.metadata:
            result["metadata"] = self.metadata
        return result

    def summary(self) -> str:
        """One-line human-readable summary."""
        subs = (
            len(self.slot_substitutions)
            + len(self.filter_replacements)
            + len(self.metric_replacements)
            + len(self.dimension_replacements)
            + len(self.join_replacements)
        )
        return (
            f"[{self.source}] family={self.family_key[:40]} "
            f"sim={self.similarity_score:.2f} "
            f"subs={subs} "
            f"conf={self.compilation_confidence:.2f} "
            f"evidence={self.evidence_strength:.2f}"
        )


def build_provenance_from_match(
    match: dict[str, Any],
    *,
    user_question: str = "",
    question_normalized: str = "",
) -> FamilyProvenance:
    """Build a FamilyProvenance from a QueryFamilyMatcher result dict.

    This is the main integration point — the matcher's output dict is
    translated into a typed provenance record.
    """
    slots = match.get("slots", {})
    slot_subs: list[SlotSubstitution] = []
    for slot_type, new_value in slots.items():
        slot_subs.append(
            SlotSubstitution(
                slot_type=slot_type,
                original_value="",
                new_value=str(new_value),
            )
        )

    source = match.get("source", "semantic_model")
    match_type = "family_key"
    if not slots and not match.get("filter_replacements"):
        match_type = "exact" if match.get("similarity", 0) >= 0.95 else "family_key"
    elif slots or match.get("filter_replacements"):
        match_type = "slot_substitution"

    return FamilyProvenance(
        family_key=match.get("family_key", ""),
        template_question=match.get("question", ""),
        template_sql=match.get("template_sql", match.get("sql", "")),
        compiled_sql=match.get("sql", ""),
        source=source,
        source_id=str(match.get("metadata", {}).get("id", "")),
        tables_used=list(match.get("tables_used", [])),
        tables_from_template=list(match.get("tables_used", [])),
        similarity_score=float(match.get("similarity", 0.0)),
        match_type=match_type,
        question_normalized=question_normalized,
        template_normalized=match.get("family_key", ""),
        slot_substitutions=slot_subs,
        filter_replacements=list(match.get("filter_replacements", [])),
        metric_replacements=list(match.get("metric_replacements", [])),
        dimension_replacements=list(match.get("dimension_replacements", [])),
        join_replacements=list(match.get("join_replacements", [])),
        metadata=dict(match.get("metadata", {})),
    )
