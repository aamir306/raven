"""Tests for the calibrated ConfidenceModel."""

from __future__ import annotations

from src.raven.validation.confidence_model import (
    ConfidenceModel,
    ConfidenceResult,
    ConfidenceSignals,
)


def test_confidence_model_high_score_for_perfect_signals():
    model = ConfidenceModel()
    signals = ConfidenceSignals(
        plan_consistent=True,
        cost_guard_passed=True,
        cost_guard_explain_ok=True,
        execution_judge_passed=True,
        entity_match_count=3,
        glossary_match_count=2,
        similar_query_top_sim=0.85,
        probe_count=2,
        has_trusted_query=True,
        n_candidates=3,
    )
    result = model.score(signals)
    assert result.band == "HIGH"
    assert result.score >= 0.72
    assert not result.should_abstain


def test_confidence_model_low_score_for_hard_violations():
    model = ConfidenceModel()
    signals = ConfidenceSignals(hard_plan_violations=2)
    result = model.score(signals)
    assert result.band == "ABSTAIN"
    assert result.score == 0.0
    assert result.should_abstain


def test_confidence_model_medium_band():
    model = ConfidenceModel()
    signals = ConfidenceSignals(
        plan_consistent=True,
        cost_guard_passed=True,
        cost_guard_explain_ok=True,
        entity_match_count=1,
        glossary_match_count=1,
        similar_query_top_sim=0.55,
        n_candidates=2,
    )
    result = model.score(signals)
    assert result.band in {"MEDIUM", "HIGH"}
    assert result.score >= 0.45


def test_confidence_model_abstain_for_weak_signals():
    model = ConfidenceModel()
    signals = ConfidenceSignals(
        plan_consistent=False,
        cost_guard_passed=False,
        grounding_ambiguity_count=3,
        taxonomy_errors_found=True,
    )
    result = model.score(signals)
    assert result.band in {"LOW", "ABSTAIN"}
    assert result.score < 0.45


def test_confidence_model_score_from_selector_backward_compat():
    model = ConfidenceModel()
    band, score = model.score_from_selector(
        n_candidates=3,
        errors_found=False,
        cost_ok=True,
        plan_consistent=True,
        hard_plan_violations=0,
        soft_plan_violations=0,
        retrieval_quality={
            "entity_match_count": 2,
            "glossary_match_count": 1,
            "similar_query_top_sim": 0.75,
            "probe_count": 1,
        },
    )
    assert band in {"HIGH", "MEDIUM"}
    assert 0.0 <= score <= 1.0


def test_confidence_model_ambiguity_penalty():
    model = ConfidenceModel()
    # Without ambiguity
    signals_good = ConfidenceSignals(
        plan_consistent=True,
        cost_guard_passed=True,
        cost_guard_explain_ok=True,
        entity_match_count=1,
    )
    result_good = model.score(signals_good)

    # With ambiguity
    signals_amb = ConfidenceSignals(
        plan_consistent=True,
        cost_guard_passed=True,
        cost_guard_explain_ok=True,
        entity_match_count=1,
        grounding_ambiguity_count=2,
    )
    result_amb = model.score(signals_amb)
    assert result_amb.score < result_good.score


def test_confidence_model_execution_judge_boost():
    model = ConfidenceModel()
    signals_pre = ConfidenceSignals(
        plan_consistent=True,
        cost_guard_passed=True,
        execution_judge_passed=None,  # Pre-execution
    )
    result_pre = model.score(signals_pre)

    signals_post = ConfidenceSignals(
        plan_consistent=True,
        cost_guard_passed=True,
        execution_judge_passed=True,  # Post-execution pass
    )
    result_post = model.score(signals_post)
    assert result_post.score > result_pre.score


def test_confidence_model_detail_keys():
    model = ConfidenceModel()
    result = model.score(ConfidenceSignals())
    assert "plan" in result.detail
    assert "cost" in result.detail
    assert "execution" in result.detail
    assert "retrieval" in result.detail
    assert "ambiguity" in result.detail
    assert "diversity" in result.detail
    assert "taxonomy" in result.detail


def test_confidence_model_score_pipeline():
    model = ConfidenceModel()
    result = model.score_pipeline(
        query_plan={"compiled_sql": "SELECT 1", "intent": "KPI"},
        execution_judge_passed=True,
        entity_match_count=2,
        glossary_match_count=1,
        has_trusted_query=True,
        cost_guard_result={"passed": True, "explain_ok": True, "estimated_scan_gb": 1.5},
    )
    assert result.band in {"HIGH", "MEDIUM"}
    assert not result.should_abstain
