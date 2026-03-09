#!/usr/bin/env python3
"""
RAVEN — Confidence Model Calibration
======================================
Runs production-style queries through the live pipeline, collects
confidence scores vs actual execution outcomes, and produces a
calibration report with recommended threshold adjustments.

Usage:
    # Full calibration (100 test_set + 20 business_critical + 6 e2e)
    python scripts/calibrate_confidence.py

    # Quick calibration (business-critical only)
    python scripts/calibrate_confidence.py --quick

    # Specific test set
    python scripts/calibrate_confidence.py --test-set tests/test_set.json

    # Against running API server (default: direct pipeline)
    python scripts/calibrate_confidence.py --api http://localhost:8000

    # Dry-run: skip execution, only score signals
    python scripts/calibrate_confidence.py --dry-run

Requires:
    - Live Trino connection (VPN)
    - .env with credentials
    - Backend running if --api used
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import os
import statistics
import sys
import time
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

# Add project root
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("calibration")


# ── Data structures ────────────────────────────────────────────────────


@dataclass
class CalibrationSample:
    """One query's calibration data point."""

    question_id: int | str = ""
    question: str = ""
    category: str = ""
    difficulty: str = ""

    # Confidence model outputs
    confidence_band: str = ""
    confidence_score: float = 0.0
    confidence_raw: float = 0.0
    confidence_detail: dict = field(default_factory=dict)

    # Actual outcomes
    execution_success: bool = False
    row_count: int = 0
    sql_generated: bool = False
    sql: str = ""
    status: str = ""
    error: str = ""

    # Timing
    latency_s: float = 0.0
    cost_usd: float = 0.0

    # Pipeline debug signals
    entity_matches: int = 0
    glossary_matches: int = 0
    selected_tables: int = 0
    candidates_count: int = 0
    probe_count: int = 0
    validation_issues: list = field(default_factory=list)
    query_plan_type: str = ""
    trusted_query_source: str = ""


@dataclass
class CalibrationReport:
    """Full calibration analysis."""

    timestamp: str = ""
    total_queries: int = 0
    execution_rate: float = 0.0
    total_latency_s: float = 0.0
    total_cost_usd: float = 0.0

    # Band distribution
    band_counts: dict = field(default_factory=dict)
    band_success_rates: dict = field(default_factory=dict)
    band_avg_latency: dict = field(default_factory=dict)

    # Calibration metrics
    brier_score: float = 0.0
    ece: float = 0.0  # Expected Calibration Error

    # Threshold recommendations
    current_thresholds: dict = field(default_factory=dict)
    recommended_thresholds: dict = field(default_factory=dict)
    threshold_justification: list = field(default_factory=list)

    # Per-category breakdown
    category_stats: dict = field(default_factory=dict)
    difficulty_stats: dict = field(default_factory=dict)

    # Raw samples for inspection
    samples: list = field(default_factory=list)

    # Abstain analysis
    abstain_count: int = 0
    false_abstain_count: int = 0  # Would have succeeded
    true_abstain_count: int = 0  # Would have failed


# ── Query runner (direct pipeline) ────────────────────────────────────


async def run_via_pipeline(
    pipeline: Any,
    question: str,
    question_id: int | str,
    category: str,
    difficulty: str,
) -> CalibrationSample:
    """Run a single question through the pipeline directly."""
    sample = CalibrationSample(
        question_id=question_id,
        question=question,
        category=category,
        difficulty=difficulty,
    )

    start = time.monotonic()
    try:
        result = await pipeline.generate(question)
    except Exception as e:
        sample.status = "error"
        sample.error = str(e)
        sample.latency_s = time.monotonic() - start
        return sample

    elapsed = time.monotonic() - start
    sample.latency_s = round(elapsed, 3)
    sample.status = result.get("status", "unknown")
    sample.sql = result.get("sql", "")
    sample.sql_generated = bool(sample.sql)
    sample.row_count = result.get("row_count", 0) or 0
    sample.execution_success = sample.status == "success" and sample.row_count > 0
    sample.confidence_band = result.get("confidence", "LOW")
    sample.cost_usd = result.get("cost", 0.0) or 0.0
    sample.error = result.get("error", "")

    # Extract debug info
    debug = result.get("debug", {})
    sample.entity_matches = debug.get("entity_matches", 0)
    sample.glossary_matches = debug.get("glossary_matches", 0)
    sample.selected_tables = len(debug.get("selected_tables", []))
    sample.candidates_count = debug.get("candidates_count", 0)
    sample.probe_count = debug.get("probe_count", 0)
    sample.query_plan_type = (debug.get("query_plan") or {}).get("path_type", "")
    sample.trusted_query_source = debug.get("trusted_query_source", "") or ""

    return sample


# ── Query runner (API) ─────────────────────────────────────────────────


async def run_via_api(
    api_url: str,
    question: str,
    question_id: int | str,
    category: str,
    difficulty: str,
) -> CalibrationSample:
    """Run a single question through the HTTP API."""
    import httpx

    sample = CalibrationSample(
        question_id=question_id,
        question=question,
        category=category,
        difficulty=difficulty,
    )

    start = time.monotonic()
    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            resp = await client.post(
                f"{api_url.rstrip('/')}/api/query",
                json={"question": question},
            )
            resp.raise_for_status()
            result = resp.json()
    except Exception as e:
        sample.status = "error"
        sample.error = str(e)
        sample.latency_s = time.monotonic() - start
        return sample

    elapsed = time.monotonic() - start
    sample.latency_s = round(elapsed, 3)
    sample.status = result.get("status", "unknown")
    sample.sql = result.get("sql", "")
    sample.sql_generated = bool(sample.sql)
    sample.row_count = result.get("row_count", 0) or 0
    sample.execution_success = sample.status == "success" and sample.row_count > 0
    sample.confidence_band = result.get("confidence", "LOW")
    sample.cost_usd = result.get("cost", 0.0) or 0.0
    sample.error = result.get("error", "")

    debug = result.get("debug", {})
    sample.entity_matches = debug.get("entity_matches", 0)
    sample.glossary_matches = debug.get("glossary_matches", 0)
    sample.selected_tables = len(debug.get("selected_tables", []))
    sample.candidates_count = debug.get("candidates_count", 0)
    sample.probe_count = debug.get("probe_count", 0)
    sample.query_plan_type = (debug.get("query_plan") or {}).get("path_type", "")
    sample.trusted_query_source = debug.get("trusted_query_source", "") or ""

    return sample


# ── Offline confidence re-scorer ───────────────────────────────────────


def rescore_sample(sample: CalibrationSample) -> CalibrationSample:
    """Re-score a sample's confidence using the ConfidenceModel directly.

    This enriches samples with raw score and detail breakdown,
    even when run via API (which only returns band string).
    """
    from src.raven.validation.confidence_model import (
        ConfidenceModel,
        ConfidenceSignals,
    )

    model = ConfidenceModel()

    signals = ConfidenceSignals(
        plan_consistent=sample.status == "success",
        hard_plan_violations=0,
        soft_plan_violations=len(sample.validation_issues),
        cost_guard_passed=True,  # passed if execution succeeded
        cost_guard_explain_ok=True,
        execution_judge_passed=sample.execution_success or (sample.status == "success"),
        execution_judge_issues=[],
        row_count=sample.row_count,
        entity_match_count=sample.entity_matches,
        glossary_match_count=sample.glossary_matches,
        similar_query_top_sim=0.0,
        table_count=sample.selected_tables,
        probe_count=sample.probe_count,
        has_trusted_query=bool(sample.trusted_query_source),
        n_candidates=sample.candidates_count,
        taxonomy_errors_found=False,
    )

    result = model.score(signals)
    sample.confidence_score = result.score
    sample.confidence_raw = result.raw_score
    sample.confidence_detail = result.detail

    return sample


# ── Calibration analysis ───────────────────────────────────────────────


def _band_to_expected_success(band: str) -> float:
    """Map confidence band to our expected success probability."""
    return {
        "HIGH": 0.90,
        "MEDIUM": 0.65,
        "LOW": 0.35,
        "ABSTAIN": 0.05,
    }.get(band, 0.30)


def analyze(samples: list[CalibrationSample]) -> CalibrationReport:
    """Produce a full calibration report from collected samples."""
    from src.raven.validation.confidence_model import DEFAULT_THRESHOLDS

    report = CalibrationReport(
        timestamp=time.strftime("%Y-%m-%dT%H:%M:%S"),
        total_queries=len(samples),
        current_thresholds=dict(DEFAULT_THRESHOLDS),
    )

    if not samples:
        return report

    # ── Global metrics ─────────────────────────────────────────────
    success_count = sum(1 for s in samples if s.execution_success)
    report.execution_rate = round(100 * success_count / len(samples), 1)
    report.total_latency_s = round(sum(s.latency_s for s in samples), 2)
    report.total_cost_usd = round(sum(s.cost_usd for s in samples), 4)

    # ── Band distribution & success rates ──────────────────────────
    bands = defaultdict(list)
    for s in samples:
        bands[s.confidence_band].append(s)

    for band_name in ("HIGH", "MEDIUM", "LOW", "ABSTAIN"):
        band_samples = bands.get(band_name, [])
        report.band_counts[band_name] = len(band_samples)
        if band_samples:
            succ = sum(1 for s in band_samples if s.execution_success)
            report.band_success_rates[band_name] = round(100 * succ / len(band_samples), 1)
            report.band_avg_latency[band_name] = round(
                statistics.mean(s.latency_s for s in band_samples), 2
            )
        else:
            report.band_success_rates[band_name] = 0.0
            report.band_avg_latency[band_name] = 0.0

    # ── Brier Score (calibration quality) ──────────────────────────
    # Lower = better calibrated; 0 = perfect
    brier_sum = 0.0
    for s in samples:
        predicted = _band_to_expected_success(s.confidence_band)
        actual = 1.0 if s.execution_success else 0.0
        brier_sum += (predicted - actual) ** 2
    report.brier_score = round(brier_sum / len(samples), 4)

    # ── Expected Calibration Error (ECE) ───────────────────────────
    # Measures how well predicted probabilities match observed frequencies
    ece_total = 0.0
    for band_name, band_samples in bands.items():
        if not band_samples:
            continue
        expected = _band_to_expected_success(band_name)
        observed = sum(1 for s in band_samples if s.execution_success) / len(band_samples)
        weight = len(band_samples) / len(samples)
        ece_total += weight * abs(expected - observed)
    report.ece = round(ece_total, 4)

    # ── Per-category breakdown ─────────────────────────────────────
    cats = defaultdict(list)
    for s in samples:
        cats[s.category].append(s)
    for cat, cat_samples in sorted(cats.items()):
        succ = sum(1 for s in cat_samples if s.execution_success)
        avg_conf = statistics.mean(s.confidence_score for s in cat_samples) if cat_samples else 0.0
        report.category_stats[cat] = {
            "count": len(cat_samples),
            "success_rate": round(100 * succ / len(cat_samples), 1) if cat_samples else 0.0,
            "avg_confidence_score": round(avg_conf, 3),
            "avg_latency_s": round(statistics.mean(s.latency_s for s in cat_samples), 2),
        }

    # ── Per-difficulty breakdown ───────────────────────────────────
    diffs = defaultdict(list)
    for s in samples:
        diffs[s.difficulty].append(s)
    for diff, diff_samples in sorted(diffs.items()):
        succ = sum(1 for s in diff_samples if s.execution_success)
        avg_conf = statistics.mean(s.confidence_score for s in diff_samples) if diff_samples else 0.0
        report.difficulty_stats[diff] = {
            "count": len(diff_samples),
            "success_rate": round(100 * succ / len(diff_samples), 1) if diff_samples else 0.0,
            "avg_confidence_score": round(avg_conf, 3),
            "avg_latency_s": round(statistics.mean(s.latency_s for s in diff_samples), 2),
        }

    # ── Abstain analysis ───────────────────────────────────────────
    abstain_samples = bands.get("ABSTAIN", [])
    report.abstain_count = len(abstain_samples)
    report.false_abstain_count = sum(
        1 for s in abstain_samples if s.execution_success
    )
    report.true_abstain_count = report.abstain_count - report.false_abstain_count

    # ── Threshold recommendations ──────────────────────────────────
    report.recommended_thresholds, report.threshold_justification = _recommend_thresholds(
        samples, bands, report
    )

    report.samples = [asdict(s) for s in samples]

    return report


def _recommend_thresholds(
    samples: list[CalibrationSample],
    bands: dict[str, list[CalibrationSample]],
    report: CalibrationReport,
) -> tuple[dict, list[str]]:
    """Suggest new thresholds based on observed data."""
    from src.raven.validation.confidence_model import DEFAULT_THRESHOLDS

    current = dict(DEFAULT_THRESHOLDS)
    recommended = dict(DEFAULT_THRESHOLDS)
    justifications: list[str] = []

    scores_by_outcome: dict[str, list[float]] = {"success": [], "failure": []}
    for s in samples:
        if s.execution_success:
            scores_by_outcome["success"].append(s.confidence_score)
        else:
            scores_by_outcome["failure"].append(s.confidence_score)

    # ── Analyse score distributions ────────────────────────────────
    succ_scores = scores_by_outcome["success"]
    fail_scores = scores_by_outcome["failure"]

    if succ_scores:
        succ_min = min(succ_scores)
        succ_mean = statistics.mean(succ_scores)
        succ_median = statistics.median(succ_scores)
        justifications.append(
            f"Successful queries: score range [{succ_min:.3f}, {max(succ_scores):.3f}], "
            f"mean={succ_mean:.3f}, median={succ_median:.3f}"
        )
    else:
        succ_min = succ_mean = succ_median = 0.0
        justifications.append("No successful queries — cannot determine optimal high threshold")

    if fail_scores:
        fail_max = max(fail_scores)
        fail_mean = statistics.mean(fail_scores)
        fail_median = statistics.median(fail_scores)
        justifications.append(
            f"Failed queries: score range [{min(fail_scores):.3f}, {fail_max:.3f}], "
            f"mean={fail_mean:.3f}, median={fail_median:.3f}"
        )
    else:
        fail_max = fail_mean = fail_median = 0.0
        justifications.append("All queries succeeded — no failures to calibrate against")

    # ── HIGH threshold: should capture queries with >85% success ───
    if succ_scores:
        # Use the 25th percentile of successful scores as HIGH floor
        sorted_succ = sorted(succ_scores)
        p25_idx = max(0, len(sorted_succ) // 4 - 1)
        p25_succ = sorted_succ[p25_idx]

        if fail_scores:
            # Set HIGH above the max failure score ideally
            new_high = max(fail_max + 0.05, p25_succ)
            new_high = round(min(new_high, 0.95), 2)
        else:
            new_high = round(max(p25_succ, 0.60), 2)

        if abs(new_high - current["high"]) > 0.03:
            recommended["high"] = new_high
            justifications.append(
                f"HIGH threshold: {current['high']:.2f} → {new_high:.2f} "
                f"(25th pct success={p25_succ:.3f}, max fail={fail_max:.3f})"
            )
        else:
            justifications.append(f"HIGH threshold {current['high']:.2f} is well-calibrated")

    # ── MEDIUM threshold: balance sensitivity vs specificity ───────
    if succ_scores and fail_scores:
        # Optimal boundary: midpoint between fail_mean and succ_mean
        new_medium = round((fail_mean + succ_mean) / 2, 2)
        new_medium = max(0.20, min(new_medium, recommended.get("high", 0.72) - 0.10))

        if abs(new_medium - current["medium"]) > 0.03:
            recommended["medium"] = new_medium
            justifications.append(
                f"MEDIUM threshold: {current['medium']:.2f} → {new_medium:.2f} "
                f"(midpoint of fail_mean={fail_mean:.3f} and succ_mean={succ_mean:.3f})"
            )
        else:
            justifications.append(f"MEDIUM threshold {current['medium']:.2f} is well-calibrated")

    # ── ABSTAIN threshold: minimize false abstains ─────────────────
    high_success_rate = report.band_success_rates.get("HIGH", 0.0)
    if report.false_abstain_count > 0:
        # Abstaining on queries that would succeed → lower the threshold
        abstain_scores_that_succeeded = [
            s.confidence_score
            for s in bands.get("ABSTAIN", [])
            if s.execution_success
        ]
        if abstain_scores_that_succeeded:
            new_abstain = round(max(abstain_scores_that_succeeded) - 0.02, 2)
            new_abstain = max(0.05, new_abstain)
            recommended["abstain"] = new_abstain
            justifications.append(
                f"ABSTAIN threshold: {current['abstain']:.2f} → {new_abstain:.2f} "
                f"(reducing false abstains: {report.false_abstain_count} queries "
                f"would have succeeded)"
            )
    elif report.abstain_count == 0:
        justifications.append("No ABSTAIN predictions — threshold not testable with this data")
    else:
        justifications.append(
            f"ABSTAIN threshold {current['abstain']:.2f} working correctly — "
            f"all {report.true_abstain_count} abstained queries would have failed"
        )

    # ── Score-dimension weight adjustments ─────────────────────────
    # Analyze which dimensions are most predictive by correlation
    if len(samples) >= 10:
        dimension_predictiveness = _analyze_dimension_predictiveness(samples)
        justifications.append("Dimension predictiveness (point-biserial r):")
        for dim, corr in dimension_predictiveness:
            justifications.append(f"  {dim}: r={corr:+.3f}")

    return recommended, justifications


def _analyze_dimension_predictiveness(
    samples: list[CalibrationSample],
) -> list[tuple[str, float]]:
    """Compute point-biserial correlation between each dimension score and success."""
    dims_data: dict[str, list[float]] = defaultdict(list)
    outcomes: list[float] = []

    for s in samples:
        outcomes.append(1.0 if s.execution_success else 0.0)
        for k, v in s.confidence_detail.items():
            if isinstance(v, (int, float)):
                dims_data[k].append(float(v))

    results = []
    n = len(outcomes)
    if n < 3:
        return results

    for dim, values in dims_data.items():
        if len(values) != n:
            continue
        # Point-biserial correlation
        try:
            mean_all = statistics.mean(values)
            std_all = statistics.stdev(values)
            if std_all == 0:
                results.append((dim, 0.0))
                continue

            group_1 = [v for v, o in zip(values, outcomes) if o == 1.0]
            group_0 = [v for v, o in zip(values, outcomes) if o == 0.0]

            if not group_1 or not group_0:
                results.append((dim, 0.0))
                continue

            mean_1 = statistics.mean(group_1)
            mean_0 = statistics.mean(group_0)
            n1 = len(group_1)
            n0 = len(group_0)

            rpb = ((mean_1 - mean_0) / std_all) * math.sqrt(n1 * n0 / (n * n))
            results.append((dim, round(rpb, 3)))
        except Exception:
            results.append((dim, 0.0))

    results.sort(key=lambda x: abs(x[1]), reverse=True)
    return results


# ── Report formatter ───────────────────────────────────────────────────


def format_report(report: CalibrationReport) -> str:
    """Pretty-print calibration report."""
    lines = []
    w = 80

    lines.append("=" * w)
    lines.append("  RAVEN CONFIDENCE MODEL — CALIBRATION REPORT")
    lines.append(f"  {report.timestamp}")
    lines.append("=" * w)

    lines.append(f"\n  Total queries:     {report.total_queries}")
    lines.append(f"  Execution rate:    {report.execution_rate}%")
    lines.append(f"  Total latency:     {report.total_latency_s}s")
    lines.append(f"  Total cost:        ${report.total_cost_usd:.4f}")

    # Calibration quality
    lines.append(f"\n  Brier Score:       {report.brier_score:.4f}  (0=perfect, 0.25=random)")
    lines.append(f"  ECE:               {report.ece:.4f}  (0=perfectly calibrated)")

    # Band distribution
    lines.append(f"\n{'─' * w}")
    lines.append("  CONFIDENCE BAND DISTRIBUTION")
    lines.append(f"{'─' * w}")
    lines.append(f"  {'Band':<10} {'Count':>6} {'%':>6} {'Success%':>10} {'Avg Lat':>10}")
    lines.append(f"  {'─'*10} {'─'*6} {'─'*6} {'─'*10} {'─'*10}")
    for band in ("HIGH", "MEDIUM", "LOW", "ABSTAIN"):
        cnt = report.band_counts.get(band, 0)
        pct = round(100 * cnt / report.total_queries, 1) if report.total_queries else 0
        succ = report.band_success_rates.get(band, 0.0)
        lat = report.band_avg_latency.get(band, 0.0)
        marker = ""
        if band == "HIGH" and succ < 80:
            marker = " ⚠️ LOW SUCCESS"
        elif band == "ABSTAIN" and cnt > 0:
            fa = report.false_abstain_count
            marker = f" ({fa} false abstains)" if fa else " (all correct)"
        lines.append(f"  {band:<10} {cnt:>6} {pct:>5.1f}% {succ:>9.1f}% {lat:>9.2f}s{marker}")

    # Per-difficulty
    if report.difficulty_stats:
        lines.append(f"\n{'─' * w}")
        lines.append("  PER-DIFFICULTY BREAKDOWN")
        lines.append(f"{'─' * w}")
        lines.append(f"  {'Difficulty':<12} {'Count':>6} {'Success%':>10} {'Avg Conf':>10} {'Avg Lat':>10}")
        for diff, stats in sorted(report.difficulty_stats.items()):
            lines.append(
                f"  {diff:<12} {stats['count']:>6} {stats['success_rate']:>9.1f}% "
                f"{stats['avg_confidence_score']:>9.3f} {stats['avg_latency_s']:>9.2f}s"
            )

    # Per-category (condensed)
    if report.category_stats:
        lines.append(f"\n{'─' * w}")
        lines.append("  PER-CATEGORY BREAKDOWN")
        lines.append(f"{'─' * w}")
        lines.append(f"  {'Category':<20} {'N':>4} {'Succ%':>7} {'AvgConf':>8} {'AvgLat':>8}")
        for cat, stats in sorted(report.category_stats.items()):
            lines.append(
                f"  {cat:<20} {stats['count']:>4} {stats['success_rate']:>6.1f}% "
                f"{stats['avg_confidence_score']:>7.3f} {stats['avg_latency_s']:>7.2f}s"
            )

    # Threshold recommendations
    lines.append(f"\n{'─' * w}")
    lines.append("  THRESHOLD CALIBRATION")
    lines.append(f"{'─' * w}")
    lines.append(f"  {'Threshold':<12} {'Current':>10} {'Recommended':>13} {'Change':>10}")
    lines.append(f"  {'─'*12} {'─'*10} {'─'*13} {'─'*10}")
    for key in ("high", "medium", "abstain"):
        cur = report.current_thresholds.get(key, 0.0)
        rec = report.recommended_thresholds.get(key, cur)
        delta = rec - cur
        marker = "✓" if abs(delta) < 0.03 else "↑" if delta > 0 else "↓"
        lines.append(f"  {key:<12} {cur:>10.2f} {rec:>13.2f} {delta:>+9.2f} {marker}")

    lines.append(f"\n  Justifications:")
    for j in report.threshold_justification:
        lines.append(f"    • {j}")

    # Worst performers (for debugging)
    worst = [
        s for s in (CalibrationSample(**d) if isinstance(d, dict) else d for d in report.samples)
        if s.confidence_band == "HIGH" and not s.execution_success
    ]
    if worst:
        lines.append(f"\n{'─' * w}")
        lines.append(f"  HIGH-CONFIDENCE FAILURES ({len(worst)} queries)")
        lines.append(f"{'─' * w}")
        for s in worst[:10]:
            q_short = s.question[:60] + ("..." if len(s.question) > 60 else "")
            lines.append(f"  [{s.question_id}] {q_short}")
            lines.append(f"       score={s.confidence_score:.3f} status={s.status} error={s.error[:80]}")

    lines.append(f"\n{'=' * w}")
    return "\n".join(lines)


# ── Load test questions ────────────────────────────────────────────────


def load_questions(path: Path) -> list[dict]:
    """Load questions from a test set JSON file."""
    data = json.loads(path.read_text())
    questions = []
    for item in data:
        questions.append({
            "id": item.get("id", len(questions) + 1),
            "question": item["question"],
            "category": item.get("category", "unknown"),
            "difficulty": item.get("difficulty", "unknown"),
        })
    return questions


# ── Main orchestrator ──────────────────────────────────────────────────


async def run_calibration(args: argparse.Namespace) -> CalibrationReport:
    """Orchestrate the full calibration run."""

    # ── Load questions ─────────────────────────────────────────────
    questions: list[dict] = []

    if args.test_set:
        for ts_path in args.test_set:
            p = Path(ts_path)
            if p.exists():
                questions.extend(load_questions(p))
                logger.info("Loaded %d questions from %s", len(questions), p)
            else:
                logger.warning("Test set not found: %s", p)
    elif args.quick:
        # Business-critical only
        bc_path = ROOT / "tests" / "test_set_business_critical.json"
        if bc_path.exists():
            questions = load_questions(bc_path)
            logger.info("Quick mode: %d business-critical questions", len(questions))
    else:
        # Full: business_critical + test_set + e2e
        for name in ("test_set_business_critical.json", "test_set.json"):
            p = ROOT / "tests" / name
            if p.exists():
                qs = load_questions(p)
                # Avoid duplicates
                existing_ids = {q["id"] for q in questions}
                for q in qs:
                    if q["id"] not in existing_ids:
                        questions.append(q)
                logger.info("Loaded %d (total %d) from %s", len(qs), len(questions), name)

        # Add e2e test questions
        e2e_questions = [
            "How many batches are currently active?",
            "How many lectures were completed in December 2025?",
            "What is the total revenue collected in January 2026?",
            "Show me the top 10 batches by student enrollment count",
            "What is the average number of lectures per batch for batches that started in 2025?",
            "Which faculty members conducted the most lectures in the last 3 months?",
        ]
        for i, q in enumerate(e2e_questions):
            questions.append({
                "id": f"e2e_{i}",
                "question": q,
                "category": "e2e",
                "difficulty": "SIMPLE" if i < 3 else "COMPLEX",
            })

    if not questions:
        logger.error("No questions loaded!")
        return CalibrationReport()

    # ── Limit if requested ─────────────────────────────────────────
    if args.limit and args.limit < len(questions):
        questions = questions[: args.limit]
        logger.info("Limited to %d questions", args.limit)

    logger.info("Running calibration with %d questions...", len(questions))

    # ── Prepare runner ─────────────────────────────────────────────
    pipeline = None
    if not args.api:
        from src.raven.connectors.openai_client import OpenAIClient
        from src.raven.connectors.pgvector_store import PgVectorStore
        from src.raven.connectors.trino_connector import TrinoConnector
        from src.raven.pipeline import Pipeline

        logger.info("Initializing direct pipeline connectors...")
        trino = TrinoConnector(
            host=os.getenv("TRINO_HOST", ""),
            port=int(os.getenv("TRINO_PORT", "443")),
            user=os.getenv("TRINO_USER", ""),
            password=os.getenv("TRINO_PASSWORD", ""),
            catalog=os.getenv("TRINO_CATALOG", "cdp"),
        )
        pgvector = PgVectorStore(
            host=os.getenv("PGVECTOR_HOST", ""),
            port=int(os.getenv("PGVECTOR_PORT", "5432")),
            dbname=os.getenv("PGVECTOR_DB", ""),
            user=os.getenv("PGVECTOR_USER", ""),
            password=os.getenv("PGVECTOR_PASSWORD", ""),
        )
        openai_client = OpenAIClient()
        pipeline = Pipeline(trino=trino, pgvector=pgvector, openai=openai_client)
        logger.info("Pipeline initialized successfully")

    # ── Run queries sequentially ───────────────────────────────────
    samples: list[CalibrationSample] = []
    total = len(questions)

    for i, q in enumerate(questions):
        qid = q["id"]
        question = q["question"]
        cat = q["category"]
        diff = q["difficulty"]

        progress = f"[{i + 1}/{total}]"
        logger.info("%s Q%s: %s", progress, qid, question[:60])

        if args.api:
            sample = await run_via_api(args.api, question, qid, cat, diff)
        else:
            assert pipeline is not None
            sample = await run_via_pipeline(pipeline, question, qid, cat, diff)

        # Re-score with the confidence model for detailed breakdown
        sample = rescore_sample(sample)

        icon = "✅" if sample.execution_success else ("🟡" if sample.status == "ambiguous" else "❌")
        logger.info(
            "%s  %s band=%s score=%.3f rows=%d lat=%.1fs",
            progress,
            icon,
            sample.confidence_band,
            sample.confidence_score,
            sample.row_count,
            sample.latency_s,
        )

        samples.append(sample)

        # Small delay to avoid overwhelming the system
        if i < total - 1:
            await asyncio.sleep(0.5)

    # ── Analyze ────────────────────────────────────────────────────
    logger.info("Analyzing %d samples...", len(samples))
    report = analyze(samples)

    return report


def main():
    parser = argparse.ArgumentParser(
        description="RAVEN Confidence Model Calibration",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--api",
        type=str,
        default=None,
        help="API base URL (e.g. http://localhost:8000). If omitted, uses direct pipeline.",
    )
    parser.add_argument(
        "--test-set",
        nargs="+",
        help="Paths to test set JSON files",
    )
    parser.add_argument(
        "--quick",
        action="store_true",
        help="Quick mode: only business-critical questions (20)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max number of questions to run",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="data/calibration_report.json",
        help="Output path for JSON report",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Debug logging",
    )

    args = parser.parse_args()

    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(name)-25s %(levelname)-5s %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("openai").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    report = asyncio.run(run_calibration(args))

    # Print report
    print(format_report(report))

    # Save JSON report
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(asdict(report), f, indent=2, default=str)
    print(f"\n  Full report saved to {out_path}")

    # Save recommended thresholds separately
    if report.recommended_thresholds != report.current_thresholds:
        thresh_path = out_path.parent / "calibrated_thresholds.json"
        with open(thresh_path, "w") as f:
            json.dump(report.recommended_thresholds, f, indent=2)
        print(f"  Recommended thresholds saved to {thresh_path}")

    return report


if __name__ == "__main__":
    main()
