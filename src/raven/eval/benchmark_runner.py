"""
Benchmark Runner — Release Gate
=================================
Runs the accuracy evaluation suite, compares results against a stored
baseline, and produces a pass/fail gate for CI/CD.

Workflow:
  1. Load baseline from ``data/benchmark_baseline.json``
  2. Run evaluation via ``AccuracyEvaluator``
  3. Compare deltas across all tracked metrics
  4. Report pass/fail with regression summary
  5. Optionally update the baseline snapshot

Usage (CLI):
    # Run benchmark and compare against baseline
    python -m src.raven.eval.benchmark_runner

    # Run specific categories only
    python -m src.raven.eval.benchmark_runner --categories batch revenue

    # Update baseline with current results
    python -m src.raven.eval.benchmark_runner --update-baseline

    # Dry-run (no pipeline calls — validates test set only)
    python -m src.raven.eval.benchmark_runner --dry-run
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

# Allow running from project root
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

logger = logging.getLogger(__name__)

BASELINE_PATH = Path(__file__).resolve().parents[3] / "data" / "benchmark_baseline.json"
RESULTS_DIR = Path(__file__).resolve().parents[3] / "data" / "benchmark_results"


@dataclass
class MetricDelta:
    """Change in a single metric vs baseline."""

    name: str
    baseline: float
    current: float
    delta: float = 0.0
    regression: bool = False
    threshold: float = 0.0  # max allowed regression

    def __post_init__(self) -> None:
        self.delta = round(self.current - self.baseline, 4)
        self.regression = self.delta < -self.threshold


@dataclass
class BenchmarkResult:
    """Output of a benchmark run."""

    timestamp: str = ""
    passed: bool = True
    total_questions: int = 0
    pass_rate: float = 0.0
    execution_success_rate: float = 0.0
    table_coverage_avg: float = 0.0
    composite_score_avg: float = 0.0
    latency_avg_s: float = 0.0
    latency_p95_s: float = 0.0
    total_cost_usd: float = 0.0
    confidence_distribution: dict[str, int] = field(default_factory=dict)
    regressions: list[MetricDelta] = field(default_factory=list)
    improvements: list[MetricDelta] = field(default_factory=list)
    category_scores: dict[str, dict] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["regressions"] = [asdict(r) for r in self.regressions]
        data["improvements"] = [asdict(i) for i in self.improvements]
        return data


# ── Gate thresholds (percentage points of allowed regression) ──────────

DEFAULT_GATE_THRESHOLDS = {
    "pass_rate": 2.0,               # Allow max 2pp drop in pass rate
    "execution_success_rate": 2.0,
    "table_coverage_avg": 3.0,
    "composite_score_avg": 2.0,
    "latency_avg_s": -5.0,          # Negative = higher is worse (allow 5s increase)
}


class BenchmarkRunner:
    """Run accuracy benchmarks and gate releases on regression."""

    def __init__(
        self,
        baseline_path: str | Path | None = None,
        gate_thresholds: dict[str, float] | None = None,
    ):
        self._baseline_path = Path(baseline_path or BASELINE_PATH)
        self._thresholds = {**DEFAULT_GATE_THRESHOLDS, **(gate_thresholds or {})}
        self._baseline = self._load_baseline()

    async def run(
        self,
        dry_run: bool = False,
        sample: int | None = None,
        categories: list[str] | None = None,
        test_set_path: str | None = None,
    ) -> BenchmarkResult:
        """
        Run benchmark evaluation and compare to baseline.

        Returns BenchmarkResult with pass/fail gate status.
        """
        # Late import to avoid circular deps at module level
        from tests.eval_accuracy import AccuracyEvaluator, create_pipeline

        pipeline = None if dry_run else create_pipeline()
        evaluator = AccuracyEvaluator(pipeline=pipeline, test_set_path=test_set_path)

        summary = await evaluator.run(
            dry_run=dry_run,
            sample=sample,
            categories=categories,
        )

        result = BenchmarkResult(
            timestamp=time.strftime("%Y-%m-%dT%H:%M:%S"),
            total_questions=summary.total,
            pass_rate=summary.pass_rate,
            execution_success_rate=summary.execution_success_rate,
            table_coverage_avg=summary.table_coverage_avg,
            composite_score_avg=summary.composite_score_avg,
            latency_avg_s=summary.latency_avg_s,
            latency_p95_s=summary.latency_p95_s,
            total_cost_usd=summary.total_cost_usd,
            category_scores=summary.category_scores,
        )

        # Confidence distribution
        for r in summary.results:
            band = (r.confidence or "UNKNOWN").upper()
            result.confidence_distribution[band] = (
                result.confidence_distribution.get(band, 0) + 1
            )

        # Compare to baseline
        if self._baseline:
            self._compare(result)

        # Save results
        self._save_result(result)

        return result

    def update_baseline(self, result: BenchmarkResult) -> None:
        """Persist current results as the new baseline."""
        self._baseline_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "timestamp": result.timestamp,
            "pass_rate": result.pass_rate,
            "execution_success_rate": result.execution_success_rate,
            "table_coverage_avg": result.table_coverage_avg,
            "composite_score_avg": result.composite_score_avg,
            "latency_avg_s": result.latency_avg_s,
            "category_scores": result.category_scores,
        }
        self._baseline_path.write_text(json.dumps(payload, indent=2))
        logger.info("Baseline updated at %s", self._baseline_path)

    # ── Reporting ──────────────────────────────────────────────────────

    @staticmethod
    def format_report(result: BenchmarkResult) -> str:
        lines = [
            "",
            "╔" + "═" * 68 + "╗",
            "║  RAVEN Benchmark Report" + " " * 44 + "║",
            "╠" + "═" * 68 + "╣",
            f"║  Questions:          {result.total_questions:>6d}" + " " * 35 + "║",
            f"║  Pass rate:          {result.pass_rate:>6.1f}%" + " " * 34 + "║",
            f"║  Exec success rate:  {result.execution_success_rate:>6.1f}%" + " " * 34 + "║",
            f"║  Table coverage:     {result.table_coverage_avg:>6.1f}%" + " " * 34 + "║",
            f"║  Composite score:    {result.composite_score_avg:>6.1f}" + " " * 35 + "║",
            f"║  Avg latency:        {result.latency_avg_s:>6.1f}s" + " " * 34 + "║",
            f"║  Total cost:         ${result.total_cost_usd:>7.4f}" + " " * 33 + "║",
        ]

        if result.confidence_distribution:
            lines.append("╠" + "─" * 68 + "╣")
            lines.append("║  Confidence Distribution:" + " " * 42 + "║")
            for band in ["HIGH", "MEDIUM", "LOW", "ABSTAIN"]:
                count = result.confidence_distribution.get(band, 0)
                if count:
                    pct = count / max(result.total_questions, 1) * 100
                    lines.append(f"║    {band:8s} {count:>4d} ({pct:>5.1f}%)" + " " * 40 + "║")

        if result.regressions:
            lines.append("╠" + "─" * 68 + "╣")
            lines.append("║  ⚠ REGRESSIONS:" + " " * 51 + "║")
            for r in result.regressions:
                lines.append(
                    f"║    {r.name:25s} {r.baseline:>6.1f} → {r.current:>6.1f}  "
                    f"({r.delta:>+.1f})" + " " * 17 + "║"
                )

        if result.improvements:
            lines.append("╠" + "─" * 68 + "╣")
            lines.append("║  ✓ Improvements:" + " " * 50 + "║")
            for i in result.improvements:
                lines.append(
                    f"║    {i.name:25s} {i.baseline:>6.1f} → {i.current:>6.1f}  "
                    f"({i.delta:>+.1f})" + " " * 17 + "║"
                )

        gate = "PASSED ✓" if result.passed else "FAILED ✗"
        lines.append("╠" + "═" * 68 + "╣")
        lines.append(f"║  Gate: {gate}" + " " * (60 - len(gate)) + "║")
        lines.append("╚" + "═" * 68 + "╝")

        return "\n".join(lines)

    # ── Internal ───────────────────────────────────────────────────────

    def _compare(self, result: BenchmarkResult) -> None:
        """Compare result against baseline and populate regressions/improvements."""
        metrics = [
            ("pass_rate", result.pass_rate),
            ("execution_success_rate", result.execution_success_rate),
            ("table_coverage_avg", result.table_coverage_avg),
            ("composite_score_avg", result.composite_score_avg),
        ]

        for name, current in metrics:
            baseline_val = self._baseline.get(name, 0.0)
            threshold = self._thresholds.get(name, 2.0)
            delta = MetricDelta(
                name=name,
                baseline=baseline_val,
                current=current,
                threshold=threshold,
            )
            if delta.regression:
                result.regressions.append(delta)
                result.passed = False
            elif delta.delta > 0.5:
                result.improvements.append(delta)

        # Latency is inverse — higher is worse
        baseline_latency = self._baseline.get("latency_avg_s", 0.0)
        if baseline_latency > 0 and result.latency_avg_s > 0:
            lat_delta = result.latency_avg_s - baseline_latency
            threshold = abs(self._thresholds.get("latency_avg_s", -5.0))
            if lat_delta > threshold:
                result.regressions.append(MetricDelta(
                    name="latency_avg_s",
                    baseline=baseline_latency,
                    current=result.latency_avg_s,
                    threshold=0.0,
                ))
                result.regressions[-1].regression = True
                result.passed = False

    def _load_baseline(self) -> dict[str, Any]:
        if not self._baseline_path.exists():
            logger.info("No baseline found at %s — skipping comparison", self._baseline_path)
            return {}
        try:
            data = json.loads(self._baseline_path.read_text())
            logger.info("Loaded baseline from %s (timestamp: %s)", self._baseline_path, data.get("timestamp"))
            return data
        except Exception as e:
            logger.warning("Failed to load baseline: %s", e)
            return {}

    def _save_result(self, result: BenchmarkResult) -> None:
        RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        filename = f"benchmark_{result.timestamp.replace(':', '-')}.json"
        path = RESULTS_DIR / filename
        path.write_text(json.dumps(result.to_dict(), indent=2, default=str))
        logger.info("Benchmark result saved to %s", path)


async def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="RAVEN Benchmark Runner")
    parser.add_argument("--dry-run", action="store_true", help="Validate only, no pipeline calls")
    parser.add_argument("--sample", type=int, help="Random sample size")
    parser.add_argument("--categories", nargs="+", help="Filter by categories")
    parser.add_argument("--test-set", type=str, help="Path to test set JSON")
    parser.add_argument("--update-baseline", action="store_true", help="Update baseline with current results")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-5s %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("openai").setLevel(logging.WARNING)

    runner = BenchmarkRunner()
    result = await runner.run(
        dry_run=args.dry_run,
        sample=args.sample,
        categories=args.categories,
        test_set_path=args.test_set,
    )

    print(BenchmarkRunner.format_report(result))

    if args.update_baseline:
        runner.update_baseline(result)
        print("\n  Baseline updated.")

    if not result.passed:
        print("\n  ⚠ Benchmark gate FAILED — regressions detected.")
        sys.exit(1)
    else:
        print("\n  ✓ Benchmark gate PASSED.")


if __name__ == "__main__":
    asyncio.run(main())
