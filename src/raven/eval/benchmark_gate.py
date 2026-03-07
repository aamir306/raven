#!/usr/bin/env python3
"""
Benchmark Gate — CI-friendly offline regression check.

Compares a benchmark results JSON against the stored baseline,
applies configurable thresholds, and exits non-zero on regression.

This script does NOT require a live Trino connection or LLM keys.
It works purely on the JSON artefacts produced by benchmark_runner.

Usage (CI):
    python -m src.raven.eval.benchmark_gate

    # With custom thresholds
    python -m src.raven.eval.benchmark_gate --max-pass-rate-drop 3.0

    # Against a specific result file
    python -m src.raven.eval.benchmark_gate --result data/benchmark_results/latest.json
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parents[3]
BASELINE_PATH = _ROOT / "data" / "benchmark_baseline.json"
RESULTS_DIR = _ROOT / "data" / "benchmark_results"
TEST_SET_DIR = _ROOT / "tests"


@dataclass
class GateCheck:
    """A single gate check comparing current vs baseline."""

    metric: str
    baseline: float
    current: float
    threshold: float  # max allowed regression (positive = drop allowed)
    direction: str = "higher_is_better"  # or "lower_is_better"

    @property
    def delta(self) -> float:
        return round(self.current - self.baseline, 4)

    @property
    def passed(self) -> bool:
        if self.direction == "lower_is_better":
            # e.g. latency: current > baseline + threshold means regression
            return self.current <= self.baseline + self.threshold
        else:
            # e.g. pass_rate: current < baseline - threshold means regression
            return self.current >= self.baseline - self.threshold


@dataclass
class GateResult:
    """Overall gate decision."""

    passed: bool
    checks: list[GateCheck]
    baseline_version: str = ""
    result_file: str = ""

    @property
    def regressions(self) -> list[GateCheck]:
        return [c for c in self.checks if not c.passed]

    @property
    def improvements(self) -> list[GateCheck]:
        return [c for c in self.checks if c.delta > 0.5 and c.direction == "higher_is_better"]


# ── Default thresholds ─────────────────────────────────────────────

DEFAULT_THRESHOLDS = {
    "pass_rate": {"threshold": 2.0, "direction": "higher_is_better"},
    "execution_success_rate": {"threshold": 2.0, "direction": "higher_is_better"},
    "exec_rate": {"threshold": 2.0, "direction": "higher_is_better"},
    "table_coverage": {"threshold": 3.0, "direction": "higher_is_better"},
    "table_coverage_avg": {"threshold": 3.0, "direction": "higher_is_better"},
    "composite_score_avg": {"threshold": 2.0, "direction": "higher_is_better"},
    "avg_latency_s": {"threshold": 5.0, "direction": "lower_is_better"},
    "latency_avg_s": {"threshold": 5.0, "direction": "lower_is_better"},
    "abstain_rate": {"threshold": 10.0, "direction": "lower_is_better"},
}


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def _find_latest_result() -> Path | None:
    """Find the most recent benchmark result JSON."""
    if not RESULTS_DIR.exists():
        return None
    results = sorted(RESULTS_DIR.glob("benchmark_*.json"), reverse=True)
    return results[0] if results else None


def _extract_metrics(data: dict[str, Any]) -> dict[str, float]:
    """Extract flat metric dict from either baseline or result format."""
    # Handle nested "metrics" key (baseline format)
    if "metrics" in data:
        metrics = dict(data["metrics"])
    else:
        metrics = {}

    # Also pull top-level metric keys (result format)
    for key in DEFAULT_THRESHOLDS:
        if key in data and key not in metrics:
            metrics[key] = float(data[key])

    return metrics


def run_gate(
    baseline_path: Path = BASELINE_PATH,
    result_path: Path | None = None,
    thresholds: dict[str, dict] | None = None,
) -> GateResult:
    """Run the gate comparison.

    Args:
        baseline_path: Path to baseline JSON
        result_path: Path to result JSON (None = latest in results dir)
        thresholds: Override thresholds per metric

    Returns:
        GateResult with pass/fail and individual checks
    """
    thresholds = {**DEFAULT_THRESHOLDS, **(thresholds or {})}

    # Load baseline
    if not baseline_path.exists():
        logger.warning("No baseline at %s — gate passes by default", baseline_path)
        return GateResult(passed=True, checks=[], baseline_version="none")

    baseline_data = _load_json(baseline_path)
    baseline_metrics = _extract_metrics(baseline_data)

    # Load result
    if result_path is None:
        result_path = _find_latest_result()
    if result_path is None or not result_path.exists():
        logger.info("No benchmark result found — gate passes (no data to compare)")
        return GateResult(passed=True, checks=[], result_file="none")

    result_data = _load_json(result_path)
    result_metrics = _extract_metrics(result_data)

    # Run checks for all metrics that exist in BOTH baseline and result
    checks: list[GateCheck] = []
    for metric_name, cfg in thresholds.items():
        if metric_name in baseline_metrics and metric_name in result_metrics:
            check = GateCheck(
                metric=metric_name,
                baseline=baseline_metrics[metric_name],
                current=result_metrics[metric_name],
                threshold=cfg["threshold"],
                direction=cfg.get("direction", "higher_is_better"),
            )
            checks.append(check)

    passed = all(c.passed for c in checks)

    return GateResult(
        passed=passed,
        checks=checks,
        baseline_version=baseline_data.get("timestamp", "unknown"),
        result_file=str(result_path),
    )


def format_gate_report(gate: GateResult) -> str:
    """Format gate result as a human-readable report."""
    lines = [
        "",
        "┌" + "─" * 66 + "┐",
        "│  RAVEN Benchmark Gate" + " " * 44 + "│",
        "├" + "─" * 66 + "┤",
        f"│  Baseline:  {gate.baseline_version[:50]:50s}" + " │",
        f"│  Result:    {Path(gate.result_file).name[:50]:50s}" + " │",
        "├" + "─" * 66 + "┤",
    ]

    for check in gate.checks:
        status = "✓" if check.passed else "✗"
        arrow = "↑" if check.delta > 0 else "↓" if check.delta < 0 else "="
        lines.append(
            f"│  {status} {check.metric:28s} "
            f"{check.baseline:>7.2f} → {check.current:>7.2f}  "
            f"{arrow} {check.delta:>+6.2f}" + " " * 3 + "│"
        )

    if gate.regressions:
        lines.append("├" + "─" * 66 + "┤")
        lines.append(
            f"│  ⚠ {len(gate.regressions)} regression(s) detected"
            + " " * (40 - len(str(len(gate.regressions))))
            + "│"
        )

    verdict = "PASSED ✓" if gate.passed else "FAILED ✗"
    lines.append("├" + "─" * 66 + "┤")
    lines.append(f"│  Gate: {verdict}" + " " * (58 - len(verdict)) + "│")
    lines.append("└" + "─" * 66 + "┘")

    return "\n".join(lines)


def validate_test_sets() -> list[str]:
    """Validate all test set JSON files. Returns errors."""
    errors = []
    for path in [
        TEST_SET_DIR / "test_set.json",
        TEST_SET_DIR / "test_set_business_critical.json",
    ]:
        if not path.exists():
            errors.append(f"Missing test set: {path}")
            continue
        try:
            data = json.loads(path.read_text())
            if not isinstance(data, list):
                errors.append(f"{path.name}: expected list, got {type(data).__name__}")
                continue
            for i, item in enumerate(data):
                if "question" not in item and "id" not in item:
                    errors.append(f"{path.name}[{i}]: missing 'question' or 'id'")
        except json.JSONDecodeError as e:
            errors.append(f"{path.name}: invalid JSON — {e}")
    return errors


def main() -> None:
    parser = argparse.ArgumentParser(description="RAVEN Benchmark Gate (offline)")
    parser.add_argument("--result", type=str, help="Path to benchmark result JSON")
    parser.add_argument("--baseline", type=str, default=str(BASELINE_PATH))
    parser.add_argument("--max-pass-rate-drop", type=float, default=2.0)
    parser.add_argument("--max-latency-increase", type=float, default=5.0)
    parser.add_argument("--validate-only", action="store_true",
                        help="Only validate test set files, skip gate check")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)-5s %(message)s",
    )

    # Always validate test sets first
    errors = validate_test_sets()
    if errors:
        for e in errors:
            print(f"  ✗ {e}")
        if args.validate_only:
            sys.exit(1)
    else:
        print("  ✓ All test sets valid")

    if args.validate_only:
        sys.exit(0)

    # Apply CLI threshold overrides
    thresholds = {**DEFAULT_THRESHOLDS}
    thresholds["pass_rate"]["threshold"] = args.max_pass_rate_drop
    thresholds["avg_latency_s"]["threshold"] = args.max_latency_increase
    thresholds["latency_avg_s"]["threshold"] = args.max_latency_increase

    result_path = Path(args.result) if args.result else None
    gate = run_gate(
        baseline_path=Path(args.baseline),
        result_path=result_path,
        thresholds=thresholds,
    )

    print(format_gate_report(gate))

    if not gate.passed:
        sys.exit(1)


if __name__ == "__main__":
    main()
