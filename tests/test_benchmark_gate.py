"""Tests for benchmark_gate.py — offline CI regression gate."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from src.raven.eval.benchmark_gate import (
    DEFAULT_THRESHOLDS,
    GateCheck,
    GateResult,
    _extract_metrics,
    format_gate_report,
    run_gate,
    validate_test_sets,
)


# ── GateCheck unit tests ──────────────────────────────────────


class TestGateCheck:
    def test_higher_is_better_pass(self):
        c = GateCheck(metric="pass_rate", baseline=80.0, current=79.0, threshold=2.0)
        assert c.passed is True
        assert c.delta == -1.0

    def test_higher_is_better_fail(self):
        c = GateCheck(metric="pass_rate", baseline=80.0, current=77.0, threshold=2.0)
        assert c.passed is False
        assert c.delta == -3.0

    def test_lower_is_better_pass(self):
        c = GateCheck(
            metric="avg_latency_s", baseline=2.0, current=3.0,
            threshold=5.0, direction="lower_is_better",
        )
        assert c.passed is True

    def test_lower_is_better_fail(self):
        c = GateCheck(
            metric="avg_latency_s", baseline=2.0, current=8.0,
            threshold=5.0, direction="lower_is_better",
        )
        assert c.passed is False

    def test_improvement_detected(self):
        c = GateCheck(metric="pass_rate", baseline=80.0, current=90.0, threshold=2.0)
        assert c.passed is True
        assert c.delta == 10.0

    def test_exact_boundary_pass(self):
        c = GateCheck(metric="pass_rate", baseline=80.0, current=78.0, threshold=2.0)
        assert c.passed is True  # exactly at boundary


# ── GateResult unit tests ─────────────────────────────────────


class TestGateResult:
    def test_all_passing(self):
        checks = [
            GateCheck("pass_rate", 80, 82, 2.0),
            GateCheck("exec_rate", 90, 89, 2.0),
        ]
        r = GateResult(passed=True, checks=checks)
        assert r.regressions == []
        assert len(r.improvements) == 1  # pass_rate improved

    def test_with_regressions(self):
        checks = [
            GateCheck("pass_rate", 80, 75, 2.0),
            GateCheck("exec_rate", 90, 89, 2.0),
        ]
        r = GateResult(passed=False, checks=checks)
        assert len(r.regressions) == 1
        assert r.regressions[0].metric == "pass_rate"


# ── _extract_metrics ──────────────────────────────────────────


class TestExtractMetrics:
    def test_baseline_format(self):
        data = {
            "metrics": {"pass_rate": 85.0, "exec_rate": 90.0},
        }
        m = _extract_metrics(data)
        assert m["pass_rate"] == 85.0
        assert m["exec_rate"] == 90.0

    def test_flat_result_format(self):
        data = {"pass_rate": 85.0, "avg_latency_s": 2.1}
        m = _extract_metrics(data)
        assert m["pass_rate"] == 85.0
        assert m["avg_latency_s"] == 2.1

    def test_nested_takes_precedence(self):
        data = {
            "metrics": {"pass_rate": 85.0},
            "pass_rate": 99.0,  # top-level should NOT override nested
        }
        m = _extract_metrics(data)
        assert m["pass_rate"] == 85.0


# ── run_gate integration ──────────────────────────────────────


class TestRunGate:
    def _write_json(self, path: Path, data: dict) -> None:
        path.write_text(json.dumps(data))

    def test_no_baseline_passes(self, tmp_path):
        """If there is no baseline file, gate passes by default."""
        result = run_gate(
            baseline_path=tmp_path / "nonexistent.json",
            result_path=tmp_path / "result.json",
        )
        assert result.passed is True
        assert result.checks == []

    def test_no_result_passes(self, tmp_path):
        """If there is no result file, gate passes (nothing to compare)."""
        baseline = tmp_path / "baseline.json"
        self._write_json(baseline, {"metrics": {"pass_rate": 80.0}})
        result = run_gate(
            baseline_path=baseline,
            result_path=tmp_path / "nonexistent.json",
        )
        assert result.passed is True

    def test_gate_passes_within_threshold(self, tmp_path):
        baseline = tmp_path / "baseline.json"
        result_f = tmp_path / "result.json"
        self._write_json(baseline, {
            "timestamp": "2025-01-01",
            "metrics": {"pass_rate": 80.0, "exec_rate": 90.0},
        })
        self._write_json(result_f, {"pass_rate": 79.0, "exec_rate": 89.0})

        gate = run_gate(baseline_path=baseline, result_path=result_f)
        assert gate.passed is True

    def test_gate_fails_on_regression(self, tmp_path):
        baseline = tmp_path / "baseline.json"
        result_f = tmp_path / "result.json"
        self._write_json(baseline, {
            "timestamp": "2025-01-01",
            "metrics": {"pass_rate": 80.0},
        })
        self._write_json(result_f, {"pass_rate": 70.0})

        gate = run_gate(baseline_path=baseline, result_path=result_f)
        assert gate.passed is False
        assert len(gate.regressions) == 1

    def test_custom_thresholds(self, tmp_path):
        baseline = tmp_path / "baseline.json"
        result_f = tmp_path / "result.json"
        self._write_json(baseline, {
            "timestamp": "2025-01-01",
            "metrics": {"pass_rate": 80.0},
        })
        self._write_json(result_f, {"pass_rate": 70.0})

        # With a large threshold, this should pass
        thresholds = {"pass_rate": {"threshold": 15.0, "direction": "higher_is_better"}}
        gate = run_gate(baseline_path=baseline, result_path=result_f, thresholds=thresholds)
        assert gate.passed is True

    def test_latency_regression(self, tmp_path):
        baseline = tmp_path / "baseline.json"
        result_f = tmp_path / "result.json"
        self._write_json(baseline, {
            "timestamp": "2025-01-01",
            "metrics": {"avg_latency_s": 2.0},
        })
        self._write_json(result_f, {"avg_latency_s": 10.0})

        gate = run_gate(baseline_path=baseline, result_path=result_f)
        assert gate.passed is False


# ── format_gate_report ────────────────────────────────────────


class TestGateReport:
    def test_report_contains_verdict(self):
        gate = GateResult(passed=True, checks=[], baseline_version="v1")
        report = format_gate_report(gate)
        assert "PASSED" in report

    def test_report_shows_regression(self):
        checks = [GateCheck("pass_rate", 80.0, 70.0, 2.0)]
        gate = GateResult(
            passed=False, checks=checks,
            baseline_version="2025-01-01", result_file="result.json",
        )
        report = format_gate_report(gate)
        assert "FAILED" in report
        assert "pass_rate" in report
        assert "regression" in report.lower()


# ── validate_test_sets ────────────────────────────────────────


class TestValidateTestSets:
    def test_validates_real_test_sets(self):
        """Ensure the project's actual test sets are valid."""
        errors = validate_test_sets()
        assert errors == [], f"Test set errors: {errors}"


# ── DEFAULT_THRESHOLDS sanity ─────────────────────────────────


def test_default_thresholds_all_have_direction():
    for metric, cfg in DEFAULT_THRESHOLDS.items():
        assert "threshold" in cfg, f"{metric} missing threshold"
        assert "direction" in cfg, f"{metric} missing direction"
        assert cfg["direction"] in ("higher_is_better", "lower_is_better")
