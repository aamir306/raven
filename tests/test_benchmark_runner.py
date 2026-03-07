"""Tests for BenchmarkRunner (unit-level, no pipeline calls)."""

from __future__ import annotations

from src.raven.eval.benchmark_runner import BenchmarkResult, BenchmarkRunner, MetricDelta


def test_metric_delta_regression():
    d = MetricDelta(name="pass_rate", baseline=80.0, current=77.0, threshold=2.0)
    assert d.delta == -3.0
    assert d.regression is True


def test_metric_delta_improvement():
    d = MetricDelta(name="pass_rate", baseline=80.0, current=85.0, threshold=2.0)
    assert d.delta == 5.0
    assert d.regression is False


def test_metric_delta_within_threshold():
    d = MetricDelta(name="pass_rate", baseline=80.0, current=79.0, threshold=2.0)
    assert d.delta == -1.0
    assert d.regression is False


def test_benchmark_result_to_dict():
    result = BenchmarkResult(
        timestamp="2025-01-01T00:00:00",
        passed=True,
        total_questions=10,
        pass_rate=85.0,
    )
    d = result.to_dict()
    assert d["timestamp"] == "2025-01-01T00:00:00"
    assert d["pass_rate"] == 85.0
    assert d["passed"] is True


def test_benchmark_report_format():
    result = BenchmarkResult(
        timestamp="2025-01-01T00:00:00",
        passed=True,
        total_questions=50,
        pass_rate=82.5,
        execution_success_rate=90.0,
        table_coverage_avg=75.0,
        composite_score_avg=70.0,
        latency_avg_s=2.5,
        total_cost_usd=0.0123,
        confidence_distribution={"HIGH": 20, "MEDIUM": 25, "LOW": 5},
    )
    report = BenchmarkRunner.format_report(result)
    assert "82.5%" in report
    assert "PASSED" in report
    assert "HIGH" in report


def test_benchmark_report_with_regressions():
    result = BenchmarkResult(
        timestamp="2025-01-01T00:00:00",
        passed=False,
        total_questions=50,
        pass_rate=75.0,
        regressions=[
            MetricDelta(name="pass_rate", baseline=80.0, current=75.0, threshold=2.0),
        ],
    )
    report = BenchmarkRunner.format_report(result)
    assert "FAILED" in report
    assert "REGRESSIONS" in report
