"""
Accuracy Evaluator
==================
Automated evaluation of RAVEN's SQL generation accuracy.
Compares generated SQL against test_set.json expectations:
  - Table coverage (are expected tables present?)
  - SQL pattern matching (does output match expected patterns?)
  - Difficulty routing (is the question classified correctly?)
  - Overall accuracy scoring

Usage:
    python -m tests.eval_accuracy [--test-set tests/test_set.json] [--dry-run]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_TEST_SET = Path(__file__).parent / "test_set.json"


@dataclass
class EvalResult:
    """Result for a single test case."""

    question_id: int
    question: str
    expected_difficulty: str
    actual_difficulty: str | None = None
    expected_tables: list[str] = field(default_factory=list)
    actual_tables: list[str] = field(default_factory=list)
    expected_pattern: str = ""
    generated_sql: str = ""

    # Scores
    difficulty_match: bool = False
    table_coverage: float = 0.0  # 0.0 - 1.0
    pattern_match: bool = False
    execution_success: bool = False

    # Timing
    latency_ms: float = 0.0
    error: str = ""

    @property
    def composite_score(self) -> float:
        """Weighted composite accuracy score (0-100)."""
        weights = {
            "difficulty": 10,
            "tables": 40,
            "pattern": 30,
            "execution": 20,
        }
        score = 0.0
        score += weights["difficulty"] * (1.0 if self.difficulty_match else 0.0)
        score += weights["tables"] * self.table_coverage
        score += weights["pattern"] * (1.0 if self.pattern_match else 0.0)
        score += weights["execution"] * (1.0 if self.execution_success else 0.0)
        return score


@dataclass
class EvalSummary:
    """Summary of evaluation run."""

    total: int = 0
    difficulty_accuracy: float = 0.0
    table_coverage_avg: float = 0.0
    pattern_match_rate: float = 0.0
    execution_success_rate: float = 0.0
    composite_score_avg: float = 0.0
    latency_avg_ms: float = 0.0
    latency_p95_ms: float = 0.0

    # By difficulty
    simple_score: float = 0.0
    complex_score: float = 0.0

    # By category
    category_scores: dict[str, float] = field(default_factory=dict)

    results: list[EvalResult] = field(default_factory=list)


class AccuracyEvaluator:
    """Evaluate pipeline accuracy against a test set."""

    def __init__(self, pipeline: Any = None, test_set_path: str | Path | None = None):
        """
        Args:
            pipeline: A Pipeline instance (or None for dry-run mode).
            test_set_path: Path to test_set.json.
        """
        self.pipeline = pipeline
        self.test_set = self._load_test_set(test_set_path or DEFAULT_TEST_SET)

    async def run(self, dry_run: bool = False) -> EvalSummary:
        """
        Run full evaluation.

        Args:
            dry_run: If True, only validate test set structure without pipeline calls.

        Returns:
            EvalSummary with all results and aggregate scores.
        """
        results: list[EvalResult] = []

        for case in self.test_set:
            result = EvalResult(
                question_id=case["id"],
                question=case["question"],
                expected_difficulty=case.get("difficulty", "UNKNOWN"),
                expected_tables=case.get("expected_tables", []),
                expected_pattern=case.get("expected_sql_pattern", ""),
            )

            if dry_run:
                result.difficulty_match = True  # Assume correct in dry run
                result.table_coverage = 1.0
                result.pattern_match = True
                result.execution_success = True
            else:
                await self._evaluate_case(case, result)

            results.append(result)

        return self._compute_summary(results)

    async def _evaluate_case(self, case: dict, result: EvalResult) -> None:
        """Evaluate a single test case against the pipeline."""
        if not self.pipeline:
            result.error = "No pipeline configured"
            return

        try:
            start = time.monotonic()
            response = await self.pipeline.generate(case["question"])
            result.latency_ms = (time.monotonic() - start) * 1000

            result.generated_sql = response.get("sql", "")
            result.actual_difficulty = response.get("difficulty", "")

            # Check difficulty routing
            result.difficulty_match = (
                result.actual_difficulty.upper() == result.expected_difficulty.upper()
            )

            # Check table coverage
            result.actual_tables = self._extract_tables(result.generated_sql)
            result.table_coverage = self._compute_table_coverage(
                result.expected_tables, result.actual_tables,
            )

            # Check SQL pattern
            if result.expected_pattern:
                result.pattern_match = bool(re.search(
                    result.expected_pattern, result.generated_sql,
                    re.IGNORECASE | re.DOTALL,
                ))

            # Check execution success
            result.execution_success = response.get("status") == "success"

        except Exception as e:
            result.error = str(e)
            logger.error("Eval case %d failed: %s", case["id"], e)

    def _compute_summary(self, results: list[EvalResult]) -> EvalSummary:
        """Compute aggregate summary from individual results."""
        summary = EvalSummary(total=len(results), results=results)

        if not results:
            return summary

        # Aggregate scores
        summary.difficulty_accuracy = (
            sum(1 for r in results if r.difficulty_match) / len(results) * 100
        )
        summary.table_coverage_avg = (
            sum(r.table_coverage for r in results) / len(results) * 100
        )
        summary.pattern_match_rate = (
            sum(1 for r in results if r.pattern_match) / len(results) * 100
        )
        summary.execution_success_rate = (
            sum(1 for r in results if r.execution_success) / len(results) * 100
        )
        summary.composite_score_avg = (
            sum(r.composite_score for r in results) / len(results)
        )

        # Latency
        latencies = [r.latency_ms for r in results if r.latency_ms > 0]
        if latencies:
            summary.latency_avg_ms = sum(latencies) / len(latencies)
            sorted_lat = sorted(latencies)
            p95_idx = int(len(sorted_lat) * 0.95)
            summary.latency_p95_ms = sorted_lat[min(p95_idx, len(sorted_lat) - 1)]

        # By difficulty
        simple = [r for r in results if r.expected_difficulty == "SIMPLE"]
        complex_ = [r for r in results if r.expected_difficulty == "COMPLEX"]
        if simple:
            summary.simple_score = sum(r.composite_score for r in simple) / len(simple)
        if complex_:
            summary.complex_score = sum(r.composite_score for r in complex_) / len(complex_)

        # By category
        categories: dict[str, list[float]] = {}
        for case, result in zip(self.test_set, results):
            cat = case.get("category", "unknown")
            categories.setdefault(cat, []).append(result.composite_score)
        for cat, scores in categories.items():
            summary.category_scores[cat] = sum(scores) / len(scores)

        return summary

    @staticmethod
    def _extract_tables(sql: str) -> list[str]:
        """Extract table references from SQL (heuristic)."""
        tables: list[str] = []
        # Match catalog.schema.table pattern
        pattern = r"\b(\w+\.\w+\.\w+)\b"
        for match in re.finditer(pattern, sql):
            table = match.group(1)
            # Filter out common false positives
            if not any(kw in table.upper() for kw in ["ROWS.", "DATE.", "CURRENT."]):
                tables.append(table)
        return list(set(tables))

    @staticmethod
    def _compute_table_coverage(expected: list[str], actual: list[str]) -> float:
        """Compute table coverage score (recall)."""
        if not expected:
            return 1.0
        expected_set = {t.lower() for t in expected}
        actual_set = {t.lower() for t in actual}
        hits = len(expected_set & actual_set)
        return hits / len(expected_set)

    @staticmethod
    def _load_test_set(path: str | Path) -> list[dict]:
        """Load test set JSON."""
        path = Path(path)
        if not path.exists():
            logger.error("Test set not found: %s", path)
            return []
        try:
            data = json.loads(path.read_text())
            logger.info("Loaded %d test cases from %s", len(data), path)
            return data
        except Exception as e:
            logger.error("Failed to load test set: %s", e)
            return []


def format_report(summary: EvalSummary) -> str:
    """Format evaluation summary as a readable report."""
    lines = [
        "=" * 60,
        "RAVEN Accuracy Evaluation Report",
        "=" * 60,
        f"Total test cases:          {summary.total}",
        f"Composite score (avg):     {summary.composite_score_avg:.1f} / 100",
        "",
        "── Breakdown ──────────────────────────────────",
        f"Difficulty routing:         {summary.difficulty_accuracy:.1f}%",
        f"Table coverage (avg):       {summary.table_coverage_avg:.1f}%",
        f"SQL pattern match rate:     {summary.pattern_match_rate:.1f}%",
        f"Execution success rate:     {summary.execution_success_rate:.1f}%",
        "",
        "── By Difficulty ──────────────────────────────",
        f"SIMPLE score:               {summary.simple_score:.1f}",
        f"COMPLEX score:              {summary.complex_score:.1f}",
        "",
        "── By Category ────────────────────────────────",
    ]
    for cat, score in sorted(summary.category_scores.items(), key=lambda x: -x[1]):
        lines.append(f"  {cat:24s}  {score:.1f}")

    if summary.latency_avg_ms > 0:
        lines.extend([
            "",
            "── Latency ────────────────────────────────────",
            f"Average:                    {summary.latency_avg_ms:.0f} ms",
            f"P95:                        {summary.latency_p95_ms:.0f} ms",
        ])

    lines.append("=" * 60)
    return "\n".join(lines)


async def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="RAVEN Accuracy Evaluator")
    parser.add_argument(
        "--test-set", type=str, default=str(DEFAULT_TEST_SET),
        help="Path to test_set.json",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Validate test set without running pipeline",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    evaluator = AccuracyEvaluator(test_set_path=args.test_set)
    summary = await evaluator.run(dry_run=args.dry_run)

    print(format_report(summary))

    # Exit with non-zero if composite score below target (70%)
    if summary.composite_score_avg < 70 and not args.dry_run:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
