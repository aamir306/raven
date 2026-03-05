"""
Accuracy Evaluator
==================
Automated evaluation of RAVEN's SQL generation accuracy against the
200-question PW CDP test set.

Supports:
  - Full 200-question or sampled subset evaluation
  - Table coverage scoring (recall of expected tables in generated SQL)
  - Execution success rate (did Trino return rows?)
  - Difficulty routing accuracy
  - Per-category breakdown (batch, revenue, enrollment, etc.)
  - Failure mode analysis with examples
  - JSON results output for tracking over time

Usage:
    # Run all 200 questions (full eval — ~3hrs at 60s/q)
    python tests/eval_accuracy.py --test-set tests/test_set_200.json

    # Sample 20 random questions
    python tests/eval_accuracy.py --sample 20

    # Run specific IDs
    python tests/eval_accuracy.py --ids 1 5 10 25 50

    # Run specific categories
    python tests/eval_accuracy.py --categories batch revenue enrollment

    # Dry run (validate test set only)
    python tests/eval_accuracy.py --dry-run
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import random
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv
load_dotenv()

logger = logging.getLogger(__name__)

DEFAULT_TEST_SET = Path(__file__).parent / "test_set_200.json"


@dataclass
class EvalResult:
    """Result for a single test case."""

    question_id: int
    question: str
    category: str = ""
    expected_difficulty: str = ""
    actual_difficulty: str | None = None
    expected_tables: list[str] = field(default_factory=list)
    actual_tables: list[str] = field(default_factory=list)
    generated_sql: str = ""
    notes: str = ""

    # Scores
    difficulty_match: bool = False
    table_coverage: float = 0.0  # 0.0 - 1.0
    execution_success: bool = False
    row_count: int = 0

    # Metadata
    latency_s: float = 0.0
    cost_usd: float = 0.0
    confidence: str = ""
    summary: str = ""
    error: str = ""
    cached: bool = False

    @property
    def passed(self) -> bool:
        """A question passes if SQL executed successfully and returned rows."""
        return self.execution_success and self.row_count > 0

    @property
    def composite_score(self) -> float:
        """Weighted composite accuracy score (0-100)."""
        score = 0.0
        score += 10 * (1.0 if self.difficulty_match else 0.0)
        score += 40 * self.table_coverage
        score += 50 * (1.0 if self.execution_success else 0.0)
        return score


@dataclass
class EvalSummary:
    """Summary of evaluation run."""

    total: int = 0
    execution_success_rate: float = 0.0
    pass_rate: float = 0.0
    difficulty_accuracy: float = 0.0
    table_coverage_avg: float = 0.0
    composite_score_avg: float = 0.0
    latency_avg_s: float = 0.0
    latency_p95_s: float = 0.0
    total_cost_usd: float = 0.0

    # By difficulty
    simple_pass_rate: float = 0.0
    complex_pass_rate: float = 0.0

    # By category
    category_scores: dict[str, dict] = field(default_factory=dict)

    # Failures
    failures: list[dict] = field(default_factory=list)

    results: list[EvalResult] = field(default_factory=list)


class AccuracyEvaluator:
    """Evaluate pipeline accuracy against a test set."""

    def __init__(self, pipeline: Any = None, test_set_path: str | Path | None = None):
        self.pipeline = pipeline
        self.test_set = self._load_test_set(test_set_path or DEFAULT_TEST_SET)

    async def run(
        self,
        dry_run: bool = False,
        sample: int | None = None,
        ids: list[int] | None = None,
        categories: list[str] | None = None,
    ) -> EvalSummary:
        """
        Run evaluation.

        Args:
            dry_run: Validate test set without pipeline calls.
            sample: Random sample size (None = all).
            ids: Specific question IDs to run.
            categories: Only run questions in these categories.
        """
        cases = self.test_set

        # Filter by categories
        if categories:
            cases = [c for c in cases if c.get("category") in categories]

        # Filter by IDs
        if ids:
            id_set = set(ids)
            cases = [c for c in cases if c["id"] in id_set]

        # Random sample
        if sample and sample < len(cases):
            cases = random.sample(cases, sample)

        logger.info("Running %d test cases (of %d total)", len(cases), len(self.test_set))

        results: list[EvalResult] = []

        for i, case in enumerate(cases):
            result = EvalResult(
                question_id=case["id"],
                question=case["question"],
                category=case.get("category", ""),
                expected_difficulty=case.get("difficulty", "UNKNOWN"),
                expected_tables=case.get("expected_tables", []),
                notes=case.get("notes", ""),
            )

            if dry_run:
                result.difficulty_match = True
                result.table_coverage = 1.0
                result.execution_success = True
                result.row_count = 1
            else:
                await self._evaluate_case(case, result)

            results.append(result)

            # Progress output
            status_icon = "✅" if result.passed else ("⚠️" if result.execution_success else "❌")
            logger.info(
                "[%d/%d] Q%d %s  %.1fs  tables=%.0f%%  %s",
                i + 1, len(cases), result.question_id, status_icon,
                result.latency_s, result.table_coverage * 100,
                result.question[:60],
            )

        return self._compute_summary(results)

    async def _evaluate_case(self, case: dict, result: EvalResult) -> None:
        """Evaluate a single test case against the pipeline."""
        if not self.pipeline:
            result.error = "No pipeline configured"
            return

        try:
            start = time.monotonic()
            response = await self.pipeline.generate(case["question"])
            result.latency_s = time.monotonic() - start

            result.generated_sql = response.get("sql", "")
            result.actual_difficulty = response.get("difficulty", "")
            result.confidence = str(response.get("confidence", ""))
            result.summary = response.get("summary", "")
            result.row_count = response.get("row_count", 0) or 0
            result.cost_usd = response.get("cost", 0.0) or 0.0
            result.cached = response.get("cached", False)
            result.error = response.get("error", "")

            # Difficulty routing
            if result.actual_difficulty:
                result.difficulty_match = (
                    result.actual_difficulty.upper() == result.expected_difficulty.upper()
                )

            # Table coverage (recall)
            result.actual_tables = self._extract_tables(result.generated_sql)
            result.table_coverage = self._compute_table_coverage(
                result.expected_tables, result.actual_tables,
            )

            # Execution success
            result.execution_success = response.get("status") == "success"

        except Exception as e:
            result.error = str(e)
            logger.error("Eval case Q%d failed: %s", case["id"], e)

    def _compute_summary(self, results: list[EvalResult]) -> EvalSummary:
        """Compute aggregate summary from individual results."""
        summary = EvalSummary(total=len(results), results=results)
        if not results:
            return summary

        # Aggregate scores
        summary.execution_success_rate = (
            sum(1 for r in results if r.execution_success) / len(results) * 100
        )
        summary.pass_rate = (
            sum(1 for r in results if r.passed) / len(results) * 100
        )
        summary.difficulty_accuracy = (
            sum(1 for r in results if r.difficulty_match) / len(results) * 100
        )
        summary.table_coverage_avg = (
            sum(r.table_coverage for r in results) / len(results) * 100
        )
        summary.composite_score_avg = (
            sum(r.composite_score for r in results) / len(results)
        )

        # Cost
        summary.total_cost_usd = sum(r.cost_usd for r in results)

        # Latency
        latencies = [r.latency_s for r in results if r.latency_s > 0]
        if latencies:
            summary.latency_avg_s = sum(latencies) / len(latencies)
            sorted_lat = sorted(latencies)
            p95_idx = int(len(sorted_lat) * 0.95)
            summary.latency_p95_s = sorted_lat[min(p95_idx, len(sorted_lat) - 1)]

        # By difficulty
        simple = [r for r in results if r.expected_difficulty == "SIMPLE"]
        complex_ = [r for r in results if r.expected_difficulty == "COMPLEX"]
        if simple:
            summary.simple_pass_rate = sum(1 for r in simple if r.passed) / len(simple) * 100
        if complex_:
            summary.complex_pass_rate = sum(1 for r in complex_ if r.passed) / len(complex_) * 100

        # By category
        cat_map: dict[str, list[EvalResult]] = {}
        for r in results:
            cat_map.setdefault(r.category or "unknown", []).append(r)
        for cat, cat_results in cat_map.items():
            n = len(cat_results)
            passed = sum(1 for r in cat_results if r.passed)
            exec_ok = sum(1 for r in cat_results if r.execution_success)
            summary.category_scores[cat] = {
                "total": n,
                "passed": passed,
                "pass_rate": passed / n * 100 if n else 0,
                "exec_rate": exec_ok / n * 100 if n else 0,
                "avg_latency": sum(r.latency_s for r in cat_results) / n if n else 0,
            }

        # Collect failures
        for r in results:
            if not r.passed:
                summary.failures.append({
                    "id": r.question_id,
                    "question": r.question,
                    "category": r.category,
                    "difficulty": r.expected_difficulty,
                    "error": r.error[:200] if r.error else "",
                    "execution_success": r.execution_success,
                    "table_coverage": r.table_coverage,
                    "sql": r.generated_sql[:300] if r.generated_sql else "",
                    "expected_tables": r.expected_tables,
                    "actual_tables": r.actual_tables,
                })

        return summary

    @staticmethod
    def _extract_tables(sql: str) -> list[str]:
        """Extract table references from SQL (heuristic)."""
        tables: list[str] = []
        pattern = r"\b(\w+\.\w+\.\w+)\b"
        for match in re.finditer(pattern, sql):
            table = match.group(1)
            if not any(kw in table.upper() for kw in ["ROWS.", "DATE.", "CURRENT.", "WITH."]):
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
        "",
        "═" * 70,
        "  RAVEN Accuracy Evaluation Report",
        "═" * 70,
        f"  Test cases:              {summary.total}",
        f"  Pass rate:               {summary.pass_rate:.1f}% ({sum(1 for r in summary.results if r.passed)}/{summary.total})",
        f"  Execution success rate:  {summary.execution_success_rate:.1f}%",
        f"  Difficulty accuracy:     {summary.difficulty_accuracy:.1f}%",
        f"  Table coverage (avg):    {summary.table_coverage_avg:.1f}%",
        f"  Composite score (avg):   {summary.composite_score_avg:.1f} / 100",
        "",
        "── By Difficulty ──────────────────────────────────────────",
        f"  SIMPLE pass rate:        {summary.simple_pass_rate:.1f}%",
        f"  COMPLEX pass rate:       {summary.complex_pass_rate:.1f}%",
        "",
        "── By Category ────────────────────────────────────────────",
    ]
    for cat, stats in sorted(summary.category_scores.items(), key=lambda x: -x[1]["pass_rate"]):
        lines.append(
            f"  {cat:20s}  {stats['passed']:2d}/{stats['total']:2d} = {stats['pass_rate']:5.1f}%  "
            f"avg {stats['avg_latency']:.0f}s"
        )

    if summary.latency_avg_s > 0:
        lines.extend([
            "",
            "── Latency & Cost ─────────────────────────────────────────",
            f"  Average latency:         {summary.latency_avg_s:.1f}s",
            f"  P95 latency:             {summary.latency_p95_s:.1f}s",
            f"  Total cost:              ${summary.total_cost_usd:.4f}",
            f"  Avg cost/query:          ${summary.total_cost_usd / max(summary.total, 1):.4f}",
        ])

    if summary.failures:
        lines.extend([
            "",
            "── Top Failures ───────────────────────────────────────────",
        ])
        for f in summary.failures[:10]:
            lines.append(
                f"  Q{f['id']:3d} [{f['category']:12s}] {f['difficulty']:7s}  "
                f"tables={f['table_coverage']:.0%}  {f['question'][:50]}"
            )
            if f["error"]:
                lines.append(f"       → {f['error'][:80]}")

    lines.append("═" * 70)
    return "\n".join(lines)


def create_pipeline():
    """Create pipeline with real connectors (same as e2e_test.py)."""
    from src.raven.connectors.openai_client import OpenAIClient
    from src.raven.connectors.pgvector_store import PgVectorStore
    from src.raven.connectors.trino_connector import TrinoConnector
    from src.raven.pipeline import Pipeline

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
    return Pipeline(trino=trino, pgvector=pgvector, openai=openai_client)


async def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="RAVEN Accuracy Evaluator")
    parser.add_argument("--test-set", type=str, default=str(DEFAULT_TEST_SET))
    parser.add_argument("--dry-run", action="store_true", help="Validate test set only")
    parser.add_argument("--sample", type=int, help="Random sample size")
    parser.add_argument("--ids", nargs="+", type=int, help="Specific question IDs")
    parser.add_argument("--categories", nargs="+", help="Filter by categories")
    parser.add_argument("--output", type=str, default="data/eval_results.json", help="Output JSON path")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)-5s %(message)s", datefmt="%H:%M:%S")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("openai").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    pipeline = None if args.dry_run else create_pipeline()

    evaluator = AccuracyEvaluator(pipeline=pipeline, test_set_path=args.test_set)
    summary = await evaluator.run(
        dry_run=args.dry_run,
        sample=args.sample,
        ids=args.ids,
        categories=args.categories,
    )

    print(format_report(summary))

    # Save results
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_data = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "total": summary.total,
        "pass_rate": summary.pass_rate,
        "execution_success_rate": summary.execution_success_rate,
        "difficulty_accuracy": summary.difficulty_accuracy,
        "table_coverage_avg": summary.table_coverage_avg,
        "composite_score_avg": summary.composite_score_avg,
        "latency_avg_s": summary.latency_avg_s,
        "total_cost_usd": summary.total_cost_usd,
        "simple_pass_rate": summary.simple_pass_rate,
        "complex_pass_rate": summary.complex_pass_rate,
        "category_scores": summary.category_scores,
        "failures": summary.failures,
        "results": [
            {
                "id": r.question_id,
                "question": r.question,
                "category": r.category,
                "expected_difficulty": r.expected_difficulty,
                "actual_difficulty": r.actual_difficulty,
                "passed": r.passed,
                "execution_success": r.execution_success,
                "table_coverage": r.table_coverage,
                "row_count": r.row_count,
                "latency_s": round(r.latency_s, 2),
                "cost_usd": round(r.cost_usd, 4),
                "sql": r.generated_sql[:500],
                "error": r.error[:200],
            }
            for r in summary.results
        ],
    }
    with open(out_path, "w") as f:
        json.dump(out_data, f, indent=2)
    logger.info("Results saved to %s", out_path)

    # Exit with non-zero if pass rate below 60%
    if summary.pass_rate < 60 and not args.dry_run:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
