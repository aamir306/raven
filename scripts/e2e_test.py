#!/usr/bin/env python3
"""
RAVEN — End-to-End Pipeline Test
==================================
Runs a set of natural-language questions through the full 8-stage
pipeline with real infrastructure:
  - Azure OpenAI (gpt4o + embedlarge)
  - Trino (prod-replica)
  - pgvector (RDS)

Usage:
    python scripts/e2e_test.py                     # all 6 default questions
    python scripts/e2e_test.py -q "How many ..."   # single question
    python scripts/e2e_test.py --index 0 2 5       # specific indices

Requires:
    - VPN connected (Trino + pgvector access)
    - .env with all credentials
    - Preprocessing artifacts in data/
    - Embeddings loaded in pgvector
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv
load_dotenv()

from src.raven.connectors.openai_client import OpenAIClient
from src.raven.connectors.pgvector_store import PgVectorStore
from src.raven.connectors.trino_connector import TrinoConnector
from src.raven.pipeline import Pipeline

# ── Test Questions ─────────────────────────────────────────────────────

TEST_QUESTIONS = [
    # Q0: Simple — single table aggregate
    "How many batches are currently active?",

    # Q1: Simple — filtered count
    "How many lectures were completed in December 2025?",

    # Q2: Simple — single table with time filter
    "What is the total revenue collected in January 2026?",

    # Q3: Complex — multi-table join
    "Show me the top 10 batches by student enrollment count",

    # Q4: Complex — aggregation with join and filter
    "What is the average number of lectures per batch for batches that started in 2025?",

    # Q5: Complex — multi-hop join with time range
    "Which faculty members conducted the most lectures in the last 3 months?",
]


def create_connectors():
    """Initialize all three connectors from environment variables."""
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

    openai = OpenAIClient()

    return trino, pgvector, openai


def print_result(idx: int, question: str, result: dict, elapsed: float):
    """Pretty-print a single test result."""
    status = result.get("status", "unknown")
    difficulty = result.get("difficulty", "?")
    confidence = result.get("confidence", "?")
    sql = result.get("sql", "")
    row_count = result.get("row_count", 0)
    summary = result.get("summary", "")
    cost = result.get("cost", 0.0)
    timings = result.get("timings", {})
    error = result.get("error", "")

    status_icon = {"success": "✅", "error": "❌", "ambiguous": "🟡"}.get(status, "❓")

    print(f"\n{'═' * 80}")
    print(f"  Q{idx}: {question}")
    print(f"{'─' * 80}")
    print(f"  Status:     {status_icon} {status}")
    print(f"  Difficulty: {difficulty}")
    print(f"  Confidence: {confidence}")
    print(f"  Rows:       {row_count}")
    print(f"  Time:       {elapsed:.1f}s")
    print(f"  Cost:       ${cost:.4f}")

    if sql:
        # Truncate long SQL for readability
        sql_display = sql[:500] + ("..." if len(sql) > 500 else "")
        print(f"\n  SQL:\n    {sql_display}")

    if summary:
        print(f"\n  Summary: {summary[:300]}")

    if error:
        print(f"\n  Error: {error[:300]}")

    if timings:
        timing_str = " | ".join(f"{k}: {v:.1f}s" for k, v in timings.items())
        print(f"\n  Timings: {timing_str}")

    print(f"{'═' * 80}")


async def run_test(pipeline: Pipeline, questions: list[tuple[int, str]]):
    """Run E2E tests sequentially and collect results."""
    results = []
    overall_start = time.monotonic()

    for idx, question in questions:
        print(f"\n⏳ Running Q{idx}: {question[:70]}...")
        start = time.monotonic()
        try:
            result = await pipeline.generate(question)
        except Exception as e:
            result = {"status": "error", "error": str(e)}
        elapsed = time.monotonic() - start

        print_result(idx, question, result, elapsed)
        results.append({
            "index": idx,
            "question": question,
            "status": result.get("status"),
            "difficulty": result.get("difficulty"),
            "confidence": result.get("confidence"),
            "sql": result.get("sql", ""),
            "row_count": result.get("row_count", 0),
            "summary": result.get("summary", ""),
            "error": result.get("error", ""),
            "elapsed": round(elapsed, 2),
            "cost": result.get("cost", 0.0),
        })

    overall_time = time.monotonic() - overall_start

    # ── Summary ──────────────────────────────────────────────────────
    print(f"\n\n{'=' * 80}")
    print("  E2E TEST SUMMARY")
    print(f"{'=' * 80}")

    success = sum(1 for r in results if r["status"] == "success")
    errors = sum(1 for r in results if r["status"] == "error")
    ambiguous = sum(1 for r in results if r["status"] == "ambiguous")
    total_cost = sum(r["cost"] for r in results)

    print(f"  Total:     {len(results)} questions")
    print(f"  Success:   {success} ✅")
    print(f"  Error:     {errors} ❌")
    print(f"  Ambiguous: {ambiguous} 🟡")
    print(f"  Time:      {overall_time:.1f}s total")
    print(f"  Cost:      ${total_cost:.4f}")
    print(f"  Pass Rate: {success}/{len(results)} ({100*success/len(results):.0f}%)")
    print(f"{'=' * 80}\n")

    # Save results to file
    out_path = Path("data/e2e_test_results.json")
    with open(out_path, "w") as f:
        json.dump({
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "total_time": round(overall_time, 2),
            "total_cost": round(total_cost, 4),
            "pass_rate": f"{success}/{len(results)}",
            "results": results,
        }, f, indent=2, default=str)
    print(f"  Results saved to {out_path}")

    return results


def main():
    parser = argparse.ArgumentParser(description="RAVEN E2E Pipeline Test")
    parser.add_argument("-q", "--question", help="Run a single custom question")
    parser.add_argument("--index", nargs="+", type=int, help="Run specific question indices (0-5)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable DEBUG logging")
    args = parser.parse_args()

    # Logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(name)-30s %(levelname)-5s %(message)s",
        datefmt="%H:%M:%S",
    )
    # Quiet noisy libraries
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("openai").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    # Build question list
    if args.question:
        questions = [(0, args.question)]
    elif args.index:
        questions = [(i, TEST_QUESTIONS[i]) for i in args.index if i < len(TEST_QUESTIONS)]
    else:
        questions = list(enumerate(TEST_QUESTIONS))

    # Initialize
    print("🔧 Initializing connectors...")
    trino, pgvector, openai_client = create_connectors()

    print("🔧 Building pipeline...")
    pipeline = Pipeline(trino=trino, pgvector=pgvector, openai=openai_client)

    print(f"🚀 Running {len(questions)} question(s)...\n")
    asyncio.run(run_test(pipeline, questions))


if __name__ == "__main__":
    main()
