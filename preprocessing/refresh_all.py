"""
Preprocessing: Refresh All
============================
Orchestrator that runs all preprocessing scripts sequentially.
Designed for scheduled execution (K8s CronJob, cron, or manual).

Stages (in order):
  1. Extract dbt metadata (manifest → catalog + lineage + embeddings)
  2. Extract Metabase questions (SQL pairs → questions + join patterns)
  3. Build LSH index (column values → MinHash LSH for entity matching)
  4. Build Content Awareness (column stats → content_awareness.json)
  5. Build Table Graph (merge dbt + Metabase + semantic → unified graph)
  6. Build Glossary (semantic model → glossary embeddings)
  7. Ingest Documentation (docs + annotations → doc embeddings)

Environment Variables:
  RAVEN_DBT_MANIFEST    - Path to dbt manifest.json
  RAVEN_METABASE_DSN    - Metabase PostgreSQL connection string
  RAVEN_TRINO_HOST      - Trino host for live sampling
  RAVEN_TRINO_PORT      - Trino port (default: 8080)
  RAVEN_PGVECTOR_DSN    - pgvector PostgreSQL connection string
  RAVEN_SEMANTIC_MODEL  - Path to semantic_model.yaml
  RAVEN_DOCS_DIR        - Path to documentation directory
  OPENAI_API_KEY        - OpenAI API key for embeddings

Usage:
    python -m preprocessing.refresh_all
    python -m preprocessing.refresh_all --stages dbt,lsh,graph
    python -m preprocessing.refresh_all --dry-run
"""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# ── Stage Definitions ─────────────────────────────────────────────────


@dataclass
class Stage:
    name: str
    description: str
    module: str
    args: list[str] = field(default_factory=list)
    required_env: list[str] = field(default_factory=list)
    required_files: list[str] = field(default_factory=list)
    optional: bool = False


ALL_STAGES: list[Stage] = [
    Stage(
        name="dbt",
        description="Extract dbt metadata (catalog + lineage + embedding texts)",
        module="preprocessing.extract_dbt_metadata",
        args=["--manifest", "{RAVEN_DBT_MANIFEST}", "--output-dir", "data/"],
        required_env=["RAVEN_DBT_MANIFEST"],
        required_files=["{RAVEN_DBT_MANIFEST}"],
    ),
    Stage(
        name="metabase",
        description="Extract Metabase SQL questions and JOIN patterns",
        module="preprocessing.extract_metabase_questions",
        args=["--dsn", "{RAVEN_METABASE_DSN}", "--output-dir", "data/"],
        required_env=["RAVEN_METABASE_DSN"],
        optional=True,
    ),
    Stage(
        name="lsh",
        description="Build LSH index for entity matching",
        module="preprocessing.build_lsh_index",
        args=[
            "--trino-host", "{RAVEN_TRINO_HOST}",
            "--trino-port", "{RAVEN_TRINO_PORT}",
            "--output", "data/lsh_index.pkl",
        ],
        required_env=["RAVEN_TRINO_HOST"],
        optional=True,
    ),
    Stage(
        name="content_awareness",
        description="Build Content Awareness column metadata",
        module="preprocessing.build_content_awareness",
        args=[
            "--catalog-path", "data/schema_catalog.json",
            "--trino-host", "{RAVEN_TRINO_HOST}",
            "--output", "data/content_awareness.json",
        ],
        required_files=["data/schema_catalog.json"],
        optional=True,
    ),
    Stage(
        name="graph",
        description="Build unified table relationship graph",
        module="preprocessing.build_table_graph",
        args=[
            "--dbt-lineage", "data/dbt_lineage_graph.gpickle",
            "--metabase-joins", "data/metabase_join_patterns.json",
            "--semantic-model", "{RAVEN_SEMANTIC_MODEL}",
            "--output", "data/table_graph.gpickle",
        ],
    ),
    Stage(
        name="glossary",
        description="Build glossary embeddings from semantic model",
        module="preprocessing.build_glossary",
        args=[
            "--semantic-model", "{RAVEN_SEMANTIC_MODEL}",
            "--pgvector-dsn", "{RAVEN_PGVECTOR_DSN}",
            "--output", "data/glossary_entries.json",
        ],
    ),
    Stage(
        name="docs",
        description="Ingest documentation and table annotations",
        module="preprocessing.ingest_documentation",
        args=[
            "--docs-dir", "{RAVEN_DOCS_DIR}",
            "--annotations", "config/table_annotations.yaml",
            "--pgvector-dsn", "{RAVEN_PGVECTOR_DSN}",
            "--output", "data/doc_chunks.json",
        ],
    ),
]


# ── Runner ─────────────────────────────────────────────────────────────


@dataclass
class StageResult:
    name: str
    status: str  # success, skipped, failed
    duration_sec: float
    error: str | None = None


def resolve_arg(arg: str, env: dict[str, str]) -> str:
    """Replace {ENV_VAR} placeholders with environment values."""
    if arg.startswith("{") and arg.endswith("}"):
        var = arg[1:-1]
        return env.get(var, "")
    return arg


def check_prerequisites(stage: Stage, env: dict[str, str]) -> str | None:
    """Return error message if prerequisites not met, else None."""
    for var in stage.required_env:
        if not env.get(var):
            return f"Missing env: {var}"

    for fp in stage.required_files:
        resolved = resolve_arg(fp, env) if "{" in fp else fp
        if resolved and not Path(resolved).exists():
            return f"Missing file: {resolved}"

    return None


def run_stage(stage: Stage, env: dict[str, str], dry_run: bool = False) -> StageResult:
    """Run a single preprocessing stage."""
    start = time.time()

    # Check prerequisites
    prereq_error = check_prerequisites(stage, env)
    if prereq_error:
        if stage.optional:
            logger.info("[SKIP] %s: %s (optional)", stage.name, prereq_error)
            return StageResult(stage.name, "skipped", time.time() - start, prereq_error)
        else:
            logger.warning("[SKIP] %s: %s", stage.name, prereq_error)
            return StageResult(stage.name, "skipped", time.time() - start, prereq_error)

    # Resolve args
    args = [resolve_arg(a, env) for a in stage.args]
    # Filter empty args (from missing optional env vars)
    filtered_args = []
    skip_next = False
    for i, arg in enumerate(args):
        if skip_next:
            skip_next = False
            continue
        if not arg and i > 0 and args[i - 1].startswith("--"):
            # Remove the flag too
            filtered_args.pop()
            continue
        filtered_args.append(arg)

    cmd = [sys.executable, "-m", stage.module] + filtered_args

    if dry_run:
        logger.info("[DRY-RUN] %s: %s", stage.name, " ".join(cmd))
        return StageResult(stage.name, "skipped", time.time() - start)

    logger.info("=" * 60)
    logger.info("[START] %s: %s", stage.name, stage.description)
    logger.info("Command: %s", " ".join(cmd))

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=3600,  # 1 hour max per stage
        )

        if result.stdout:
            for line in result.stdout.strip().split("\n"):
                logger.info("  %s", line)

        if result.returncode != 0:
            error_msg = result.stderr.strip() or f"Exit code: {result.returncode}"
            logger.error("[FAILED] %s: %s", stage.name, error_msg)
            return StageResult(stage.name, "failed", time.time() - start, error_msg)

        duration = time.time() - start
        logger.info("[DONE] %s (%.1fs)", stage.name, duration)
        return StageResult(stage.name, "success", duration)

    except subprocess.TimeoutExpired:
        return StageResult(stage.name, "failed", time.time() - start, "Timeout (1h)")
    except Exception as e:
        return StageResult(stage.name, "failed", time.time() - start, str(e))


def run_all(
    stages: list[Stage] | None = None,
    stage_filter: set[str] | None = None,
    dry_run: bool = False,
) -> list[StageResult]:
    """Run all (or filtered) preprocessing stages."""
    stages = stages or ALL_STAGES

    if stage_filter:
        stages = [s for s in stages if s.name in stage_filter]

    # Collect environment
    env = {
        "RAVEN_DBT_MANIFEST": os.getenv("RAVEN_DBT_MANIFEST", ""),
        "RAVEN_METABASE_DSN": os.getenv("RAVEN_METABASE_DSN", ""),
        "RAVEN_TRINO_HOST": os.getenv("RAVEN_TRINO_HOST", ""),
        "RAVEN_TRINO_PORT": os.getenv("RAVEN_TRINO_PORT", "8080"),
        "RAVEN_PGVECTOR_DSN": os.getenv("RAVEN_PGVECTOR_DSN", ""),
        "RAVEN_SEMANTIC_MODEL": os.getenv("RAVEN_SEMANTIC_MODEL", "config/semantic_model.yaml"),
        "RAVEN_DOCS_DIR": os.getenv("RAVEN_DOCS_DIR", "docs/"),
        "OPENAI_API_KEY": os.getenv("OPENAI_API_KEY", ""),
    }

    # Ensure data directory exists
    Path("data").mkdir(exist_ok=True)

    results: list[StageResult] = []
    total_start = time.time()

    logger.info("RAVEN Preprocessing Pipeline — %d stages", len(stages))
    logger.info("=" * 60)

    for stage in stages:
        result = run_stage(stage, env, dry_run=dry_run)
        results.append(result)

        # Stop on critical failure (non-optional)
        if result.status == "failed" and not stage.optional:
            logger.error("Critical stage failed: %s — stopping pipeline", stage.name)
            break

    # Summary
    total_duration = time.time() - total_start
    logger.info("")
    logger.info("=" * 60)
    logger.info("PIPELINE SUMMARY (%.1fs total)", total_duration)
    logger.info("=" * 60)

    for r in results:
        icon = {"success": "OK", "skipped": "SKIP", "failed": "FAIL"}[r.status]
        detail = f" ({r.error})" if r.error else ""
        logger.info("  [%s] %-20s %.1fs%s", icon, r.name, r.duration_sec, detail)

    succeeded = sum(1 for r in results if r.status == "success")
    failed = sum(1 for r in results if r.status == "failed")
    skipped = sum(1 for r in results if r.status == "skipped")
    logger.info("")
    logger.info("Results: %d success, %d failed, %d skipped", succeeded, failed, skipped)

    return results


# ── CLI ────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="RAVEN Preprocessing Pipeline — Refresh All")
    parser.add_argument(
        "--stages",
        help="Comma-separated list of stages to run (default: all). Options: dbt,metabase,lsh,content_awareness,graph,glossary,docs",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print commands without executing")
    parser.add_argument("--list", action="store_true", help="List available stages")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    if args.list:
        print("\nAvailable preprocessing stages:")
        print("-" * 50)
        for s in ALL_STAGES:
            opt = " (optional)" if s.optional else ""
            print(f"  {s.name:20s} {s.description}{opt}")
        return

    stage_filter = None
    if args.stages:
        stage_filter = {s.strip() for s in args.stages.split(",")}

    results = run_all(stage_filter=stage_filter, dry_run=args.dry_run)

    # Exit code: 1 if any critical failures
    if any(r.status == "failed" for r in results):
        sys.exit(1)


if __name__ == "__main__":
    main()
