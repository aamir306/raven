#!/usr/bin/env python3
"""
Preprocessing: Fine-Tuning Data Export
=======================================
Export validated (question, SQL) pairs from query_log for RLVR fine-tuning.

Sources:
  1. Thumbs-up pairs from query_log (feedback = 'positive')
  2. Corrected pairs (feedback = 'corrected', correction_sql is not null)
  3. Verified queries from semantic_model.yaml

Output: JSONL file compatible with OpenAI fine-tuning format.

Usage:
    PYTHONPATH=. python preprocessing/export_finetuning_data.py \
        --output data/finetuning_pairs.jsonl \
        --min-confidence MEDIUM \
        --format openai
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)


async def export_from_query_log(dsn: str) -> list[dict]:
    """Export thumbs-up and corrected pairs from PostgreSQL query_log."""
    import asyncpg

    pairs = []

    try:
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=3)

        # Thumbs-up pairs
        rows = await pool.fetch("""
            SELECT question, sql_query, confidence, difficulty, tables_used,
                   created_at, feedback
            FROM query_log
            WHERE feedback IN ('positive', 'thumbs_up', 'up')
              AND sql_query IS NOT NULL
              AND sql_query != ''
            ORDER BY created_at DESC
        """)

        for row in rows:
            pairs.append({
                "question": row["question"],
                "sql": row["sql_query"],
                "confidence": row["confidence"],
                "difficulty": row["difficulty"],
                "tables_used": row["tables_used"],
                "source": "thumbs_up",
                "created_at": str(row["created_at"]),
            })

        logger.info("Exported %d thumbs-up pairs from query_log", len(rows))

        # Corrected pairs (human-verified SQL)
        correction_rows = await pool.fetch("""
            SELECT question, correction_sql AS sql_query, confidence, difficulty,
                   tables_used, created_at
            FROM query_log
            WHERE correction_sql IS NOT NULL
              AND correction_sql != ''
            ORDER BY created_at DESC
        """)

        for row in correction_rows:
            pairs.append({
                "question": row["question"],
                "sql": row["sql_query"],
                "confidence": row["confidence"],
                "difficulty": row["difficulty"],
                "tables_used": row["tables_used"],
                "source": "human_correction",
                "created_at": str(row["created_at"]),
            })

        logger.info("Exported %d corrected pairs from query_log", len(correction_rows))

        await pool.close()

    except Exception as e:
        logger.warning("Could not connect to query_log: %s", e)

    return pairs


def export_from_semantic_model(model_path: Path) -> list[dict]:
    """Export verified queries from semantic_model.yaml."""
    pairs = []

    if not model_path.exists():
        logger.warning("Semantic model not found: %s", model_path)
        return pairs

    with open(model_path) as f:
        model = yaml.safe_load(f)

    for vq in model.get("verified_queries", []):
        question = vq.get("question", "")
        sql = vq.get("sql", "")
        if question and sql:
            pairs.append({
                "question": question,
                "sql": sql,
                "confidence": "HIGH",
                "difficulty": vq.get("difficulty", "SIMPLE"),
                "tables_used": vq.get("tables_used", []),
                "source": "verified_query",
                "created_at": None,
            })

    logger.info("Exported %d verified queries from semantic model", len(pairs))
    return pairs


def export_from_metabase(questions_path: Path) -> list[dict]:
    """Export high-quality Metabase questions as training pairs."""
    pairs = []

    if not questions_path.exists():
        logger.warning("Metabase questions not found: %s", questions_path)
        return pairs

    with open(questions_path) as f:
        questions = json.load(f)

    for q in questions:
        name = q.get("name", "")
        sql = q.get("sql", "")
        if name and sql and len(sql) > 20:
            pairs.append({
                "question": name,
                "sql": sql,
                "confidence": "HIGH",
                "difficulty": "SIMPLE" if "SELECT" in sql.upper() and sql.upper().count("JOIN") < 2 else "COMPLEX",
                "tables_used": q.get("tables", []),
                "source": "metabase",
                "created_at": None,
            })

    logger.info("Exported %d Metabase questions", len(pairs))
    return pairs


def deduplicate(pairs: list[dict]) -> list[dict]:
    """Deduplicate by normalized question text."""
    seen = set()
    unique = []
    for p in pairs:
        key = p["question"].lower().strip()
        if key not in seen:
            seen.add(key)
            unique.append(p)
    return unique


def format_openai(pair: dict) -> dict:
    """Format as OpenAI fine-tuning JSONL entry."""
    return {
        "messages": [
            {
                "role": "system",
                "content": "You are RAVEN, an expert Trino SQL generator for a data warehouse. "
                           "Generate accurate Trino SQL from natural language questions.",
            },
            {"role": "user", "content": pair["question"]},
            {"role": "assistant", "content": pair["sql"]},
        ]
    }


def format_dpo(pair: dict) -> dict:
    """Format as DPO (Direct Preference Optimization) entry."""
    return {
        "prompt": pair["question"],
        "chosen": pair["sql"],
        "rejected": "",  # Placeholder — fill with negative examples later
        "metadata": {
            "source": pair.get("source"),
            "confidence": pair.get("confidence"),
        },
    }


def format_raw(pair: dict) -> dict:
    """Raw question-SQL pair."""
    return {
        "question": pair["question"],
        "sql": pair["sql"],
        "source": pair.get("source"),
        "confidence": pair.get("confidence"),
        "difficulty": pair.get("difficulty"),
    }


FORMATTERS = {
    "openai": format_openai,
    "dpo": format_dpo,
    "raw": format_raw,
}


async def main():
    parser = argparse.ArgumentParser(description="Export fine-tuning data for RLVR")
    parser.add_argument("--output", default="data/finetuning_pairs.jsonl")
    parser.add_argument("--format", choices=["openai", "dpo", "raw"], default="openai")
    parser.add_argument("--min-confidence", choices=["HIGH", "MEDIUM", "LOW"], default="MEDIUM")
    parser.add_argument("--include-metabase", action="store_true",
                        help="Include Metabase saved questions")
    parser.add_argument("--semantic-model", default="config/semantic_model.yaml")
    parser.add_argument("--metabase-path", default="data/metabase_questions.json")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-5s %(message)s",
        datefmt="%H:%M:%S",
    )

    confidence_rank = {"HIGH": 3, "MEDIUM": 2, "LOW": 1}
    min_rank = confidence_rank.get(args.min_confidence, 1)

    all_pairs = []

    # 1. Query log (thumbs-up + corrections)
    pgvector_dsn = os.environ.get("PGVECTOR_DSN")
    if not pgvector_dsn:
        logger.warning(
            "PGVECTOR_DSN not set. Set it in .env or environment. "
            "Example: postgresql://user:pass@host:5432/dbname"
        )
        pgvector_dsn = "postgresql://localhost:5432/raven"
    query_log_pairs = await export_from_query_log(pgvector_dsn)
    all_pairs.extend(query_log_pairs)

    # 2. Verified queries from semantic model
    sm_pairs = export_from_semantic_model(Path(args.semantic_model))
    all_pairs.extend(sm_pairs)

    # 3. Optionally include Metabase
    if args.include_metabase:
        mb_pairs = export_from_metabase(Path(args.metabase_path))
        all_pairs.extend(mb_pairs)

    # Filter by confidence
    filtered = [
        p for p in all_pairs
        if confidence_rank.get(p.get("confidence", "LOW"), 0) >= min_rank
    ]

    # Deduplicate
    unique = deduplicate(filtered)

    logger.info(
        "Total pairs: %d → filtered: %d → unique: %d",
        len(all_pairs), len(filtered), len(unique),
    )

    if args.dry_run:
        print(f"\nDRY RUN: Would export {len(unique)} pairs")
        print(f"  By source:")
        from collections import Counter
        source_counts = Counter(p.get("source") for p in unique)
        for source, count in source_counts.most_common():
            print(f"    {source}: {count}")
        print(f"  Format: {args.format}")
        return

    # Format and write
    formatter = FORMATTERS[args.format]
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        for pair in unique:
            line = json.dumps(formatter(pair), ensure_ascii=False)
            f.write(line + "\n")

    print(f"\nExported {len(unique)} pairs to {output_path}")
    print(f"Format: {args.format}")

    from collections import Counter
    source_counts = Counter(p.get("source") for p in unique)
    for source, count in source_counts.most_common():
        print(f"  {source}: {count}")


if __name__ == "__main__":
    asyncio.run(main())
