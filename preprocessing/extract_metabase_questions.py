"""
Preprocessing: Metabase Question Extraction
=============================================
Extracts native SQL questions from Metabase PostgreSQL database:
  1. Queries the report_card table for native SQL questions
  2. Filters for valid Trino SQL (parses with sqlparse)
  3. Deduplicates by SQL hash
  4. Extracts JOIN patterns → edges for table graph
  5. Prepares embedding texts for pgvector

Usage:
    python -m preprocessing.extract_metabase_questions \
        --host metabase-db.example.com \
        --port 5432 \
        --database metabase \
        --user reader \
        --password $METABASE_DB_PASSWORD \
        --output-dir data/
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import sys
from pathlib import Path
from typing import Any

import psycopg2
import sqlparse

logger = logging.getLogger(__name__)

# SQL to extract native queries from Metabase
METABASE_QUERY = """
SELECT
    rc.id AS card_id,
    rc.name AS question_text,
    (rc.dataset_query::json->>'native')::json->>'query' AS sql_query,
    rc.created_at,
    rc.updated_at
FROM report_card rc
WHERE (rc.dataset_query::json->>'type') = 'native'
  AND rc.archived = false
  AND (rc.dataset_query::json->>'native')::json->>'query' IS NOT NULL
ORDER BY rc.updated_at DESC;
"""

# Regex to extract JOIN clauses
JOIN_PATTERN = re.compile(
    r"(?:(?:LEFT|RIGHT|INNER|OUTER|FULL|CROSS)\s+)*JOIN\s+"
    r"([a-zA-Z0-9_.]+)\s+"
    r"(?:AS\s+\w+\s+)?ON\s+"
    r"([a-zA-Z0-9_.]+)\s*=\s*([a-zA-Z0-9_.]+)",
    re.IGNORECASE | re.MULTILINE,
)

# Regex to extract FROM tables
FROM_PATTERN = re.compile(
    r"FROM\s+([a-zA-Z0-9_.]+)",
    re.IGNORECASE,
)


def fetch_metabase_questions(
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
) -> list[dict]:
    """Fetch native SQL questions from Metabase PostgreSQL database."""
    logger.info("Connecting to Metabase DB: %s:%d/%s", host, port, database)

    conn = psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
    )

    try:
        with conn.cursor() as cur:
            cur.execute(METABASE_QUERY)
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            questions = [dict(zip(columns, row)) for row in rows]
    finally:
        conn.close()

    logger.info("Fetched %d raw questions from Metabase", len(questions))
    return questions


def filter_valid_sql(questions: list[dict]) -> list[dict]:
    """Filter to only valid, parseable SQL queries."""
    valid = []
    for q in questions:
        sql = q.get("sql_query", "")
        if not sql or not sql.strip():
            continue

        try:
            parsed = sqlparse.parse(sql)
            if not parsed:
                continue

            # Check it starts with SELECT or WITH
            first_token = parsed[0].get_type()
            if first_token not in ("SELECT", "UNKNOWN"):
                # UNKNOWN can be CTE (WITH)
                first_word = sql.strip().split()[0].upper()
                if first_word not in ("SELECT", "WITH"):
                    continue

            valid.append(q)
        except Exception:
            continue

    logger.info("Valid SQL: %d / %d", len(valid), len(questions))
    return valid


def deduplicate_by_sql(questions: list[dict]) -> list[dict]:
    """Deduplicate questions by SQL hash. Keep the most recently updated."""
    seen: dict[str, dict] = {}

    for q in questions:
        sql = q.get("sql_query", "").strip()
        # Normalize: remove whitespace variations for hashing
        normalized = " ".join(sql.split()).lower()
        sql_hash = hashlib.sha256(normalized.encode()).hexdigest()[:16]

        if sql_hash not in seen:
            seen[sql_hash] = q
        else:
            # Keep the more recently updated one
            existing = seen[sql_hash]
            if q.get("updated_at", "") > existing.get("updated_at", ""):
                seen[sql_hash] = q

    deduped = list(seen.values())
    logger.info("Deduplicated: %d → %d questions", len(questions), len(deduped))
    return deduped


def extract_join_patterns(questions: list[dict]) -> list[dict]:
    """
    Parse SQL for JOIN clauses and extract table relationships.

    Returns list of: {left_table, right_table, left_col, right_col, frequency}
    """
    join_counts: dict[tuple, int] = {}

    for q in questions:
        sql = q.get("sql_query", "")
        matches = JOIN_PATTERN.findall(sql)

        for joined_table, left_ref, right_ref in matches:
            # Normalize table.column references
            left_parts = left_ref.rsplit(".", 1)
            right_parts = right_ref.rsplit(".", 1)

            if len(left_parts) == 2 and len(right_parts) == 2:
                key = (left_parts[0], joined_table, left_parts[1], right_parts[1])
                join_counts[key] = join_counts.get(key, 0) + 1

    joins = []
    for (left_table, right_table, left_col, right_col), freq in join_counts.items():
        joins.append({
            "left_table": left_table,
            "right_table": right_table,
            "left_col": left_col,
            "right_col": right_col,
            "frequency": freq,
        })

    # Sort by frequency descending
    joins.sort(key=lambda x: x["frequency"], reverse=True)
    logger.info("Extracted %d unique JOIN patterns", len(joins))
    return joins


def extract_referenced_tables(questions: list[dict]) -> dict[str, int]:
    """Count table references across all questions."""
    table_counts: dict[str, int] = {}

    for q in questions:
        sql = q.get("sql_query", "")
        # FROM tables
        from_matches = FROM_PATTERN.findall(sql)
        # JOIN tables
        join_matches = JOIN_PATTERN.findall(sql)

        tables = set(from_matches)
        tables.update(m[0] for m in join_matches)

        for table in tables:
            table = table.strip().lower()
            if table and not table.startswith("("):
                table_counts[table] = table_counts.get(table, 0) + 1

    return dict(sorted(table_counts.items(), key=lambda x: -x[1]))


def build_embedding_texts(questions: list[dict]) -> list[dict]:
    """
    Build embedding texts for pgvector question_embeddings table.

    Each question becomes: question_text string, with SQL stored as metadata.
    """
    texts = []
    for q in questions:
        question_text = q.get("question_text", "").strip()
        sql_query = q.get("sql_query", "").strip()
        if not question_text or not sql_query:
            continue

        texts.append({
            "text": question_text,
            "metadata": {
                "question_text": question_text,
                "sql_query": sql_query,
                "card_id": q.get("card_id"),
                "source": "metabase",
            },
        })

    return texts


async def embed_and_store(
    texts: list[dict],
    openai_client: Any,
    pgvector_store: Any,
    batch_size: int = 50,
) -> int:
    """Embed question texts and store in pgvector question_embeddings table."""
    stored = 0
    for i in range(0, len(texts), batch_size):
        batch = texts[i : i + batch_size]
        text_strings = [t["text"] for t in batch]
        embeddings = await openai_client.batch_embed(text_strings)

        for item, embedding in zip(batch, embeddings):
            pgvector_store.insert(
                table_name="question_embeddings",
                embedding=embedding,
                metadata=item["metadata"],
                source_id=f"metabase_{item['metadata'].get('card_id', '')}",
            )
            stored += 1

        logger.info("Embedded batch %d-%d (%d total)", i, i + len(batch), stored)

    return stored


# ── CLI Entry Point ────────────────────────────────────────────────────


def save_questions(questions: list[dict], output_path: Path) -> None:
    """Save processed questions to JSON."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    # Serialize — convert datetime objects to strings
    serializable = []
    for q in questions:
        sq = {}
        for k, v in q.items():
            sq[k] = str(v) if hasattr(v, "isoformat") else v
        serializable.append(sq)

    with open(output_path, "w") as f:
        json.dump(serializable, f, indent=2)
    logger.info("Saved %d questions to %s", len(questions), output_path)


def main():
    parser = argparse.ArgumentParser(description="Extract Metabase questions for RAVEN")
    parser.add_argument("--host", required=True, help="Metabase PostgreSQL host")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--database", default="metabase")
    parser.add_argument("--user", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--output-dir", default="data")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    output_dir = Path(args.output_dir)

    # Fetch
    raw_questions = fetch_metabase_questions(
        host=args.host,
        port=args.port,
        database=args.database,
        user=args.user,
        password=args.password,
    )

    # Process
    valid = filter_valid_sql(raw_questions)
    deduped = deduplicate_by_sql(valid)
    join_patterns = extract_join_patterns(deduped)
    table_refs = extract_referenced_tables(deduped)

    # Save
    save_questions(deduped, output_dir / "metabase_questions.json")

    with open(output_dir / "metabase_join_patterns.json", "w") as f:
        json.dump(join_patterns, f, indent=2)
    logger.info("Saved %d JOIN patterns", len(join_patterns))

    with open(output_dir / "metabase_table_references.json", "w") as f:
        json.dump(table_refs, f, indent=2)
    logger.info("Saved table reference counts (%d tables)", len(table_refs))

    # Embedding texts (for later batch embedding)
    texts = build_embedding_texts(deduped)
    texts_path = output_dir / "question_embedding_texts.json"
    with open(texts_path, "w") as f:
        json.dump(texts, f, indent=2)
    logger.info("Saved %d embedding texts to %s", len(texts), texts_path)

    logger.info(
        "Metabase extraction complete! %d questions, %d JOIN patterns, %d referenced tables",
        len(deduped),
        len(join_patterns),
        len(table_refs),
    )


if __name__ == "__main__":
    main()
