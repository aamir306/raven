"""
Preprocessing: Glossary Builder
=================================
Embeds semantic model entries into pgvector for glossary retrieval:
  - Business terms & definitions
  - Verified SQL queries (with question-SQL pairs)
  - Dimension/metric descriptions
  - Synonyms & alias mappings
  - Business rules (e.g., "active user = last_login_at > 30 days")

Source: config/semantic_model.yaml
Target: pgvector glossary_embeddings table

Usage:
    python -m preprocessing.build_glossary \
        --semantic-model config/semantic_model.yaml \
        --pgvector-dsn postgresql://raven:raven@localhost:5432/raven
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)


# ── Text Extraction ────────────────────────────────────────────────────


def extract_glossary_entries(model: dict) -> list[dict]:
    """
    Extract all embeddable entries from semantic model.

    Entry types:
      - table: table description + columns
      - dimension: individual dimension/measure
      - metric: composite metric with formula
      - business_rule: rule + SQL translation
      - verified_query: question-SQL pair
      - synonym: alias -> canonical mapping
    """
    entries: list[dict] = []
    seen_hashes: set[str] = set()

    # 1. Table descriptions
    for table in model.get("tables", []):
        table_name = table.get("name", "")
        desc = table.get("description", "")
        if table_name and desc:
            entry = {
                "type": "table",
                "name": table_name,
                "text": f"Table {table_name}: {desc}",
                "metadata": {
                    "table": table_name,
                    "layer": table.get("layer", ""),
                    "schema": table.get("schema", ""),
                },
            }
            entries.append(_dedup(entry, seen_hashes))

        # Dimensions
        for dim in table.get("dimensions", []):
            dim_name = dim.get("name", "")
            dim_desc = dim.get("description", "")
            if dim_name:
                text = f"Dimension '{dim_name}' in {table_name}: {dim_desc}"
                if dim.get("expr"):
                    text += f" (SQL: {dim['expr']})"
                entry = {
                    "type": "dimension",
                    "name": dim_name,
                    "text": text,
                    "metadata": {
                        "table": table_name,
                        "column": dim.get("expr", dim_name),
                        "data_type": dim.get("data_type", ""),
                    },
                }
                entries.append(_dedup(entry, seen_hashes))

        # Measures/Metrics per table
        for measure in table.get("measures", []):
            m_name = measure.get("name", "")
            m_desc = measure.get("description", "")
            if m_name:
                text = f"Measure '{m_name}' in {table_name}: {m_desc}"
                if measure.get("expr"):
                    text += f" (SQL: {measure['expr']})"
                if measure.get("agg"):
                    text += f" (Aggregation: {measure['agg']})"
                entry = {
                    "type": "measure",
                    "name": m_name,
                    "text": text,
                    "metadata": {
                        "table": table_name,
                        "expr": measure.get("expr", ""),
                        "agg": measure.get("agg", ""),
                    },
                }
                entries.append(_dedup(entry, seen_hashes))

        # Relationships
        for rel in table.get("relationships", []):
            target = rel.get("target", "")
            join_key = rel.get("join_key", "")
            if target:
                text = f"Join: {table_name} to {target} on {join_key} ({rel.get('type', '')})"
                entry = {
                    "type": "relationship",
                    "name": f"{table_name}__{target}",
                    "text": text,
                    "metadata": {
                        "from_table": table_name,
                        "to_table": target,
                        "join_key": join_key,
                    },
                }
                entries.append(_dedup(entry, seen_hashes))

    # 2. Global metrics (composite)
    for metric in model.get("metrics", []):
        m_name = metric.get("name", "")
        m_desc = metric.get("description", "")
        if m_name:
            text = f"Metric '{m_name}': {m_desc}"
            if metric.get("formula"):
                text += f" (Formula: {metric['formula']})"
            if metric.get("sql"):
                text += f" (SQL: {metric['sql']})"
            entry = {
                "type": "metric",
                "name": m_name,
                "text": text,
                "metadata": {
                    "formula": metric.get("formula", ""),
                    "tables": metric.get("tables", []),
                },
            }
            entries.append(_dedup(entry, seen_hashes))

    # 3. Business rules
    for rule in model.get("business_rules", []):
        r_name = rule.get("name", "")
        r_def = rule.get("definition", "")
        r_sql = rule.get("sql", "")
        if r_name:
            text = f"Business rule '{r_name}': {r_def}"
            if r_sql:
                text += f" → SQL: {r_sql}"
            entry = {
                "type": "business_rule",
                "name": r_name,
                "text": text,
                "metadata": {
                    "definition": r_def,
                    "sql": r_sql,
                    "tables": rule.get("tables", []),
                },
            }
            entries.append(_dedup(entry, seen_hashes))

    # 4. Verified queries
    for vq in model.get("verified_queries", []):
        question = vq.get("question", "")
        sql = vq.get("sql", "")
        if question and sql:
            text = f"Verified query: '{question}' → {sql}"
            entry = {
                "type": "verified_query",
                "name": question[:100],
                "text": text,
                "metadata": {
                    "question": question,
                    "sql": sql,
                    "tables": vq.get("tables", []),
                    "verified_by": vq.get("verified_by", ""),
                },
            }
            entries.append(_dedup(entry, seen_hashes))

    # 5. Synonyms
    for syn in model.get("synonyms", []):
        alias = syn.get("alias", "")
        canonical = syn.get("canonical", "")
        if alias and canonical:
            text = f"Synonym: '{alias}' means '{canonical}'"
            entry = {
                "type": "synonym",
                "name": alias,
                "text": text,
                "metadata": {
                    "alias": alias,
                    "canonical": canonical,
                },
            }
            entries.append(_dedup(entry, seen_hashes))

    # Filter None entries (duplicates)
    entries = [e for e in entries if e is not None]

    logger.info(
        "Extracted %d glossary entries: %s",
        len(entries),
        _type_counts(entries),
    )
    return entries


def _dedup(entry: dict, seen: set[str]) -> dict | None:
    """Deduplicate by text hash."""
    h = hashlib.sha256(entry["text"].encode()).hexdigest()[:16]
    if h in seen:
        return None
    seen.add(h)
    entry["hash"] = h
    return entry


def _type_counts(entries: list[dict]) -> dict[str, int]:
    """Count entries by type."""
    counts: dict[str, int] = {}
    for e in entries:
        t = e.get("type", "unknown")
        counts[t] = counts.get(t, 0) + 1
    return counts


# ── Embedding & Storage ───────────────────────────────────────────────


async def embed_and_store(
    entries: list[dict],
    pgvector_dsn: str,
    openai_api_key: str | None = None,
    batch_size: int = 100,
) -> int:
    """
    Embed glossary texts and store in pgvector.

    Table: glossary_embeddings
    Columns: id, type, name, text, embedding, metadata
    """
    import openai
    import asyncpg

    client = openai.AsyncOpenAI(api_key=openai_api_key)

    conn = await asyncpg.connect(pgvector_dsn)
    try:
        # Create table if not exists
        await conn.execute("""
            CREATE EXTENSION IF NOT EXISTS vector;
            CREATE TABLE IF NOT EXISTS glossary_embeddings (
                id SERIAL PRIMARY KEY,
                type VARCHAR(50) NOT NULL,
                name VARCHAR(500) NOT NULL,
                text TEXT NOT NULL,
                embedding vector(1536),
                metadata JSONB DEFAULT '{}',
                text_hash VARCHAR(16) UNIQUE,
                created_at TIMESTAMP DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_glossary_embedding
                ON glossary_embeddings USING ivfflat (embedding vector_cosine_ops)
                WITH (lists = 50);
        """)

        stored = 0
        for i in range(0, len(entries), batch_size):
            batch = entries[i : i + batch_size]
            texts = [e["text"] for e in batch]

            # Embed
            resp = await client.embeddings.create(
                model="text-embedding-3-small",
                input=texts,
            )
            embeddings = [item.embedding for item in resp.data]

            # Upsert
            for entry, emb in zip(batch, embeddings):
                await conn.execute(
                    """
                    INSERT INTO glossary_embeddings (type, name, text, embedding, metadata, text_hash)
                    VALUES ($1, $2, $3, $4::vector, $5, $6)
                    ON CONFLICT (text_hash) DO UPDATE SET
                        embedding = EXCLUDED.embedding,
                        metadata = EXCLUDED.metadata,
                        created_at = NOW()
                    """,
                    entry["type"],
                    entry["name"],
                    entry["text"],
                    str(emb),
                    json.dumps(entry.get("metadata", {})),
                    entry.get("hash", ""),
                )
                stored += 1

            logger.info("Embedded batch %d-%d / %d", i, i + len(batch), len(entries))

        return stored

    finally:
        await conn.close()


# ── File-Based Fallback ───────────────────────────────────────────────


def save_glossary_texts(entries: list[dict], output_path: Path) -> None:
    """Save glossary entries to JSON for offline/file-based retrieval."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(entries, f, indent=2)
    logger.info("Saved %d glossary entries to %s", len(entries), output_path)


# ── CLI ────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="Build glossary embeddings for RAVEN")
    parser.add_argument("--semantic-model", default="config/semantic_model.yaml")
    parser.add_argument("--pgvector-dsn", help="PostgreSQL DSN (optional — skips embedding if not set)")
    parser.add_argument("--output", default="data/glossary_entries.json")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    model_path = Path(args.semantic_model)
    if not model_path.exists():
        logger.error("Semantic model not found: %s", model_path)
        sys.exit(1)

    with open(model_path) as f:
        model = yaml.safe_load(f) or {}

    entries = extract_glossary_entries(model)

    # Always save to file
    save_glossary_texts(entries, Path(args.output))

    # Optionally embed and store
    if args.pgvector_dsn:
        import asyncio
        import os

        stored = asyncio.run(
            embed_and_store(
                entries,
                pgvector_dsn=args.pgvector_dsn,
                openai_api_key=os.getenv("OPENAI_API_KEY"),
            )
        )
        logger.info("Stored %d embeddings in pgvector", stored)
    else:
        logger.info("No pgvector DSN — skipped embedding. Use --pgvector-dsn to embed.")

    logger.info("Glossary build complete!")


if __name__ == "__main__":
    main()
