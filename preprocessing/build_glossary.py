"""
Preprocessing: Glossary Builder
=================================
Embeds semantic model entries into pgvector for glossary retrieval:
  - Table descriptions + synonyms
  - Dimensions and time-dimensions
  - Metrics (per-table, with SQL fragments)
  - Business rules (term → SQL translation)
  - Verified SQL queries (question-SQL pairs)
  - Table relationships/join paths

Source: config/semantic_model.yaml  (Snowflake Cortex Analyst-style)
Target: pgvector glossary_embeddings table

Usage:
    python -m preprocessing.build_glossary \
        --semantic-model config/semantic_model.yaml

    Requires env vars: OPENAI_API_BASE, OPENAI_API_KEY, etc.
    (same as other preprocessing scripts).
"""

from __future__ import annotations

import argparse
import asyncio
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
    Extract all embeddable entries from semantic model YAML.

    Handles the PW CDP semantic model format:
      - tables: with synonyms, dimensions, time_dimensions, metrics
      - business_rules: term + definition + sql_fragment + synonyms
      - verified_queries: question + sql + notes
      - relationships: top-level join paths between tables

    Entry types produced:
      - table: table description + synonym list
      - dimension: column used for grouping/filtering
      - time_dimension: date/timestamp column
      - metric: aggregate measure with SQL fragment
      - business_rule: rule + SQL translation + synonyms
      - verified_query: gold-standard question-SQL pair
      - relationship: explicit join path between tables
      - synonym: alias → canonical mapping (extracted from tables + rules)
    """
    entries: list[dict] = []
    seen_hashes: set[str] = set()

    # ── 1. Tables ──────────────────────────────────────────────────

    for table in model.get("tables", []):
        table_name = table.get("name", "")
        desc = table.get("description", "").strip()
        synonyms = table.get("synonyms", [])
        if not table_name:
            continue

        # Table entry (include synonyms in text for semantic matching)
        if desc:
            syn_text = f" Also known as: {', '.join(synonyms)}." if synonyms else ""
            entry = {
                "type": "table",
                "name": table_name,
                "text": f"Table {table_name}: {desc}{syn_text}",
                "metadata": {
                    "table": table_name,
                    "synonyms": synonyms,
                },
            }
            entries.append(_dedup(entry, seen_hashes))

        # Dimensions
        for dim in table.get("dimensions", []):
            dim_name = dim.get("name", "")
            dim_desc = dim.get("description", "").strip()
            if not dim_name:
                continue
            text = f"Dimension '{dim_name}' in {table_name}: {dim_desc}"
            values = dim.get("values", [])
            if values:
                text += f" Allowed values: {', '.join(str(v) for v in values)}."
            entry = {
                "type": "dimension",
                "name": dim_name,
                "text": text,
                "metadata": {
                    "table": table_name,
                    "column": dim_name,
                    "values": values,
                },
            }
            entries.append(_dedup(entry, seen_hashes))

        # Time dimensions
        for td in table.get("time_dimensions", []):
            td_name = td.get("name", "")
            td_desc = td.get("description", "").strip()
            if not td_name:
                continue
            entry = {
                "type": "time_dimension",
                "name": td_name,
                "text": f"Time dimension '{td_name}' in {table_name}: {td_desc}",
                "metadata": {
                    "table": table_name,
                    "column": td_name,
                },
            }
            entries.append(_dedup(entry, seen_hashes))

        # Metrics (per-table, with SQL fragment)
        for metric in table.get("metrics", []):
            m_name = metric.get("name", "")
            m_desc = metric.get("description", "").strip()
            m_sql = metric.get("sql", "")
            if not m_name:
                continue
            text = f"Metric '{m_name}' in {table_name}: {m_desc}"
            if m_sql:
                text += f" (SQL: {m_sql})"
            entry = {
                "type": "metric",
                "name": m_name,
                "text": text,
                "metadata": {
                    "table": table_name,
                    "sql": m_sql,
                },
            }
            entries.append(_dedup(entry, seen_hashes))

        # Per-table synonym entries (alias → table)
        for syn in synonyms:
            entry = {
                "type": "synonym",
                "name": syn,
                "text": f"Synonym: '{syn}' refers to table {table_name}",
                "metadata": {"alias": syn, "canonical": table_name},
            }
            entries.append(_dedup(entry, seen_hashes))

    # ── 2. Business rules ──────────────────────────────────────────

    for rule in model.get("business_rules", []):
        term = rule.get("term", rule.get("name", ""))
        definition = rule.get("definition", "").strip()
        sql_fragment = rule.get("sql_fragment", rule.get("sql", ""))
        rule_synonyms = rule.get("synonyms", [])
        if not term:
            continue

        text = f"Business rule '{term}': {definition}"
        if sql_fragment:
            text += f" → SQL: {sql_fragment}"
        if rule_synonyms:
            text += f" Also known as: {', '.join(rule_synonyms)}."

        entry = {
            "type": "business_rule",
            "name": term,
            "text": text,
            "metadata": {
                "term": term,
                "definition": definition,
                "sql_fragment": sql_fragment,
                "synonyms": rule_synonyms,
            },
        }
        entries.append(_dedup(entry, seen_hashes))

        # Synonym entries for each rule alias
        for syn in rule_synonyms:
            entry = {
                "type": "synonym",
                "name": syn,
                "text": f"Synonym: '{syn}' means '{term}'",
                "metadata": {"alias": syn, "canonical": term},
            }
            entries.append(_dedup(entry, seen_hashes))

    # ── 3. Verified queries ────────────────────────────────────────

    for vq in model.get("verified_queries", []):
        question = vq.get("question", "").strip()
        sql = vq.get("sql", "").strip()
        if not (question and sql):
            continue
        notes = vq.get("notes", "")
        text = f"Verified query: '{question}' → {sql}"
        if notes:
            text += f" Notes: {notes}"
        entry = {
            "type": "verified_query",
            "name": question[:100],
            "text": text,
            "metadata": {
                "question": question,
                "sql": sql,
                "use_as_onboarding": vq.get("use_as_onboarding", False),
                "notes": notes,
            },
        }
        entries.append(_dedup(entry, seen_hashes))

    # ── 4. Top-level relationships ─────────────────────────────────

    for rel in model.get("relationships", []):
        left = rel.get("left_table", "")
        right = rel.get("right_table", "")
        join_cols = rel.get("join_columns", {})
        left_col = join_cols.get("left", "")
        right_col = join_cols.get("right", "")
        cast_required = rel.get("cast_required", False)
        cast_type = rel.get("cast_type", "")
        notes = rel.get("notes", "")

        if not (left and right):
            continue

        text = f"Join: {left} to {right} on {left}.{left_col} = {right}.{right_col}"
        if cast_required:
            text += f" (requires TRY_CAST to {cast_type})"
        if notes:
            text += f". {notes}"

        entry = {
            "type": "relationship",
            "name": f"{left}__{right}",
            "text": text,
            "metadata": {
                "left_table": left,
                "right_table": right,
                "left_col": left_col,
                "right_col": right_col,
                "cast_required": cast_required,
                "cast_type": cast_type,
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
    openai_client: Any,
    pgvector_store: Any,
    batch_size: int = 50,
) -> int:
    """
    Embed glossary texts and store in pgvector glossary_embeddings table.

    Uses the project's OpenAIClient (Azure) for embeddings and
    PgVectorStore (psycopg2) for storage — same as other preprocessing
    scripts.
    """
    stored = 0
    for i in range(0, len(entries), batch_size):
        batch = entries[i : i + batch_size]
        text_strings = [e["text"] for e in batch]

        # Embed via Azure OpenAI (text-embedding-3-large, 3072 dims)
        embeddings = await openai_client.batch_embed(text_strings)

        for entry, embedding in zip(batch, embeddings):
            pgvector_store.insert(
                table_name="glossary_embeddings",
                embedding=embedding,
                metadata=entry.get("metadata", {}),
                source_id=f"{entry['type']}::{entry['name'][:200]}",
            )
            stored += 1

        logger.info("Embedded batch %d-%d / %d", i, i + len(batch), len(entries))

    return stored


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
    parser.add_argument("--output", default="data/glossary_entries.json")
    parser.add_argument("--embed", action="store_true", help="Embed and store in pgvector (requires config/raven_config.yaml)")
    parser.add_argument("--dry-run", action="store_true", help="Extract and show entries without embedding")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    model_path = Path(args.semantic_model)
    if not model_path.exists():
        logger.error("Semantic model not found: %s", model_path)
        sys.exit(1)

    with open(model_path) as f:
        model = yaml.safe_load(f) or {}

    entries = extract_glossary_entries(model)

    if args.dry_run:
        for e in entries:
            print(f"  [{e['type']:16s}]  {e['name'][:50]:50s}  {e['text'][:80]}...")
        print(f"\nTotal: {len(entries)} entries")
        return

    # Always save to file
    save_glossary_texts(entries, Path(args.output))

    # Embed and store using project connectors
    if args.embed:
        from src.raven.connectors.openai_client import OpenAIClient
        from src.raven.connectors.pgvector_store import PgVectorStore

        config_path = Path("config/raven_config.yaml")
        if not config_path.exists():
            logger.error("Config not found: %s — needed for OpenAI + pgvector creds", config_path)
            sys.exit(1)

        with open(config_path) as f:
            config = yaml.safe_load(f)

        openai_client = OpenAIClient(config)
        pgvector = PgVectorStore(config)
        pgvector.init_tables()

        stored = asyncio.run(embed_and_store(entries, openai_client, pgvector))
        logger.info("Stored %d glossary embeddings in pgvector", stored)
    else:
        logger.info("Saved %d entries to %s. Use --embed to store in pgvector.", len(entries), args.output)

    logger.info("Glossary build complete!")


if __name__ == "__main__":
    main()
