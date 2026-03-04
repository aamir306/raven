"""
Preprocessing: MinHash LSH Index Builder
==========================================
Builds a MinHash LSH index for fuzzy entity matching.
Samples categorical column values from gold + silver Trino tables
and indexes them for local (no-API) real-time entity matching.

This enables:
- "Enterprise" → matches gold.dim_customers.segment = "Enterprise"
- "Enterprize" → still matches (fuzzy via character n-gram MinHash)
- "Batch 42" → matches gold.batches.batch_name = "Batch 42"

Usage:
    python -m preprocessing.build_lsh_index \
        --catalog-path data/schema_catalog.json \
        --trino-host trino.example.com \
        --output data/lsh_index.pkl
"""

from __future__ import annotations

import argparse
import json
import logging
import pickle
import sys
from pathlib import Path
from typing import Any

from datasketch import MinHash, MinHashLSH

logger = logging.getLogger(__name__)

# LSH config
LSH_THRESHOLD = 0.3       # Jaccard similarity threshold
LSH_NUM_PERM = 128        # Number of hash permutations
MAX_DISTINCT_VALUES = 10_000  # Skip columns with more distinct values
MAX_TABLES = 500           # Safety: max tables to process
NGRAM_SIZE = 3             # Character n-gram size


def load_schema_catalog(catalog_path: Path) -> list[dict]:
    """Load schema catalog from JSON."""
    with open(catalog_path) as f:
        catalog = json.load(f)
    logger.info("Loaded catalog: %d tables", len(catalog))
    return catalog


def get_categorical_columns(catalog: list[dict]) -> list[dict]:
    """
    Identify categorical columns suitable for LSH indexing.

    Criteria:
    - VARCHAR/STRING data type
    - In gold or silver schemas (skip bronze — raw data)
    - Not obviously high-cardinality (IDs, UUIDs) based on column name
    """
    HIGH_CARDINALITY_PATTERNS = {
        "id", "uuid", "guid", "hash", "token", "key", "password",
        "email", "phone", "ip", "url", "path", "timestamp", "created_at",
        "updated_at", "deleted_at",
    }

    candidates = []
    for table in catalog:
        table_name = table.get("table_name", "")
        schema = table.get("schema", "").lower()

        # Only gold + silver layers (or any non-bronze)
        if "bronze" in schema or "raw" in schema:
            continue

        for col in table.get("columns", []):
            col_name = col.get("name", "").lower()
            dtype = col.get("data_type", "").upper()

            # Filter data types
            if dtype not in ("VARCHAR", "STRING", "TEXT", "CHAR", ""):
                continue

            # Skip likely high-cardinality columns
            if any(pattern in col_name for pattern in HIGH_CARDINALITY_PATTERNS):
                continue

            candidates.append({
                "table": table_name,
                "column": col["name"],
                "data_type": dtype,
            })

    logger.info("Found %d categorical column candidates for LSH", len(candidates))
    return candidates


def sample_column_values(
    trino_connector: Any,
    table: str,
    column: str,
    limit: int = MAX_DISTINCT_VALUES,
) -> list[str]:
    """Sample distinct values from a column via Trino."""
    sql = f"SELECT DISTINCT CAST({column} AS VARCHAR) FROM {table} WHERE {column} IS NOT NULL LIMIT {limit}"
    try:
        df = trino_connector.execute(sql)
        if df is not None and not df.empty:
            return df.iloc[:, 0].dropna().astype(str).tolist()
    except Exception as e:
        logger.debug("Failed to sample %s.%s: %s", table, column, e)
    return []


def char_ngrams(text: str, n: int = NGRAM_SIZE) -> list[str]:
    """Generate character n-grams from text."""
    text = text.lower().strip()
    if len(text) < n:
        return [text]
    return [text[i : i + n] for i in range(len(text) - n + 1)]


def build_minhash(text: str, num_perm: int = LSH_NUM_PERM) -> MinHash:
    """Create a MinHash from a text string using character n-grams."""
    m = MinHash(num_perm=num_perm)
    for ngram in char_ngrams(text, NGRAM_SIZE):
        m.update(ngram.encode("utf-8"))
    return m


def build_lsh_index(
    columns: list[dict],
    trino_connector: Any | None = None,
    preloaded_values: dict[str, list[str]] | None = None,
) -> tuple[MinHashLSH, dict]:
    """
    Build MinHash LSH index from column values.

    Args:
        columns: List of {table, column, data_type} dicts
        trino_connector: Optional Trino connector for live sampling
        preloaded_values: Optional pre-sampled values {table.column: [values]}

    Returns:
        (lsh_index, index_metadata)
    """
    lsh = MinHashLSH(threshold=LSH_THRESHOLD, num_perm=LSH_NUM_PERM)
    metadata: dict[str, dict] = {}  # key → {table, column, value}
    total_values = 0
    skipped = 0

    for col_info in columns[:MAX_TABLES]:
        table = col_info["table"]
        column = col_info["column"]
        col_key = f"{table}.{column}"

        # Get values
        if preloaded_values and col_key in preloaded_values:
            values = preloaded_values[col_key]
        elif trino_connector:
            values = sample_column_values(trino_connector, table, column)
        else:
            continue

        if not values:
            continue

        # Skip if too high cardinality
        if len(values) > MAX_DISTINCT_VALUES:
            skipped += 1
            continue

        for value in values:
            value_str = str(value).strip()
            if not value_str or len(value_str) < 2:
                continue

            key = f"{table}.{column}::{value_str}"
            mh = build_minhash(value_str)

            try:
                lsh.insert(key, mh)
                metadata[key] = {
                    "table": table,
                    "column": column,
                    "value": value_str,
                }
                total_values += 1
            except ValueError:
                # Duplicate key — skip
                pass

    logger.info(
        "LSH index built: %d values indexed, %d columns skipped (high cardinality)",
        total_values,
        skipped,
    )
    return lsh, metadata


class LSHMatcher:
    """
    Runtime matcher: queries the LSH index for entity matching.

    Used by Stage 2 (Context Retrieval) — runs entirely locally.
    """

    def __init__(self, lsh: MinHashLSH, metadata: dict[str, dict]):
        self.lsh = lsh
        self.metadata = metadata

    def query(self, text: str, max_results: int = 10) -> list[dict]:
        """
        Query the LSH index for matches.

        Returns list of: {table, column, value, similarity}
        """
        mh = build_minhash(text)
        results = []

        try:
            matches = self.lsh.query(mh)
        except Exception:
            return []

        for key in matches[:max_results]:
            meta = self.metadata.get(key, {})
            if meta:
                # Estimate Jaccard similarity
                stored_mh = build_minhash(meta.get("value", ""))
                similarity = mh.jaccard(stored_mh)

                results.append({
                    "table": meta["table"],
                    "column": meta["column"],
                    "value": meta["value"],
                    "similarity": round(similarity, 3),
                })

        # Sort by similarity descending
        results.sort(key=lambda x: x["similarity"], reverse=True)
        return results

    @classmethod
    def load(cls, index_path: Path) -> "LSHMatcher":
        """Load a saved LSH index from pickle."""
        with open(index_path, "rb") as f:
            data = pickle.load(f)
        return cls(lsh=data["lsh"], metadata=data["metadata"])


def save_index(lsh: MinHashLSH, metadata: dict, output_path: Path) -> None:
    """Save LSH index and metadata to pickle."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "wb") as f:
        pickle.dump({"lsh": lsh, "metadata": metadata}, f, pickle.HIGHEST_PROTOCOL)
    logger.info("Saved LSH index: %s (%d entries)", output_path, len(metadata))


# ── CLI Entry Point ────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="Build MinHash LSH index for RAVEN")
    parser.add_argument("--catalog-path", default="data/schema_catalog.json")
    parser.add_argument("--trino-host", help="Trino host (optional — for live sampling)")
    parser.add_argument("--trino-port", type=int, default=8080)
    parser.add_argument("--trino-user", default="raven")
    parser.add_argument("--trino-catalog", default="iceberg")
    parser.add_argument("--output", default="data/lsh_index.pkl")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    catalog_path = Path(args.catalog_path)
    if not catalog_path.exists():
        logger.error("Catalog not found: %s", catalog_path)
        sys.exit(1)

    catalog = load_schema_catalog(catalog_path)
    columns = get_categorical_columns(catalog)

    # Build with optional Trino connection
    trino_connector = None
    if args.trino_host:
        from src.raven.connectors.trino_connector import TrinoConnector

        trino_connector = TrinoConnector(
            host=args.trino_host,
            port=args.trino_port,
            user=args.trino_user,
            catalog=args.trino_catalog,
        )

    lsh, metadata = build_lsh_index(columns, trino_connector=trino_connector)
    save_index(lsh, metadata, Path(args.output))

    logger.info("LSH index build complete!")


if __name__ == "__main__":
    main()
