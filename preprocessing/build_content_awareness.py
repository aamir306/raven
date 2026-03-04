"""
Preprocessing: Content Awareness Builder
==========================================
Samples column patterns from Trino to build metadata about:
- Data types and format patterns
- Distinct value counts
- NULL percentages
- Enum values (for low-cardinality columns)
- Min/max for numeric and date columns
- Partition indicators

Output: data/content_awareness.json

Usage:
    python -m preprocessing.build_content_awareness \
        --catalog-path data/schema_catalog.json \
        --trino-host trino.example.com \
        --output data/content_awareness.json
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Limits
MAX_ENUM_DISTINCT = 50     # Max distinct values to store as enum
MAX_TABLES = 500            # Safety limit
SAMPLE_LIMIT = 10_000       # Sample size for stats


def load_schema_catalog(catalog_path: Path) -> list[dict]:
    """Load schema catalog from JSON."""
    with open(catalog_path) as f:
        return json.load(f)


def build_content_awareness(
    catalog: list[dict],
    trino_connector: Any | None = None,
) -> dict:
    """
    Build Content Awareness metadata for all tables in catalog.

    Returns:
    {
        "table_name": {
            "column_name": {
                "data_type": "VARCHAR",
                "distinct_count": 42,
                "null_pct": 1.5,
                "enum_values": ["a", "b", "c"],
                "format_pattern": "YYYY-MM-DD",
                "min": "2023-01-01",
                "max": "2026-03-03",
                "is_partition": true,
                "case_sensitive": true,
                "note": "Always include in WHERE"
            }
        }
    }
    """
    awareness: dict[str, dict] = {}

    tables_processed = 0
    for table in catalog[:MAX_TABLES]:
        table_name = table.get("table_name", "")
        schema = table.get("schema", "").lower()

        # Skip bronze/raw — focus on gold + silver
        if "bronze" in schema or "raw" in schema:
            continue

        columns = table.get("columns", [])
        if not columns:
            continue

        table_awareness = {}

        for col in columns:
            col_name = col.get("name", "")
            data_type = (col.get("data_type") or "").upper()
            is_partition = col.get("is_partition", False)

            col_meta: dict[str, Any] = {
                "data_type": data_type,
                "is_partition": is_partition,
            }

            # Auto-detect partition from name
            if col_name.lower() in ("ds", "dt", "date", "partition_date", "event_date"):
                col_meta["is_partition"] = True
                col_meta["note"] = "ALWAYS include in WHERE"

            if trino_connector:
                stats = _sample_column_stats(trino_connector, table_name, col_name, data_type)
                col_meta.update(stats)
            else:
                # Without Trino, use what we know from catalog
                col_meta["distinct_count"] = None
                col_meta["null_pct"] = None

            table_awareness[col_name] = col_meta

        if table_awareness:
            # Flatten to "table.column" keys for runtime ContentAwareness loader
            for col_name, col_meta in table_awareness.items():
                awareness[f"{table_name}.{col_name}"] = col_meta
            tables_processed += 1

    logger.info("Built Content Awareness for %d tables", tables_processed)
    return awareness


def _sample_column_stats(
    trino_connector: Any,
    table_name: str,
    column_name: str,
    data_type: str,
) -> dict:
    """Sample statistics for a single column from Trino."""
    stats: dict[str, Any] = {}

    try:
        # Basic stats: distinct count and NULL percentage
        sql = f"""
        SELECT
            approx_distinct({column_name}) AS distinct_count,
            COUNT(*) FILTER (WHERE {column_name} IS NULL) * 100.0 / NULLIF(COUNT(*), 0) AS null_pct
        FROM {table_name}
        """
        df = trino_connector.execute(sql)
        if df is not None and not df.empty:
            stats["distinct_count"] = int(df.iloc[0]["distinct_count"]) if df.iloc[0]["distinct_count"] else None
            stats["null_pct"] = round(float(df.iloc[0]["null_pct"]), 2) if df.iloc[0]["null_pct"] else 0.0

        # For low-cardinality string columns: get enum values
        if data_type in ("VARCHAR", "STRING", "TEXT", "CHAR", ""):
            distinct = stats.get("distinct_count")
            if distinct and distinct <= MAX_ENUM_DISTINCT:
                enum_sql = f"SELECT DISTINCT CAST({column_name} AS VARCHAR) AS val FROM {table_name} WHERE {column_name} IS NOT NULL LIMIT {MAX_ENUM_DISTINCT}"
                enum_df = trino_connector.execute(enum_sql)
                if enum_df is not None and not enum_df.empty:
                    values = enum_df.iloc[:, 0].dropna().tolist()
                    stats["enum_values"] = sorted(str(v) for v in values)
                    # Check case sensitivity
                    lower_set = {str(v).lower() for v in values}
                    stats["case_sensitive"] = len(lower_set) < len(values)

        # For numeric columns: min/max
        elif data_type in ("BIGINT", "INTEGER", "DOUBLE", "REAL", "DECIMAL", "SMALLINT", "TINYINT"):
            range_sql = f"SELECT MIN({column_name}) AS min_val, MAX({column_name}) AS max_val FROM {table_name}"
            range_df = trino_connector.execute(range_sql)
            if range_df is not None and not range_df.empty:
                stats["min"] = str(range_df.iloc[0]["min_val"]) if range_df.iloc[0]["min_val"] is not None else None
                stats["max"] = str(range_df.iloc[0]["max_val"]) if range_df.iloc[0]["max_val"] is not None else None

        # For date columns: min/max + format
        elif data_type in ("DATE", "TIMESTAMP"):
            range_sql = f"SELECT MIN({column_name}) AS min_val, MAX({column_name}) AS max_val FROM {table_name}"
            range_df = trino_connector.execute(range_sql)
            if range_df is not None and not range_df.empty:
                stats["min"] = str(range_df.iloc[0]["min_val"]) if range_df.iloc[0]["min_val"] is not None else None
                stats["max"] = str(range_df.iloc[0]["max_val"]) if range_df.iloc[0]["max_val"] is not None else None
                stats["format_pattern"] = "YYYY-MM-DD" if data_type == "DATE" else "YYYY-MM-DD HH:MM:SS"

    except Exception as e:
        logger.debug("Stats sampling failed for %s.%s: %s", table_name, column_name, e)

    return stats


def _detect_format_pattern(values: list[str]) -> str | None:
    """Detect common format patterns from sample values."""
    import re

    if not values:
        return None

    sample = [str(v) for v in values[:10]]

    # Check for zero-padded numbers
    if all(re.match(r"^\d{6}$", v) for v in sample if v):
        return "6-digit zero-padded string"
    if all(re.match(r"^\d{4}$", v) for v in sample if v):
        return "4-digit code"

    # Check for email-like
    if all("@" in v for v in sample if v):
        return "email"

    # Check for UUID-like
    if all(re.match(r"^[0-9a-f-]{36}$", v.lower()) for v in sample if v):
        return "UUID"

    return None


# ── CLI Entry Point ────────────────────────────────────────────────────


def save_awareness(awareness: dict, output_path: Path) -> None:
    """Save Content Awareness to JSON."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(awareness, f, indent=2)
    logger.info("Saved Content Awareness: %s (%d tables)", output_path, len(awareness))


def main():
    parser = argparse.ArgumentParser(description="Build Content Awareness for RAVEN")
    parser.add_argument("--catalog-path", default="data/schema_catalog.json")
    parser.add_argument("--trino-host", help="Trino host (optional)")
    parser.add_argument("--trino-port", type=int, default=8080)
    parser.add_argument("--trino-user", default="raven")
    parser.add_argument("--trino-catalog", default="iceberg")
    parser.add_argument("--output", default="data/content_awareness.json")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    catalog_path = Path(args.catalog_path)
    if not catalog_path.exists():
        logger.error("Catalog not found: %s", catalog_path)
        sys.exit(1)

    catalog = load_schema_catalog(catalog_path)

    trino_connector = None
    if args.trino_host:
        from src.raven.connectors.trino_connector import TrinoConnector

        trino_connector = TrinoConnector(
            host=args.trino_host,
            port=args.trino_port,
            user=args.trino_user,
            catalog=args.trino_catalog,
        )

    awareness = build_content_awareness(catalog, trino_connector=trino_connector)
    save_awareness(awareness, Path(args.output))

    logger.info("Content Awareness build complete!")


if __name__ == "__main__":
    main()
