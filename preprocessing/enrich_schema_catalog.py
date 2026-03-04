"""
Preprocessing: Enrich Schema Catalog from Trino information_schema
====================================================================
Fills in missing column metadata by querying Trino's information_schema.columns
for tables that have empty columns in the schema catalog (i.e., dbt didn't
define them).

Usage:
    python -m preprocessing.enrich_schema_catalog \
        --catalog-path data/schema_catalog.json \
        --output data/schema_catalog.json
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

import trino
from trino.auth import BasicAuthentication

logger = logging.getLogger(__name__)


def load_catalog(path: Path) -> list[dict]:
    with open(path) as f:
        return json.load(f)


def get_trino_connection():
    """Create a Trino connection from environment variables."""
    host = os.getenv("TRINO_HOST", "")
    port = int(os.getenv("TRINO_PORT", "443"))
    user = os.getenv("TRINO_USER", "dbt_user")
    password = os.getenv("TRINO_PASSWORD", "")
    catalog = os.getenv("TRINO_CATALOG", "cdp")

    kwargs = {
        "host": host,
        "port": port,
        "user": user,
        "catalog": catalog,
        "http_scheme": "https",
        "verify": False,
    }
    if password:
        kwargs["auth"] = BasicAuthentication(user, password)

    conn = trino.dbapi.connect(**kwargs)
    return conn, catalog


def fetch_all_columns(conn, catalog: str) -> dict[str, list[dict]]:
    """
    Fetch all columns from information_schema.columns grouped by table.

    Returns: {"schema.table": [{"name": ..., "data_type": ..., ...}, ...]}
    """
    sql = f"""
    SELECT
        table_schema,
        table_name,
        column_name,
        data_type,
        is_nullable,
        ordinal_position
    FROM {catalog}.information_schema.columns
    WHERE table_schema NOT IN ('information_schema')
    ORDER BY table_schema, table_name, ordinal_position
    """
    logger.info("Querying information_schema.columns (this may take a few minutes)...")
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    logger.info("Fetched %d column rows from information_schema", len(rows))

    # Group by fully-qualified table name
    table_columns: dict[str, list[dict]] = {}
    for row in rows:
        schema, table, col_name, dtype, nullable, ordinal = row
        fqn = f"{catalog}.{schema}.{table}"
        if fqn not in table_columns:
            table_columns[fqn] = []
        table_columns[fqn].append({
            "name": col_name,
            "data_type": dtype,
            "is_nullable": nullable == "YES",
            "ordinal_position": ordinal,
            "description": "",
            "is_partition": False,
        })

    logger.info("Got columns for %d tables from information_schema", len(table_columns))
    return table_columns


def enrich_catalog(
    catalog: list[dict],
    trino_columns: dict[str, list[dict]],
) -> tuple[list[dict], int, int]:
    """
    Enrich catalog entries that have empty columns with Trino information_schema data.

    Returns: (enriched_catalog, tables_enriched, tables_already_had_columns)
    """
    enriched = 0
    already_had = 0

    for entry in catalog:
        table_name = entry.get("table_name", "")
        existing_cols = entry.get("columns", [])

        if existing_cols:
            already_had += 1
            continue

        # Try exact match
        trino_cols = trino_columns.get(table_name)

        # Try without catalog prefix
        if not trino_cols:
            parts = table_name.split(".")
            if len(parts) == 3:
                short = f"{parts[1]}.{parts[2]}"
                # Try finding with cdp prefix
                for key in trino_columns:
                    if key.endswith(f".{short}") or key == f"cdp.{short}":
                        trino_cols = trino_columns[key]
                        break

        if trino_cols:
            entry["columns"] = trino_cols
            enriched += 1

    return catalog, enriched, already_had


def main():
    parser = argparse.ArgumentParser(description="Enrich schema catalog with Trino column metadata")
    parser.add_argument("--catalog-path", default="data/schema_catalog.json")
    parser.add_argument("--output", default="data/schema_catalog.json")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    # Load .env
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    catalog_path = Path(args.catalog_path)
    if not catalog_path.exists():
        logger.error("Catalog not found: %s", catalog_path)
        sys.exit(1)

    catalog = load_catalog(catalog_path)
    logger.info("Loaded catalog: %d tables, %d with columns",
                len(catalog), sum(1 for t in catalog if t.get("columns")))

    conn, cat_name = get_trino_connection()
    trino_columns = fetch_all_columns(conn, cat_name)
    conn.close()

    catalog, enriched, already_had = enrich_catalog(catalog, trino_columns)

    logger.info("Enrichment: %d tables enriched, %d already had columns, %d still empty",
                enriched, already_had, len(catalog) - enriched - already_had)

    # Save
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(catalog, f, indent=2)
    logger.info("Saved enriched catalog: %s", output_path)


if __name__ == "__main__":
    main()
