"""
Preprocessing: dbt Metadata Extraction
=======================================
Parses dbt manifest.json + YAML source files to produce:
  1. data/schema_catalog.json  — flat catalog of all tables with descriptions
  2. data/dbt_lineage_graph.gpickle — NetworkX directed graph of table deps
  3. pgvector: embeddings of table/column description strings

Usage:
    python -m preprocessing.extract_dbt_metadata \
        --manifest /path/to/manifest.json \
        --output-dir data/
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

import networkx as nx

logger = logging.getLogger(__name__)


def parse_manifest(manifest_path: Path) -> dict:
    """Parse dbt manifest.json and return raw manifest dict."""
    logger.info("Parsing manifest: %s", manifest_path)
    with open(manifest_path) as f:
        return json.load(f)


def extract_schema_catalog(manifest: dict) -> list[dict]:
    """
    Extract a flat catalog of tables with column info from dbt manifest.

    Each entry:
    {
        "table_name": "catalog.schema.table",
        "schema": "schema_name",
        "description": "Table description from dbt",
        "materialization": "table|view|incremental|ephemeral",
        "tags": ["tag1", "tag2"],
        "columns": [
            {
                "name": "col_name",
                "description": "Column description",
                "data_type": "VARCHAR",
                "tests": ["unique", "not_null"],
                "is_partition": false
            }
        ],
        "row_count_estimate": null,
        "depends_on": ["catalog.schema.upstream_table"]
    }
    """
    catalog = []

    # Process models (nodes)
    for node_id, node in manifest.get("nodes", {}).items():
        if node.get("resource_type") not in ("model", "seed", "snapshot"):
            continue

        table_name = _build_fqn(node)
        columns = _extract_columns(node)
        deps = _extract_dependencies(node, manifest)

        entry = {
            "table_name": table_name,
            "schema": node.get("schema", ""),
            "description": node.get("description", "").strip(),
            "materialization": node.get("config", {}).get("materialized", "unknown"),
            "tags": node.get("tags", []),
            "columns": columns,
            "row_count_estimate": None,
            "depends_on": deps,
        }
        catalog.append(entry)

    # Process sources
    for source_id, source in manifest.get("sources", {}).items():
        table_name = _build_fqn(source)
        columns = _extract_columns(source)

        entry = {
            "table_name": table_name,
            "schema": source.get("schema", ""),
            "description": source.get("description", "").strip(),
            "materialization": "source",
            "tags": source.get("tags", []),
            "columns": columns,
            "row_count_estimate": None,
            "depends_on": [],
        }
        catalog.append(entry)

    logger.info("Extracted %d tables from manifest", len(catalog))
    return catalog


def build_lineage_graph(manifest: dict) -> nx.DiGraph:
    """
    Build a NetworkX directed graph from dbt ref() dependencies.

    Nodes = table names (catalog.schema.table)
    Edges = dependency relationships (upstream → downstream)
    """
    G = nx.DiGraph()

    for node_id, node in manifest.get("nodes", {}).items():
        if node.get("resource_type") not in ("model", "seed", "snapshot"):
            continue

        table_name = _build_fqn(node)
        G.add_node(table_name, **{
            "description": node.get("description", "")[:200],
            "materialization": node.get("config", {}).get("materialized", "unknown"),
            "schema": node.get("schema", ""),
            "tags": node.get("tags", []),
        })

        # Add edges from dependencies
        for dep_id in node.get("depends_on", {}).get("nodes", []):
            dep_node = manifest.get("nodes", {}).get(dep_id) or manifest.get("sources", {}).get(dep_id)
            if dep_node:
                dep_name = _build_fqn(dep_node)
                G.add_node(dep_name)
                G.add_edge(dep_name, table_name, type="lineage")

    logger.info("Built lineage graph: %d nodes, %d edges", G.number_of_nodes(), G.number_of_edges())
    return G


def build_embedding_texts(catalog: list[dict]) -> list[dict]:
    """
    Build text strings for embedding. Each table becomes one text chunk:
    "table_name: description. Columns: col1 (type1) - desc1, col2 (type2) - desc2, ..."

    Returns list of dicts with 'text' and 'metadata' keys.
    """
    texts = []
    for entry in catalog:
        col_parts = []
        for col in entry.get("columns", []):
            col_str = f"{col['name']} ({col.get('data_type', 'unknown')})"
            if col.get("description"):
                col_str += f" - {col['description']}"
            col_parts.append(col_str)

        cols_text = ", ".join(col_parts) if col_parts else "no columns documented"
        text = f"{entry['table_name']}: {entry.get('description', 'no description')}. Columns: {cols_text}"

        texts.append({
            "text": text,
            "metadata": {
                "table_name": entry["table_name"],
                "schema": entry.get("schema", ""),
                "description": entry.get("description", ""),
                "materialization": entry.get("materialization", ""),
                "column_count": len(entry.get("columns", [])),
            },
        })

    return texts


async def embed_and_store(
    texts: list[dict],
    openai_client: Any,
    pgvector_store: Any,
    batch_size: int = 50,
) -> int:
    """Embed text chunks and store in pgvector schema_embeddings table."""
    stored = 0
    for i in range(0, len(texts), batch_size):
        batch = texts[i : i + batch_size]
        text_strings = [t["text"] for t in batch]
        embeddings = await openai_client.batch_embed(text_strings)

        for item, embedding in zip(batch, embeddings):
            pgvector_store.insert(
                table_name="schema_embeddings",
                embedding=embedding,
                metadata=item["metadata"],
                source_id=item["metadata"]["table_name"],
            )
            stored += 1

        logger.info("Embedded batch %d-%d (%d total)", i, i + len(batch), stored)

    return stored


# ── Helpers ────────────────────────────────────────────────────────────


def _build_fqn(node: dict) -> str:
    """Build fully-qualified table name: catalog.schema.name."""
    database = node.get("database", node.get("catalog", ""))
    schema = node.get("schema", "")
    name = node.get("name", node.get("alias", node.get("identifier", "")))
    parts = [p for p in [database, schema, name] if p]
    return ".".join(parts)


def _extract_columns(node: dict) -> list[dict]:
    """Extract column metadata from a dbt node."""
    columns = []
    for col_name, col_info in node.get("columns", {}).items():
        col = {
            "name": col_name,
            "description": col_info.get("description", "").strip(),
            "data_type": col_info.get("data_type", col_info.get("type", "")),
            "tests": [],
            "is_partition": False,
        }

        # Extract test info
        if col_info.get("meta", {}).get("partition"):
            col["is_partition"] = True
        for tag in col_info.get("tags", []):
            if tag in ("partition", "partition_key"):
                col["is_partition"] = True

        columns.append(col)

    return columns


def _extract_dependencies(node: dict, manifest: dict) -> list[str]:
    """Extract upstream table names from depends_on."""
    deps = []
    for dep_id in node.get("depends_on", {}).get("nodes", []):
        dep_node = manifest.get("nodes", {}).get(dep_id) or manifest.get("sources", {}).get(dep_id)
        if dep_node:
            deps.append(_build_fqn(dep_node))
    return deps


# ── CLI Entry Point ────────────────────────────────────────────────────


def save_catalog(catalog: list[dict], output_path: Path) -> None:
    """Save schema catalog to JSON."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(catalog, f, indent=2)
    logger.info("Saved schema catalog: %s (%d tables)", output_path, len(catalog))


def save_graph(graph: nx.DiGraph, output_path: Path) -> None:
    """Save NetworkX graph to pickle."""
    import pickle

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "wb") as f:
        pickle.dump(graph, f, pickle.HIGHEST_PROTOCOL)
    logger.info("Saved lineage graph: %s (%d nodes)", output_path, graph.number_of_nodes())


def main():
    parser = argparse.ArgumentParser(description="Extract dbt metadata for RAVEN")
    parser.add_argument("--manifest", required=True, help="Path to dbt manifest.json")
    parser.add_argument("--output-dir", default="data", help="Output directory")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    manifest_path = Path(args.manifest)
    output_dir = Path(args.output_dir)

    if not manifest_path.exists():
        logger.error("Manifest not found: %s", manifest_path)
        sys.exit(1)

    manifest = parse_manifest(manifest_path)
    catalog = extract_schema_catalog(manifest)
    graph = build_lineage_graph(manifest)

    save_catalog(catalog, output_dir / "schema_catalog.json")
    save_graph(graph, output_dir / "dbt_lineage_graph.gpickle")

    # Embedding texts (stored for later batch embedding)
    texts = build_embedding_texts(catalog)
    texts_path = output_dir / "schema_embedding_texts.json"
    with open(texts_path, "w") as f:
        json.dump(texts, f, indent=2)
    logger.info("Saved %d embedding texts to %s", len(texts), texts_path)

    logger.info("dbt metadata extraction complete!")


if __name__ == "__main__":
    main()
