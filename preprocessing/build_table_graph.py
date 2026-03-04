"""
Preprocessing: Table Graph Builder
=====================================
Combines three edge sources into a unified table-relationship graph:
  1. dbt lineage edges (from manifest.json)
  2. Metabase JOIN patterns (from parsed SQL)
  3. Semantic model relationships (from semantic_model.yaml)

Output: data/table_graph.gpickle (NetworkX DiGraph)

The graph is used by the Schema Selector (QueryWeaver-style traversal)
to discover multi-hop table paths for complex queries.

Usage:
    python -m preprocessing.build_table_graph \
        --dbt-lineage data/dbt_lineage_graph.gpickle \
        --metabase-joins data/join_patterns.json \
        --semantic-model config/semantic_model.yaml \
        --output data/table_graph.gpickle
"""

from __future__ import annotations

import argparse
import json
import logging
import pickle
import sys
from pathlib import Path
from typing import Any

import networkx as nx

logger = logging.getLogger(__name__)

# ── Edge Source 1: dbt Lineage ─────────────────────────────────────────


def load_dbt_lineage(lineage_path: Path) -> nx.DiGraph:
    """Load dbt lineage graph from gpickle."""
    if not lineage_path.exists():
        logger.warning("dbt lineage not found: %s", lineage_path)
        return nx.DiGraph()

    with open(lineage_path, "rb") as f:
        graph = pickle.load(f)

    logger.info("Loaded dbt lineage: %d nodes, %d edges", graph.number_of_nodes(), graph.number_of_edges())
    return graph


# ── Edge Source 2: Metabase JOIN Patterns ──────────────────────────────


def load_metabase_joins(joins_path: Path) -> list[dict]:
    """Load Metabase JOIN patterns from JSON."""
    if not joins_path.exists():
        logger.warning("Metabase joins not found: %s", joins_path)
        return []

    with open(joins_path) as f:
        patterns = json.load(f)

    logger.info("Loaded %d Metabase JOIN patterns", len(patterns))
    return patterns


def build_metabase_edges(patterns: list[dict]) -> list[tuple[str, str, dict]]:
    """
    Convert Metabase JOIN patterns to graph edges.

    Each pattern: {"left_table": "x", "right_table": "y", "join_type": "INNER", "count": 5}
    """
    edges = []
    for p in patterns:
        left = _normalize_table_name(p.get("left_table", ""))
        right = _normalize_table_name(p.get("right_table", ""))
        if left and right and left != right:
            edges.append((left, right, {
                "source": "metabase",
                "join_type": p.get("join_type", "JOIN"),
                "frequency": p.get("count", 1),
                "weight": _join_weight(p.get("count", 1)),
            }))
    return edges


# ── Edge Source 3: Semantic Model Relationships ────────────────────────


def load_semantic_model(semantic_path: Path) -> dict:
    """Load semantic model YAML."""
    if not semantic_path.exists():
        logger.warning("Semantic model not found: %s", semantic_path)
        return {}

    import yaml
    with open(semantic_path) as f:
        return yaml.safe_load(f) or {}


def build_semantic_edges(model: dict) -> list[tuple[str, str, dict]]:
    """
    Extract edges from semantic model relationships.

    Expected structure:
    tables:
      - name: fact_orders
        relationships:
          - target: dim_customer
            join_key: customer_id
            type: many_to_one
    """
    edges = []
    tables = model.get("tables", [])

    for table in tables:
        table_name = _normalize_table_name(table.get("name", ""))
        if not table_name:
            continue

        for rel in table.get("relationships", []):
            target = _normalize_table_name(rel.get("target", ""))
            if not target:
                continue

            edges.append((table_name, target, {
                "source": "semantic_model",
                "join_key": rel.get("join_key", ""),
                "relationship_type": rel.get("type", "unknown"),
                "weight": 0.9,  # High confidence — human-defined
            }))

    return edges


# ── Graph Assembly ─────────────────────────────────────────────────────


def build_table_graph(
    dbt_lineage: nx.DiGraph,
    metabase_edges: list[tuple[str, str, dict]],
    semantic_edges: list[tuple[str, str, dict]],
) -> nx.DiGraph:
    """
    Merge all edge sources into a unified table graph.

    Node attributes:
      - layer: gold|silver|bronze (inferred from name)

    Edge attributes:
      - source: dbt|metabase|semantic_model
      - weight: 0.0-1.0 (higher = stronger relationship)
      - join_key: if known
    """
    G = nx.DiGraph()

    # Source 1: dbt lineage
    for u, v, data in dbt_lineage.edges(data=True):
        u_norm = _normalize_table_name(u)
        v_norm = _normalize_table_name(v)
        edge_data = {**data, "source": "dbt", "weight": 0.8}
        G.add_edge(u_norm, v_norm, **edge_data)

    # Source 2: Metabase JOINs
    for u, v, data in metabase_edges:
        if G.has_edge(u, v):
            existing = G[u][v]
            # Merge: keep highest weight, accumulate sources
            existing["weight"] = max(existing.get("weight", 0), data.get("weight", 0))
            sources = set(existing.get("source", "").split("+"))
            sources.add("metabase")
            existing["source"] = "+".join(sorted(sources))
            existing["metabase_frequency"] = data.get("frequency", 1)
        else:
            G.add_edge(u, v, **data)

    # Source 3: Semantic model relationships
    for u, v, data in semantic_edges:
        if G.has_edge(u, v):
            existing = G[u][v]
            existing["weight"] = max(existing.get("weight", 0), data.get("weight", 0))
            sources = set(existing.get("source", "").split("+"))
            sources.add("semantic_model")
            existing["source"] = "+".join(sorted(sources))
            existing["join_key"] = data.get("join_key", existing.get("join_key", ""))
            existing["relationship_type"] = data.get("relationship_type", "")
        else:
            G.add_edge(u, v, **data)

    # Annotate nodes with layer
    for node in G.nodes():
        G.nodes[node]["layer"] = _infer_layer(node)

    logger.info(
        "Built unified table graph: %d nodes, %d edges",
        G.number_of_nodes(),
        G.number_of_edges(),
    )

    # Log source breakdown
    source_counts: dict[str, int] = {}
    for _, _, data in G.edges(data=True):
        for src in data.get("source", "unknown").split("+"):
            source_counts[src] = source_counts.get(src, 0) + 1
    logger.info("Edge sources: %s", source_counts)

    return G


# ── Helpers ────────────────────────────────────────────────────────────


def _normalize_table_name(name: str) -> str:
    """Normalize table name to lowercase, strip catalog prefix."""
    name = name.strip().lower()
    # Remove catalog prefix if present: catalog.schema.table -> schema.table
    parts = name.split(".")
    if len(parts) == 3:
        return f"{parts[1]}.{parts[2]}"
    return name


def _infer_layer(table_name: str) -> str:
    """Infer data layer from table/schema name."""
    name = table_name.lower()
    if any(x in name for x in ("gold", "mart", "dim_", "fact_", "agg_")):
        return "gold"
    elif any(x in name for x in ("silver", "cleaned", "enriched")):
        return "silver"
    elif any(x in name for x in ("bronze", "raw", "staging", "stg_")):
        return "bronze"
    return "unknown"


def _join_weight(frequency: int) -> float:
    """Convert join frequency to weight (0-1)."""
    if frequency >= 20:
        return 0.95
    elif frequency >= 10:
        return 0.85
    elif frequency >= 5:
        return 0.7
    elif frequency >= 2:
        return 0.5
    return 0.3


# ── Persistence ────────────────────────────────────────────────────────


def save_graph(G: nx.DiGraph, output_path: Path) -> None:
    """Save graph to gpickle."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "wb") as f:
        pickle.dump(G, f)
    logger.info("Saved table graph: %s", output_path)


def save_graph_summary(G: nx.DiGraph, output_path: Path) -> None:
    """Save human-readable graph summary."""
    summary = {
        "nodes": G.number_of_nodes(),
        "edges": G.number_of_edges(),
        "layers": {},
        "top_connected": [],
        "edge_sources": {},
    }

    # Layer distribution
    for node in G.nodes():
        layer = G.nodes[node].get("layer", "unknown")
        summary["layers"][layer] = summary["layers"].get(layer, 0) + 1

    # Top connected nodes (by degree)
    degrees = sorted(G.degree(), key=lambda x: x[1], reverse=True)[:20]
    summary["top_connected"] = [{"table": n, "degree": d} for n, d in degrees]

    # Edge source distribution
    for _, _, data in G.edges(data=True):
        for src in data.get("source", "unknown").split("+"):
            summary["edge_sources"][src] = summary["edge_sources"].get(src, 0) + 1

    summary_path = output_path.with_suffix(".summary.json")
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)
    logger.info("Saved graph summary: %s", summary_path)


# ── CLI ────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="Build unified table graph for RAVEN")
    parser.add_argument("--dbt-lineage", default="data/dbt_lineage_graph.gpickle")
    parser.add_argument("--metabase-joins", default="data/join_patterns.json")
    parser.add_argument("--semantic-model", default="config/semantic_model.yaml")
    parser.add_argument("--output", default="data/table_graph.gpickle")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    # Load three edge sources
    dbt_lineage = load_dbt_lineage(Path(args.dbt_lineage))
    metabase_patterns = load_metabase_joins(Path(args.metabase_joins))
    semantic_model = load_semantic_model(Path(args.semantic_model))

    # Build edges from each source
    metabase_edges = build_metabase_edges(metabase_patterns)
    semantic_edges = build_semantic_edges(semantic_model)

    # Assemble unified graph
    G = build_table_graph(dbt_lineage, metabase_edges, semantic_edges)

    # Save
    save_graph(G, Path(args.output))
    save_graph_summary(G, Path(args.output))

    logger.info("Table graph build complete!")


if __name__ == "__main__":
    main()
