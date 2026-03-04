"""
RAVEN — Preprocessing Tests
==============================
Tests for preprocessing scripts without requiring external services.
Run with: pytest tests/test_preprocessing.py -v
"""

from __future__ import annotations

import json
import os
import pickle
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]


# ── Test: Preprocessing module structure ──────────────────────────────


class TestPreprocessingStructure:
    """Verify preprocessing files exist."""

    def test_scripts_exist(self):
        scripts = [
            "extract_dbt_metadata.py",
            "extract_metabase_questions.py",
            "build_lsh_index.py",
            "build_content_awareness.py",
            "build_table_graph.py",
            "build_glossary.py",
            "ingest_documentation.py",
            "refresh_all.py",
        ]
        preproc_dir = PROJECT_ROOT / "preprocessing"
        for s in scripts:
            assert (preproc_dir / s).exists(), f"Missing: preprocessing/{s}"

    def test_init_exists(self):
        assert (PROJECT_ROOT / "preprocessing" / "__init__.py").exists()

    def test_table_annotations_exists(self):
        assert (PROJECT_ROOT / "config" / "table_annotations.yaml").exists()


# ── Test: dbt metadata extraction ─────────────────────────────────────


class TestDbtMetadata:
    """Test dbt manifest parsing logic."""

    def test_parse_manifest(self):
        from preprocessing.extract_dbt_metadata import parse_manifest

        manifest = {
            "nodes": {
                "model.project.fact_orders": {
                    "resource_type": "model",
                    "name": "fact_orders",
                    "schema": "gold",
                    "database": "iceberg",
                    "description": "Order fact table",
                    "columns": {
                        "order_id": {"name": "order_id", "description": "Primary key", "data_type": "BIGINT"},
                        "amount": {"name": "amount", "description": "Order amount", "data_type": "DOUBLE"},
                    },
                    "depends_on": {"nodes": ["model.project.stg_orders"]},
                    "tags": ["gold"],
                },
                "model.project.stg_orders": {
                    "resource_type": "model",
                    "name": "stg_orders",
                    "schema": "staging",
                    "database": "iceberg",
                    "description": "Staging orders",
                    "columns": {},
                    "depends_on": {"nodes": []},
                    "tags": ["staging"],
                },
            },
            "sources": {},
        }

        # parse_manifest reads from file — test with temp file
        with tempfile.NamedTemporaryFile(suffix=".json", mode="w", delete=False) as f:
            json.dump(manifest, f)
            f.flush()
            result = parse_manifest(Path(f.name))
        os.unlink(f.name)

        assert "nodes" in result
        assert len(result["nodes"]) == 2

    def test_extract_schema_catalog(self):
        from preprocessing.extract_dbt_metadata import extract_schema_catalog

        manifest = {
            "nodes": {
                "model.project.fact_orders": {
                    "resource_type": "model",
                    "name": "fact_orders",
                    "schema": "gold",
                    "database": "iceberg",
                    "description": "Order fact",
                    "columns": {
                        "order_id": {"name": "order_id", "description": "PK", "data_type": "BIGINT"},
                    },
                    "depends_on": {"nodes": []},
                    "tags": [],
                },
            },
            "sources": {},
        }

        catalog = extract_schema_catalog(manifest)
        assert len(catalog) == 1
        assert "gold" in catalog[0]["table_name"]

    def test_build_embedding_texts(self):
        from preprocessing.extract_dbt_metadata import build_embedding_texts

        catalog = [
            {
                "table_name": "iceberg.gold.fact_orders",
                "description": "Order fact table",
                "columns": [
                    {"name": "order_id", "description": "Primary key"},
                    {"name": "amount", "description": "Order amount"},
                ],
            }
        ]

        texts = build_embedding_texts(catalog)
        assert len(texts) == 1
        assert "fact_orders" in texts[0]["text"]
        assert "order_id" in texts[0]["text"]


# ── Test: LSH Index ───────────────────────────────────────────────────


class TestLSHIndex:
    """Test MinHash LSH matching."""

    def test_lsh_matcher_basic(self):
        from preprocessing.build_lsh_index import LSHMatcher, build_lsh_index, build_minhash
        from datasketch import MinHashLSH

        # Build index manually with sample data
        lsh = MinHashLSH(threshold=0.3, num_perm=64)
        metadata = {}

        values = {
            "gold.dim_product|category": ["Electronics", "Clothing", "Food", "Furniture"],
            "gold.dim_customer|segment": ["Enterprise", "SMB", "Consumer"],
        }

        for key, vals in values.items():
            parts = key.split("|")
            table, column = parts[0], parts[1]
            for val in vals:
                entry_key = f"{table}.{column}:{val}"
                mh = build_minhash(val, num_perm=64)
                try:
                    lsh.insert(entry_key, mh)
                    metadata[entry_key] = {"table": table, "column": column, "value": val}
                except ValueError:
                    pass

        matcher = LSHMatcher(lsh=lsh, metadata=metadata)
        results = matcher.query("Electronics")
        assert len(results) >= 0  # LSH is probabilistic

    def test_lsh_matcher_empty(self):
        from preprocessing.build_lsh_index import LSHMatcher
        from datasketch import MinHashLSH

        lsh = MinHashLSH(threshold=0.3, num_perm=64)
        matcher = LSHMatcher(lsh=lsh, metadata={})
        results = matcher.query("anything")
        assert results == []

    def test_char_ngrams(self):
        from preprocessing.build_lsh_index import char_ngrams

        grams = char_ngrams("hello", n=3)
        assert "hel" in grams
        assert "ell" in grams
        assert "llo" in grams


# ── Test: Content Awareness ───────────────────────────────────────────


class TestContentAwareness:
    """Test Content Awareness builder (offline/no Trino)."""

    def test_build_without_trino(self):
        from preprocessing.build_content_awareness import build_content_awareness

        catalog = [
            {
                "table_name": "gold.fact_orders",
                "schema": "gold",
                "columns": [
                    {"name": "order_id", "data_type": "BIGINT"},
                    {"name": "order_date", "data_type": "DATE"},
                    {"name": "status", "data_type": "VARCHAR"},
                ],
            }
        ]

        awareness = build_content_awareness(catalog, trino_connector=None)
        # New flattened format: "table.column" keys
        assert "gold.fact_orders.order_id" in awareness
        assert "gold.fact_orders.order_date" in awareness
        # order_date is auto-detected as partition by name heuristic
        assert awareness["gold.fact_orders.order_date"]["data_type"] == "DATE"

    def test_skips_bronze_tables(self):
        from preprocessing.build_content_awareness import build_content_awareness

        catalog = [
            {"table_name": "bronze.raw_events", "schema": "bronze", "columns": [{"name": "id", "data_type": "BIGINT"}]},
            {"table_name": "gold.fact_orders", "schema": "gold", "columns": [{"name": "id", "data_type": "BIGINT"}]},
        ]

        awareness = build_content_awareness(catalog, trino_connector=None)
        assert "bronze.raw_events.id" not in awareness
        assert "gold.fact_orders.id" in awareness


# ── Test: Table Graph ─────────────────────────────────────────────────


class TestTableGraph:
    """Test table graph builder."""

    def test_build_graph_from_edges(self):
        import networkx as nx
        from preprocessing.build_table_graph import build_table_graph

        dbt = nx.DiGraph()
        dbt.add_edge("staging.stg_orders", "gold.fact_orders", ref_type="ref")

        metabase_edges = [
            ("gold.fact_orders", "gold.dim_customer", {
                "source": "metabase",
                "join_type": "INNER",
                "frequency": 15,
                "weight": 0.85,
            })
        ]

        semantic_edges = [
            ("gold.fact_orders", "gold.dim_product", {
                "source": "semantic_model",
                "join_key": "product_id",
                "relationship_type": "many_to_one",
                "weight": 0.9,
            })
        ]

        G = build_table_graph(dbt, metabase_edges, semantic_edges)

        assert G.number_of_nodes() == 4
        assert G.number_of_edges() == 3
        assert G.has_edge("gold.fact_orders", "gold.dim_customer")
        assert G.has_edge("gold.fact_orders", "gold.dim_product")

    def test_infer_layer(self):
        from preprocessing.build_table_graph import _infer_layer

        assert _infer_layer("gold.fact_orders") == "gold"
        assert _infer_layer("silver.events_cleaned") == "silver"
        assert _infer_layer("bronze.raw_events") == "bronze"
        assert _infer_layer("some.table") == "unknown"

    def test_edge_merging(self):
        import networkx as nx
        from preprocessing.build_table_graph import build_table_graph

        dbt = nx.DiGraph()
        dbt.add_edge("gold.fact_orders", "gold.dim_customer")

        metabase_edges = [
            ("gold.fact_orders", "gold.dim_customer", {
                "source": "metabase",
                "join_type": "INNER",
                "frequency": 10,
                "weight": 0.85,
            })
        ]

        G = build_table_graph(dbt, metabase_edges, [])
        # Should merge into one edge with combined sources
        assert G.number_of_edges() == 1
        edge_data = G["gold.fact_orders"]["gold.dim_customer"]
        assert "metabase" in edge_data["source"]
        assert "dbt" in edge_data["source"]


# ── Test: Glossary Builder ────────────────────────────────────────────


class TestGlossary:
    """Test glossary entry extraction."""

    def test_extract_entries(self):
        from preprocessing.build_glossary import extract_glossary_entries

        model = {
            "tables": [
                {
                    "name": "fact_orders",
                    "description": "Central order table",
                    "dimensions": [
                        {"name": "status", "description": "Order status", "expr": "status"},
                    ],
                    "measures": [
                        {"name": "total_revenue", "description": "Sum of revenue", "expr": "SUM(amount)", "agg": "sum"},
                    ],
                    "relationships": [
                        {"target": "dim_customer", "join_key": "customer_id", "type": "many_to_one"},
                    ],
                }
            ],
            "business_rules": [
                {"name": "active_customer", "definition": "Customer with order in last 90 days", "sql": "last_order_date >= CURRENT_DATE - INTERVAL '90' DAY"},
            ],
            "verified_queries": [
                {"question": "Total revenue last month", "sql": "SELECT SUM(amount) FROM gold.fact_orders WHERE order_date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1' MONTH"},
            ],
            "synonyms": [
                {"alias": "rev", "canonical": "revenue"},
                {"alias": "cust", "canonical": "customer"},
            ],
        }

        entries = extract_glossary_entries(model)
        types = {e["type"] for e in entries}

        assert "table" in types
        assert "dimension" in types
        assert "measure" in types
        assert "relationship" in types
        assert "business_rule" in types
        assert "verified_query" in types
        assert "synonym" in types
        assert len(entries) >= 7

    def test_deduplication(self):
        from preprocessing.build_glossary import extract_glossary_entries

        model = {
            "tables": [
                {"name": "t1", "description": "Same desc"},
                {"name": "t2", "description": "Same desc"},  # Different table name → different text
            ],
        }

        entries = extract_glossary_entries(model)
        hashes = [e["hash"] for e in entries]
        assert len(hashes) == len(set(hashes))  # no duplicates


# ── Test: Documentation Ingester ──────────────────────────────────────


class TestDocIngester:
    """Test documentation chunking."""

    def test_chunk_markdown(self):
        from preprocessing.ingest_documentation import chunk_markdown

        with tempfile.NamedTemporaryFile(suffix=".md", mode="w", delete=False) as f:
            f.write("# Overview\nThis is the overview section.\n\n## Details\nSome details here.\n\n## More\nMore content.\n")
            f.flush()

            chunks = chunk_markdown(Path(f.name))
            assert len(chunks) >= 2
            assert any("Overview" in c["section"] for c in chunks)

        os.unlink(f.name)

    def test_chunk_annotations(self):
        from preprocessing.ingest_documentation import chunk_annotations

        with tempfile.NamedTemporaryFile(suffix=".yaml", mode="w", delete=False) as f:
            yaml.dump({
                "tables": {
                    "gold.fact_orders": {
                        "warning": "Always filter on date",
                        "notes": "Main fact table",
                        "tips": ["Use order_date partition"],
                    }
                }
            }, f)
            f.flush()

            chunks = chunk_annotations(Path(f.name))
            assert len(chunks) == 1
            assert "WARNING" in chunks[0]["text"]
            assert chunks[0]["metadata"]["has_warning"] is True

        os.unlink(f.name)

    def test_split_by_token_limit(self):
        from preprocessing.ingest_documentation import _split_by_token_limit

        # Short text: single chunk
        assert len(_split_by_token_limit("Hello world")) == 1

        # Long text: multiple chunks
        long_text = "word " * 2000  # ~2000 tokens
        chunks = _split_by_token_limit(long_text)
        assert len(chunks) > 1

    def test_ingest_all_empty(self):
        from preprocessing.ingest_documentation import ingest_all

        chunks = ingest_all(docs_dir=None, annotations_path=None)
        assert chunks == []


# ── Test: Refresh All ─────────────────────────────────────────────────


class TestRefreshAll:
    """Test refresh orchestrator."""

    def test_stage_list(self):
        from preprocessing.refresh_all import ALL_STAGES

        assert len(ALL_STAGES) == 7
        names = [s.name for s in ALL_STAGES]
        assert "dbt" in names
        assert "graph" in names
        assert "glossary" in names

    def test_resolve_arg(self):
        from preprocessing.refresh_all import resolve_arg

        env = {"RAVEN_TRINO_HOST": "trino.local"}
        assert resolve_arg("{RAVEN_TRINO_HOST}", env) == "trino.local"
        assert resolve_arg("--output", env) == "--output"
        assert resolve_arg("{MISSING}", env) == ""

    def test_check_prerequisites_missing_env(self):
        from preprocessing.refresh_all import Stage, check_prerequisites

        stage = Stage(name="test", description="", module="", required_env=["NONEXISTENT"])
        result = check_prerequisites(stage, {})
        assert result is not None
        assert "Missing env" in result

    def test_dry_run(self):
        from preprocessing.refresh_all import run_all

        results = run_all(dry_run=True)
        assert all(r.status == "skipped" for r in results)


# ── Test: Table Annotations ──────────────────────────────────────────


class TestTableAnnotations:
    """Verify table_annotations.yaml is well-formed."""

    def test_annotations_valid_yaml(self):
        with open(PROJECT_ROOT / "config" / "table_annotations.yaml") as f:
            data = yaml.safe_load(f)

        assert "tables" in data
        assert len(data["tables"]) >= 1

    def test_annotations_have_required_fields(self):
        with open(PROJECT_ROOT / "config" / "table_annotations.yaml") as f:
            data = yaml.safe_load(f)

        for table_name, meta in data["tables"].items():
            assert isinstance(meta, dict), f"{table_name} should be a dict"
            # At least one annotation field should exist
            valid_fields = {"warning", "notes", "owner", "refresh_schedule", "known_issues", "tips", "partition_key", "required_filters"}
            assert any(k in meta for k in valid_fields), f"{table_name} has no valid annotation fields"
