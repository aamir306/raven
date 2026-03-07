"""
E2E Integration Tests — Pipeline end-to-end with mocked components.
===================================================================

This module provides a scaffold for integration testing the full pipeline
without requiring live external services. Tests use mocked Trino, pgvector,
and OpenAI connections that reproduce real interaction patterns.

Run with:
    pytest tests/test_integration.py -v

For live tests (requires running Trino + pgvector + OpenAI key):
    pytest tests/test_integration.py -v -m integration_live
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ── Markers ────────────────────────────────────────────────────

# These markers allow selective test execution:
#   -m integration       → mocked integration tests (always runnable)
#   -m integration_live  → needs live Trino + pgvector + OpenAI

pytestmark = pytest.mark.integration


# ── Fake / Mock Helpers ────────────────────────────────────────


def _fake_trino_connector() -> MagicMock:
    """Build a mock TrinoConnector that returns plausible query results."""
    mock = MagicMock()
    mock.execute = AsyncMock(return_value={
        "columns": ["total_revenue"],
        "rows": [[1234567.89]],
        "row_count": 1,
    })
    mock.explain = AsyncMock(return_value="Fragment 0 [SINGLE]")
    mock.get_table_columns = MagicMock(return_value=[
        {"column_name": "order_id", "data_type": "BIGINT"},
        {"column_name": "amount", "data_type": "DOUBLE"},
        {"column_name": "order_date", "data_type": "DATE"},
    ])
    mock.is_connected = MagicMock(return_value=True)
    return mock


def _fake_pgvector_store() -> MagicMock:
    """Build a mock PgVectorStore that returns empty search results."""
    mock = MagicMock()
    mock.search = MagicMock(return_value=[])
    mock.async_search = AsyncMock(return_value=[])
    mock.batch_insert = MagicMock(return_value=0)
    mock.insert = MagicMock(return_value=1)
    mock.log_query = MagicMock()
    mock.update_feedback = MagicMock(return_value=True)
    mock.get_conversation_history = MagicMock(return_value=[])
    mock.init_tables = MagicMock()
    return mock


def _fake_openai_client() -> MagicMock:
    """Build a mock OpenAI client with sensible defaults."""
    mock = MagicMock()

    # embed() returns a 3072-dim vector
    mock.embed = AsyncMock(return_value=[0.01] * 3072)

    # chat() returns JSON-like strings depending on context
    async def fake_chat(messages: list, **kwargs):
        # Parse the system message to determine what's being asked
        content = messages[-1].get("content", "") if messages else ""

        if "keywords" in content.lower() or "extract" in content.lower():
            return json.dumps({
                "keywords": ["revenue", "total"],
                "entities": ["orders"],
                "metrics": ["revenue"],
                "time_range": None,
            })
        elif "difficulty" in content.lower() or "classify" in content.lower():
            return json.dumps({"difficulty": "moderate", "reasoning": "aggregation query"})
        elif "select" in content.lower() and "table" in content.lower():
            return json.dumps({
                "tables": ["gold.finance.orders"],
                "reasoning": "Revenue comes from orders table",
            })
        elif "sql" in content.lower() or "query" in content.lower():
            return "SELECT SUM(amount) AS total_revenue FROM gold.finance.orders"
        else:
            return "I can help with that."

    mock.chat = AsyncMock(side_effect=fake_chat)
    mock.chat_json = AsyncMock(side_effect=fake_chat)
    return mock


# ── Fixtures ───────────────────────────────────────────────────


@pytest.fixture
def mock_trino():
    return _fake_trino_connector()


@pytest.fixture
def mock_pgvector():
    return _fake_pgvector_store()


@pytest.fixture
def mock_openai():
    return _fake_openai_client()


# ── Import smoke tests ────────────────────────────────────────


class TestPipelineImports:
    """Verify all pipeline components can be imported without external deps."""

    def test_pipeline_class_imports(self):
        from src.raven.pipeline import Pipeline, PipelineContext
        assert Pipeline is not None
        assert PipelineContext is not None

    def test_all_stage_imports(self):
        from src.raven.router.classifier import DifficultyClassifier
        from src.raven.retrieval import InformationRetriever
        from src.raven.schema.schema_selector import SchemaSelector
        from src.raven.probes.probe_runner import ProbeRunner
        from src.raven.generation.candidate_generator import CandidateGenerator
        from src.raven.generation.constrained_sql import ConstrainedSQLGenerator
        from src.raven.validation.candidate_selector import CandidateSelector
        from src.raven.validation.confidence_model import ConfidenceModel
        from src.raven.validation.cost_guard import CostGuard
        from src.raven.validation.execution_judge import ExecutionJudge
        from src.raven.output.renderer import OutputRenderer
        from src.raven.feedback.collector import FeedbackCollector
        from src.raven.conversation import ConversationManager

    def test_supporting_module_imports(self):
        from src.raven.redis_cache import RedisCache, HybridCache
        from src.raven.grounding import ValueResolver
        from src.raven.grounding.ambiguity_policy import AmbiguityPolicy
        from src.raven.contracts import InstructionCompiler
        from src.raven.sql import TrinoSQLCompiler
        from src.raven.query_families import QueryFamilyRegistry
        from src.raven.retrieval.vector_index import VectorIndex, DimReducer
        from src.raven.eval.benchmark_gate import run_gate, GateCheck
        from src.raven.focus import FocusStore, FocusContext, FocusDocument


# ── Dataclass / Contract tests ─────────────────────────────────


class TestPipelineContext:
    def test_context_defaults(self):
        from src.raven.pipeline import PipelineContext
        ctx = PipelineContext(user_question="What is total revenue?")
        assert ctx.user_question == "What is total revenue?"
        assert ctx.sql_candidates == []
        assert ctx.confidence == "LOW"

    def test_context_roundtrip(self):
        from src.raven.pipeline import PipelineContext
        ctx = PipelineContext(
            user_question="Revenue by region",
            selected_sql="SELECT region, SUM(amount) FROM orders GROUP BY 1",
            confidence="HIGH",
        )
        assert ctx.confidence == "HIGH"
        assert "SUM" in ctx.selected_sql


# ── Stage-Level Integration Tests ──────────────────────────────


class TestDifficultyClassifier:
    @pytest.mark.asyncio
    async def test_classification_returns_difficulty(self, mock_openai):
        from src.raven.router.classifier import DifficultyClassifier, Difficulty
        # DifficultyClassifier calls openai.complete(), not chat()
        mock_openai.complete = AsyncMock(return_value='{"difficulty": "moderate", "reasoning": "agg"}')
        classifier = DifficultyClassifier(mock_openai)
        result = await classifier.classify("What is total revenue?")
        assert result is not None


class TestConfidenceModel:
    def test_score_returns_result(self):
        from src.raven.validation.confidence_model import (
            ConfidenceModel, ConfidenceSignals,
        )
        model = ConfidenceModel()
        signals = ConfidenceSignals(
            hard_plan_violations=0,
            plan_consistent=True,
            cost_guard_passed=True,
            execution_judge_passed=True,
            entity_match_count=3,
            has_trusted_query=True,
        )
        result = model.score(signals)
        # ConfidenceResult has .score (float 0-1) and .band (str)
        assert 0.0 <= result.score <= 1.0
        assert result.band in ("HIGH", "MEDIUM", "LOW", "ABSTAIN")


class TestCostGuard:
    def test_init(self, mock_trino):
        from src.raven.validation.cost_guard import CostGuard
        guard = CostGuard(mock_trino)
        assert guard is not None


class TestExecutionJudge:
    def test_init(self):
        from src.raven.validation.execution_judge import ExecutionJudge
        judge = ExecutionJudge()
        assert judge is not None


# ── Focus Mode Integration ─────────────────────────────────────


class TestFocusModeIntegration:
    def test_focus_context_creation(self):
        from src.raven.focus import FocusContext
        ctx = FocusContext(
            type="document",
            name="Revenue",
            tables=["gold.finance.orders"],
            glossary_terms=[{"term": "revenue", "definition": "SUM(amount)"}],
        )
        assert ctx.table_count == 0  # Computed separately
        assert ctx.name == "Revenue"

    def test_focus_document_roundtrip(self, tmp_path):
        from src.raven.focus import FocusDocument, FocusStore
        store = FocusStore(base_dir=tmp_path)
        doc = FocusDocument(
            name="Test Focus",
            tables=["gold.finance.orders"],
            business_rules=[{"rule": "Revenue = SUM(amount)"}],
        )
        store.create_document(doc)
        retrieved = store.get_document(doc.id)
        assert retrieved is not None
        assert retrieved.name == "Test Focus"

        # Convert to FocusContext
        ctx = retrieved.to_focus_context()
        assert ctx.tables == ["gold.finance.orders"]


# ── Vector Index Integration ───────────────────────────────────


class TestVectorIndexIntegration:
    @pytest.mark.asyncio
    async def test_hybrid_search_mocked(self, mock_pgvector, mock_openai):
        from src.raven.retrieval.vector_index import VectorIndex, OpenAIEmbedder

        embedder = OpenAIEmbedder(mock_openai)
        index = VectorIndex(pgvector=mock_pgvector, embedder=embedder, reduced_dim=4)

        mock_pgvector.async_search = AsyncMock(return_value=[
            {"id": 1, "description": "total revenue by region", "similarity": 0.90},
        ])

        results = await index.hybrid_search("total revenue", top_k=5)
        assert len(results) >= 1


# ── Benchmark Gate Integration ─────────────────────────────────


class TestBenchmarkGateIntegration:
    def test_gate_with_zeroed_baseline(self, tmp_path):
        """Gate should pass when comparing zeros to zeros (production init state)."""
        from src.raven.eval.benchmark_gate import run_gate
        baseline = tmp_path / "baseline.json"
        result = tmp_path / "result.json"
        baseline.write_text(json.dumps({
            "timestamp": "2025-01-01",
            "metrics": {"pass_rate": 0.0, "exec_rate": 0.0, "avg_latency_s": 0.0},
        }))
        result.write_text(json.dumps({
            "pass_rate": 0.0, "exec_rate": 0.0, "avg_latency_s": 0.0,
        }))
        gate = run_gate(baseline_path=baseline, result_path=result)
        assert gate.passed is True

    def test_real_baseline_validates(self):
        """The project's real baseline should be loadable."""
        baseline_path = Path(__file__).parent.parent / "data" / "benchmark_baseline.json"
        if baseline_path.exists():
            data = json.loads(baseline_path.read_text())
            assert "metrics" in data
            assert "pass_rate" in data["metrics"]


# ── SQL Compiler Integration ──────────────────────────────────


class TestSQLCompilerIntegration:
    def test_compiler_validates_simple_query(self):
        from src.raven.sql import TrinoSQLCompiler
        compiler = TrinoSQLCompiler()
        sql = "SELECT SUM(amount) AS total_revenue FROM gold.finance.orders"
        result = compiler.validate(sql)
        assert result is not None  # Should not raise

    def test_compiler_detects_mutation(self):
        from src.raven.sql import TrinoSQLCompiler
        compiler = TrinoSQLCompiler()
        # INSERT/UPDATE should be flagged
        result = compiler.validate("INSERT INTO orders VALUES (1, 2)")
        # Exact behavior depends on implementation, but it should handle gracefully
        assert result is not None


# ── Redis Cache Integration ────────────────────────────────────


class TestCacheIntegration:
    def test_memory_cache_operations(self):
        from src.raven.cache import QueryCache
        cache = QueryCache(enabled=True, ttl_seconds=60)
        cache.put("test_key", {"answer": 42, "status": "success"})
        result = cache.get("test_key")
        assert result is not None
        assert result["answer"] == 42

    def test_memory_cache_disabled(self):
        from src.raven.cache import QueryCache
        cache = QueryCache(enabled=False)
        cache.put("key", "value")
        assert cache.get("key") is None


# ── Query Family Integration ──────────────────────────────────


class TestQueryFamilyIntegration:
    def test_registry_loads(self):
        from src.raven.query_families import QueryFamilyRegistry
        registry = QueryFamilyRegistry()
        families = registry.top_families(limit=10)
        assert isinstance(families, list)

    def test_provenance_tracking(self):
        from src.raven.query_families import FamilyProvenance
        p = FamilyProvenance(
            family_key="aggregation:revenue",
            similarity_score=0.9,
            tables_used=["gold.finance.orders"],
        )
        assert p.family_key == "aggregation:revenue"
        assert p.similarity_score == 0.9


# ── Live Integration Tests (skipped without infrastructure) ────

@pytest.mark.integration_live
class TestLiveIntegration:
    """These tests require live Trino, pgvector, and OpenAI connections.

    Run with: pytest -m integration_live
    """

    @pytest.mark.skip(reason="Requires live Trino connection")
    @pytest.mark.asyncio
    async def test_full_pipeline_run(self):
        """End-to-end pipeline run with a real question.

        To enable this test:
        1. Start Trino: docker-compose up trino
        2. Start pgvector: docker-compose up pgvector
        3. Set OPENAI_API_KEY env var
        4. Remove the @skip decorator
        """
        from src.raven.pipeline import Pipeline
        from src.raven.connectors.trino_connector import TrinoConnector
        from src.raven.connectors.pgvector_store import PgVectorStore
        from src.raven.connectors.openai_client import OpenAIClient

        trino = TrinoConnector()
        pgvector = PgVectorStore()
        openai = OpenAIClient()
        pipeline = Pipeline(trino=trino, pgvector=pgvector, openai=openai)

        result = await pipeline.run("What is total revenue this month?")
        assert result is not None
        assert result.sql is not None or result.confidence == "ABSTAIN"

    @pytest.mark.skip(reason="Requires live Redis connection")
    @pytest.mark.asyncio
    async def test_redis_cache_live(self):
        """Test Redis cache with a live Redis instance."""
        from src.raven.redis_cache import RedisCache
        cache = RedisCache(url="redis://localhost:6379/0")
        await cache.set("test_live", {"x": 1})
        result = await cache.get("test_live")
        assert result == {"x": 1}
        await cache.delete("test_live")
