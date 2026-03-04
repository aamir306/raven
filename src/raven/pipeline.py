"""
RAVEN Pipeline Orchestrator
===========================
Main entry point that wires the 8-stage pipeline:
  1. Router  →  2. Context Retrieval  →  3. Schema Selection
  4. Test Probes  →  5. SQL Generation  →  6. Selection + Validation
  7. Execute + Render  →  8. Respond + Feedback
"""

from __future__ import annotations

import asyncio
import json
import logging
import pickle
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

from .connectors.openai_client import OpenAIClient
from .connectors.pgvector_store import PgVectorStore
from .connectors.trino_connector import TrinoConnector
from .router.classifier import DifficultyClassifier, Difficulty
from .retrieval.information_retriever import InformationRetriever
from .schema.schema_selector import SchemaSelector
from .probes.probe_runner import ProbeRunner
from .generation.candidate_generator import CandidateGenerator
from .validation.candidate_selector import CandidateSelector
from .output.renderer import OutputRenderer
from .feedback.collector import FeedbackCollector

logger = logging.getLogger(__name__)


@dataclass
class PipelineContext:
    """Accumulated context passed through pipeline stages."""

    # Input
    user_question: str
    conversation_id: str | None = None

    # Stage 1: Router
    difficulty: Difficulty | None = None

    # Stage 2: Context Retrieval
    keywords: list[str] = field(default_factory=list)
    time_range: str | None = None
    entity_matches: list[dict] = field(default_factory=list)
    similar_queries: list[dict] = field(default_factory=list)
    glossary_matches: list[dict] = field(default_factory=list)
    doc_snippets: list[dict] = field(default_factory=list)
    content_awareness: list[dict] = field(default_factory=list)

    # Stage 3: Schema Selection
    candidate_columns: list[str] = field(default_factory=list)
    selected_tables: list[str] = field(default_factory=list)
    pruned_schema: str = ""
    join_paths: list[str] = field(default_factory=list)

    # Stage 4: Test Probes
    probe_evidence: list[dict] = field(default_factory=list)

    # Stage 5: SQL Generation
    sql_candidates: list[str] = field(default_factory=list)

    # Stage 6: Selection + Validation
    selected_sql: str = ""
    confidence: str = "LOW"  # HIGH / MEDIUM / LOW

    # Stage 7: Execute + Render
    result_df: Any = None
    row_count: int = 0
    chart_type: str = "TABLE"
    chart_config: dict = field(default_factory=dict)
    nl_summary: str = ""

    # Timing
    stage_timings: dict[str, float] = field(default_factory=dict)
    total_cost: float = 0.0


class Pipeline:
    """Orchestrates the 8-stage text-to-SQL pipeline."""

    def __init__(
        self,
        trino: TrinoConnector,
        pgvector: PgVectorStore,
        openai: OpenAIClient,
    ):
        self.trino = trino
        self.pgvector = pgvector
        self.openai = openai

        # Initialize stage handlers (decomposed orchestrators)
        self.router = DifficultyClassifier(openai)
        self.retriever = InformationRetriever(openai, pgvector)
        self.schema_selector = SchemaSelector(openai, pgvector)
        self.probe_runner = ProbeRunner(openai, trino)
        self.generator = CandidateGenerator(openai, trino)
        self.validator = CandidateSelector(openai, trino)
        self.renderer = OutputRenderer(openai, trino)
        self.feedback = FeedbackCollector(pgvector)

        # Load preprocessing artifacts from data/ directory
        self._load_artifacts()

    def _load_artifacts(self) -> None:
        """Load preprocessing artifacts into stage modules at startup."""
        data_dir = Path(__file__).resolve().parents[2] / "data"

        # 1. Table graph → SchemaSelector's GraphPathFinder
        graph_path = data_dir / "table_graph.gpickle"
        if graph_path.exists():
            try:
                with open(graph_path, "rb") as f:
                    graph = pickle.load(f)
                if hasattr(self.schema_selector, "set_graph"):
                    self.schema_selector.set_graph(graph)
                    logger.info("Loaded table graph: %d nodes, %d edges",
                                graph.number_of_nodes(), graph.number_of_edges())
            except Exception as e:
                logger.warning("Failed to load table graph: %s", e)

        # 2. LSH index → InformationRetriever's LSHMatcher
        lsh_path = data_dir / "lsh_index.pkl"
        if lsh_path.exists():
            try:
                with open(lsh_path, "rb") as f:
                    lsh_data = pickle.load(f)
                if hasattr(self.retriever, "set_lsh_index"):
                    self.retriever.set_lsh_index(
                        lsh_data.get("lsh"), lsh_data.get("metadata")
                    )
                    logger.info("Loaded LSH index: %d entries",
                                len(lsh_data.get("metadata", {})))
            except Exception as e:
                logger.warning("Failed to load LSH index: %s", e)

        # 3. Schema catalog → column catalog for SchemaSelector's ColumnPruner
        catalog_path = data_dir / "schema_catalog.json"
        if catalog_path.exists():
            try:
                with open(catalog_path) as f:
                    raw_catalog = json.load(f)
                column_catalog = {}
                for table in raw_catalog:
                    fqn = table.get("table_name", "")
                    if fqn and table.get("columns"):
                        column_catalog[fqn] = table["columns"]
                if hasattr(self.schema_selector, "set_column_catalog"):
                    self.schema_selector.set_column_catalog(column_catalog)
                    logger.info("Loaded column catalog: %d tables, %d columns",
                                len(column_catalog),
                                sum(len(v) for v in column_catalog.values()))
            except Exception as e:
                logger.warning("Failed to load schema catalog: %s", e)

    async def generate(self, question: str, conversation_id: str | None = None) -> dict:
        """
        Run the full pipeline for a user question.

        Returns a dict with: sql, data, chart, summary, confidence, timings, cost.
        """
        ctx = PipelineContext(
            user_question=question,
            conversation_id=conversation_id,
        )
        pipeline_start = time.monotonic()

        try:
            # ── Stage 1: Router ────────────────────────────────────────
            await self._run_stage("router", self._stage_router, ctx)

            if ctx.difficulty == Difficulty.AMBIGUOUS:
                return self._ambiguous_response(ctx)

            # ── Stage 2: Context Retrieval ─────────────────────────────
            await self._run_stage("retrieval", self._stage_retrieval, ctx)

            # ── Stage 3: Schema Selection ──────────────────────────────
            await self._run_stage("schema_selection", self._stage_schema, ctx)

            # ── Stage 4: Test Probes (complex only) ────────────────────
            if ctx.difficulty == Difficulty.COMPLEX:
                await self._run_stage("probes", self._stage_probes, ctx)

            # ── Stage 5: SQL Generation ────────────────────────────────
            await self._run_stage("generation", self._stage_generation, ctx)

            # ── Stage 6: Selection + Validation (complex only) ─────────
            if ctx.difficulty == Difficulty.COMPLEX and len(ctx.sql_candidates) > 1:
                await self._run_stage("validation", self._stage_validation, ctx)
            else:
                ctx.selected_sql = ctx.sql_candidates[0] if ctx.sql_candidates else ""
                ctx.confidence = "MEDIUM"

            # ── Stage 7: Execute + Render ──────────────────────────────
            await self._run_stage("execute_render", self._stage_execute, ctx)

            # ── Stage 8: Respond + Feedback (async, non-blocking) ──────
            asyncio.create_task(self._stage_feedback(ctx))

        except Exception as e:
            logger.exception("Pipeline failed for question: %s", question)
            return self._error_response(ctx, e)

        ctx.stage_timings["total"] = time.monotonic() - pipeline_start
        ctx.total_cost = self.openai.get_cost_summary().get("total_usd", 0.0)

        return self._success_response(ctx)

    # ── Stage Implementations ──────────────────────────────────────────

    async def _stage_router(self, ctx: PipelineContext) -> None:
        ctx.difficulty = await self.router.classify(ctx.user_question)
        logger.info("Router classified '%s' as %s", ctx.user_question[:60], ctx.difficulty.value)

    async def _stage_retrieval(self, ctx: PipelineContext) -> None:
        result = await self.retriever.retrieve(
            question=ctx.user_question,
            difficulty=ctx.difficulty,
        )
        ctx.keywords = result.get("keywords", [])
        ctx.time_range = result.get("time_range")
        ctx.entity_matches = result.get("entity_matches", [])
        ctx.similar_queries = result.get("similar_queries", [])
        ctx.glossary_matches = result.get("glossary_matches", [])
        ctx.doc_snippets = result.get("doc_snippets", [])
        ctx.content_awareness = result.get("content_awareness", [])

    async def _stage_schema(self, ctx: PipelineContext) -> None:
        result = await self.schema_selector.select(
            question=ctx.user_question,
            entity_matches=ctx.entity_matches,
            glossary_matches=ctx.glossary_matches,
            similar_queries=ctx.similar_queries,
            doc_snippets=ctx.doc_snippets,
            content_awareness=ctx.content_awareness,
        )
        ctx.candidate_columns = result.get("candidate_columns", [])
        ctx.selected_tables = result.get("selected_tables", [])
        ctx.pruned_schema = result.get("pruned_schema", "")
        ctx.join_paths = result.get("join_paths", [])

    async def _stage_probes(self, ctx: PipelineContext) -> None:
        ctx.probe_evidence = await self.probe_runner.run_probes(
            question=ctx.user_question,
            pruned_schema=ctx.pruned_schema,
            selected_tables=ctx.selected_tables,
        )

    async def _stage_generation(self, ctx: PipelineContext) -> None:
        ctx.sql_candidates = await self.generator.generate(
            question=ctx.user_question,
            difficulty=ctx.difficulty,
            pruned_schema=ctx.pruned_schema,
            probe_evidence=ctx.probe_evidence,
            glossary_matches=ctx.glossary_matches,
            similar_queries=ctx.similar_queries,
        )

    async def _stage_validation(self, ctx: PipelineContext) -> None:
        result = await self.validator.select_best(
            question=ctx.user_question,
            candidates=ctx.sql_candidates,
            pruned_schema=ctx.pruned_schema,
            content_awareness=ctx.content_awareness,
        )
        ctx.selected_sql = result.get("sql", ctx.sql_candidates[0])
        ctx.confidence = result.get("confidence", "MEDIUM")

    async def _stage_execute(self, ctx: PipelineContext) -> None:
        # Execute SQL
        if ctx.selected_sql:
            try:
                ctx.result_df = await asyncio.to_thread(
                    self.trino.execute, ctx.selected_sql
                )
                ctx.row_count = len(ctx.result_df) if ctx.result_df is not None else 0
            except Exception as e:
                logger.warning("SQL execution failed: %s", e)
                ctx.result_df = None
                ctx.row_count = 0

        # Render output (chart detection + NL summary)
        if ctx.result_df is not None and ctx.row_count > 0:
            render = await self.renderer.render(
                question=ctx.user_question,
                sql=ctx.selected_sql,
                df=ctx.result_df,
            )
            ctx.chart_type = render.get("chart_type", "TABLE")
            ctx.chart_config = render.get("chart_config", {})
            ctx.nl_summary = render.get("summary", "")

    async def _stage_feedback(self, ctx: PipelineContext) -> None:
        """Log query for feedback collection (fire-and-forget)."""
        try:
            await self.feedback.log_query(
                question=ctx.user_question,
                sql=ctx.selected_sql,
                difficulty=ctx.difficulty.value if ctx.difficulty else "unknown",
                confidence=ctx.confidence,
                row_count=ctx.row_count,
                conversation_id=ctx.conversation_id,
            )
        except Exception:
            logger.debug("Feedback logging failed (non-critical)", exc_info=True)

    # ── Helpers ────────────────────────────────────────────────────────

    async def _run_stage(self, name: str, fn, ctx: PipelineContext) -> None:
        """Run a stage with timing."""
        start = time.monotonic()
        await fn(ctx)
        elapsed = time.monotonic() - start
        ctx.stage_timings[name] = round(elapsed, 3)
        logger.info("Stage [%s] completed in %.2fs", name, elapsed)

    def _success_response(self, ctx: PipelineContext) -> dict:
        return {
            "status": "success",
            "question": ctx.user_question,
            "sql": ctx.selected_sql,
            "data": ctx.result_df.to_dict("records") if ctx.result_df is not None else [],
            "row_count": ctx.row_count,
            "chart_type": ctx.chart_type,
            "chart_config": ctx.chart_config,
            "summary": ctx.nl_summary,
            "confidence": ctx.confidence,
            "difficulty": ctx.difficulty.value if ctx.difficulty else "unknown",
            "timings": ctx.stage_timings,
            "cost": ctx.total_cost,
        }

    def _ambiguous_response(self, ctx: PipelineContext) -> dict:
        return {
            "status": "ambiguous",
            "question": ctx.user_question,
            "message": "Your question is ambiguous. Could you be more specific about what data you need?",
            "difficulty": "AMBIGUOUS",
            "timings": ctx.stage_timings,
            "cost": ctx.total_cost,
        }

    def _error_response(self, ctx: PipelineContext, error: Exception) -> dict:
        return {
            "status": "error",
            "question": ctx.user_question,
            "error": str(error),
            "timings": ctx.stage_timings,
            "cost": ctx.total_cost,
        }
