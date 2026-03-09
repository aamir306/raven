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
import os
import pickle
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .connectors.openai_client import OpenAIClient
from .connectors.pgvector_store import PgVectorStore
from .connectors.trino_connector import TrinoConnector
from .connectors.trino_pool import TrinoSessionPool
from .connectors.openmetadata_mcp import OpenMetadataMCPClient, OpenMetadataConfig
from .contracts.instruction_compiler import InstructionCompiler
from .focus import FocusContext, suggest_enhancements
from .router.classifier import DifficultyClassifier, Difficulty
from .retrieval.information_retriever import InformationRetriever
from .schema.schema_selector import SchemaSelector
from .probes.probe_runner import ProbeRunner
from .generation.candidate_generator import CandidateGenerator
from .generation.constrained_sql import ConstrainedSQLGenerator
from .validation.candidate_selector import CandidateSelector
from .validation.confidence_model import ConfidenceModel
from .validation.cost_guard import CostGuard
from .validation.execution_judge import ExecutionJudge
from .output.renderer import OutputRenderer
from .feedback.collector import FeedbackCollector
from .conversation import ConversationManager
from .cache import QueryCache
from .redis_cache import HybridCache, RedisCache
from .grounding import ValueResolver
from .grounding.ambiguity_policy import AmbiguityPolicy
from .metabase import MetabaseQueryFamilySync
from .metrics import METRICS
from .planning import DeterministicPlanner
from .query_families.provenance import build_provenance_from_match
from .query_families.registry import QueryFamilyRegistry
from .semantic_assets import SemanticModelStore
from .sql.sqlglot_compiler import TrinoSQLCompiler

logger = logging.getLogger(__name__)

_METADATA_LOOKUP_PATTERNS = [
    re.compile(r"\b(?:what|which)\s+(?:\w+\s+){0,3}(?:table|tables|column|columns|schema|schemas)\b", re.IGNORECASE),
    re.compile(r"\bwhere\s+can\s+i\s+find\b", re.IGNORECASE),
    re.compile(r"\bwhere\s+do\s+i\s+find\b", re.IGNORECASE),
    re.compile(r"\bis\s+there\s+(?:a|any)\s+(?:table|tables)\b", re.IGNORECASE),
    re.compile(r"\b(?:table|tables)\s+(?:has|have|contains|contain|stores|store|holds|hold)\b", re.IGNORECASE),
    re.compile(r"\b(?:table|tables)\s+can\s+give\s+me\b", re.IGNORECASE),
]
# Schema-describe shortcut: matches explicit requests for column/schema info on a named table.
# Captures the FQN table name so we can run SHOW COLUMNS FROM directly.
_SCHEMA_DESCRIBE_PATTERNS = [
    # "what are all columns in cdp.x.y", "what columns does cdp.x.y have"
    re.compile(
        r"\b(?:what|which|show|list|get|give)\s+(?:\w+\s+){0,4}"
        r"(?:columns?|fields?|schema)\s+(?:in|of|from|for|does)\s+"
        r"(?P<table>[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)\b",
        re.IGNORECASE,
    ),
    # "describe cdp.x.y", "describe table cdp.x.y"
    re.compile(
        r"\b(?:describe|desc)\s+(?:table\s+)?"
        r"(?P<table>[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)\b",
        re.IGNORECASE,
    ),
    # "show columns from cdp.x.y"
    re.compile(
        r"\bshow\s+columns\s+(?:from|in)\s+"
        r"(?P<table>[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)\b",
        re.IGNORECASE,
    ),
    # "columns of cdp.x.y", "schema of cdp.x.y"
    re.compile(
        r"\b(?:columns?|fields?|schema)\s+(?:of|in|from)\s+"
        r"(?P<table>[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)\b",
        re.IGNORECASE,
    ),
]
_DISTINCT_VALUE_LOOKUP_PATTERNS = [
    re.compile(
        r"\b(?:what\s+(?:are|is)\s+)?(?:the\s+)?distinct\s+(?:values?\s+of\s+)?"
        r"(?P<column>[a-zA-Z_][a-zA-Z0-9_]*)\s+(?:in|from|for)\s+"
        r"(?P<table>[a-zA-Z_][a-zA-Z0-9_.]*)\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b(?:show|list|give|find)\s+(?:me\s+)?(?:the\s+)?distinct\s+"
        r"(?:values?\s+of\s+)?(?P<column>[a-zA-Z_][a-zA-Z0-9_]*)\s+(?:in|from|for)\s+"
        r"(?P<table>[a-zA-Z_][a-zA-Z0-9_.]*)\b",
        re.IGNORECASE,
    ),
]
# Matches fully-qualified table references like cdp.schema.table in user questions
_FQN_TABLE_RE = re.compile(
    r"\b([a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*)\b",
    re.IGNORECASE,
)

_METADATA_SKIP_TOKENS = {
    "a",
    "all",
    "an",
    "and",
    "by",
    "can",
    "column",
    "columns",
    "do",
    "find",
    "for",
    "from",
    "get",
    "give",
    "has",
    "have",
    "how",
    "i",
    "in",
    "is",
    "logs",
    "me",
    "of",
    "schema",
    "show",
    "store",
    "stores",
    "table",
    "tables",
    "the",
    "to",
    "today",
    "what",
    "where",
    "which",
}


@dataclass
class PipelineContext:
    """Accumulated context passed through pipeline stages."""

    # Input
    user_question: str
    conversation_id: str | None = None

    # Focus Mode context (scoped tables / dashboard)
    focus: Any | None = None  # FocusContext instance when focus is active

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
    om_table_candidates: list[dict] = field(default_factory=list)
    preferred_tables: list[str] = field(default_factory=list)
    trusted_query_match: dict | None = None
    query_family_match: dict | None = None
    query_intent: str = "DATA_QUERY"
    instruction_matches: list[dict] = field(default_factory=list)
    metabase_evidence: list[dict] = field(default_factory=list)
    resolved_values: list[dict] = field(default_factory=list)
    grounding_ambiguities: list[dict] = field(default_factory=list)
    query_plan: dict | None = None

    # Stage 3: Schema Selection
    candidate_columns: list[str] = field(default_factory=list)
    selected_tables: list[str] = field(default_factory=list)
    pruned_schema: str = ""
    join_paths: list[Any] = field(default_factory=list)
    quality_warnings: list[dict] = field(default_factory=list)

    # Stage 4: Test Probes
    probe_evidence: list[dict] = field(default_factory=list)

    # Stage 5: SQL Generation
    sql_candidates: list[str] = field(default_factory=list)

    # Stage 6: Selection + Validation
    selected_sql: str = ""
    confidence: str = "LOW"  # HIGH / MEDIUM / LOW
    validation_issues: list[str] = field(default_factory=list)

    # Stage 7: Execute + Render
    result_df: Any = None
    row_count: int = 0
    chart_type: str = "TABLE"
    chart_config: dict = field(default_factory=dict)
    nl_summary: str = ""

    # Timing
    stage_timings: dict[str, float] = field(default_factory=dict)
    total_cost: float = 0.0

    # Follow-up suggestions
    follow_up_suggestions: list[str] = field(default_factory=list)

    # Focus Mode: enhancement suggestions for living documents
    enhancement_suggestions: list[dict] = field(default_factory=list)


class Pipeline:
    """Orchestrates the 8-stage text-to-SQL pipeline."""

    def __init__(
        self,
        trino: TrinoConnector,
        pgvector: PgVectorStore,
        openai: OpenAIClient,
        cache_enabled: bool = True,
        cache_ttl: int = 3600,
        om_config_path: str | None = None,
    ):
        self.trino = trino
        self.pgvector = pgvector
        self.openai = openai

        # ── OpenMetadata MCP client (optional) ─────────────────────────
        self.om_client: OpenMetadataMCPClient | None = None
        try:
            config_path = om_config_path or str(
                Path(__file__).resolve().parents[2] / "config" / "openmetadata.yaml"
            )
            om_config = OpenMetadataConfig.from_yaml(config_path)
            if om_config.is_configured:
                self.om_client = OpenMetadataMCPClient(om_config)
                logger.info("OpenMetadata MCP client initialized: %s", om_config.url)
            else:
                logger.info("OpenMetadata not configured — using local artifacts only")
        except Exception as exc:
            logger.warning("Failed to initialize OpenMetadata MCP client: %s", exc)

        # Query result cache
        self.cache = QueryCache(enabled=cache_enabled, ttl_seconds=cache_ttl)
        self.redis_cache = RedisCache.from_env()
        self.hybrid_cache = HybridCache(memory_cache=self.cache, redis_cache=self.redis_cache)
        self.semantic_assets = SemanticModelStore()
        self._data_keyword_pattern = self.semantic_assets.keyword_pattern()

        # Initialize stage handlers (decomposed orchestrators)
        self.router = DifficultyClassifier(openai)
        self.retriever = InformationRetriever(openai, pgvector, om_client=self.om_client)
        self.schema_selector = SchemaSelector(
            openai,
            pgvector,
            om_client=self.om_client,
            semantic_store=self.semantic_assets,
        )
        self.probe_runner = ProbeRunner(openai, trino)
        self.generator = CandidateGenerator(openai, trino)
        self.validator = CandidateSelector(openai, trino)
        self.execution_judge = ExecutionJudge()
        self.cost_guard = CostGuard(trino)
        self.confidence_model = ConfidenceModel()
        self.renderer = OutputRenderer(openai, trino)
        self.feedback = FeedbackCollector(pgvector, openai, om_client=self.om_client)
        self.conversation = ConversationManager(openai, pgvector)
        self.value_resolver = ValueResolver(self.semantic_assets)
        self.ambiguity_policy = AmbiguityPolicy()
        self.constrained_sql = ConstrainedSQLGenerator()
        self.planner = DeterministicPlanner(self.semantic_assets)
        self.trino_pool = TrinoSessionPool(trino)
        self.sql_compiler = TrinoSQLCompiler()
        self.instruction_compiler = InstructionCompiler()
        self.family_registry = QueryFamilyRegistry()
        self.query_family_registry_path = self._default_query_family_registry_path()
        self.metabase_family_sync = MetabaseQueryFamilySync(
            self.family_registry,
            registry_path=self.query_family_registry_path,
            pgvector=pgvector,
            openai=openai,
        )

        # Compile instruction assets from semantic model rules
        if self.semantic_assets._rules:
            raw_rules = [
                {
                    "term": r.term,
                    "definition": r.definition,
                    "sql_fragment": r.sql_fragment,
                    "synonyms": list(r.synonyms),
                    "categories": list(r.categories),
                    "rule_type": r.rule_type,
                }
                for r in self.semantic_assets._rules
            ]
            self.instruction_set = self.instruction_compiler.compile(
                raw_rules, source_file="semantic_model"
            )
        else:
            from .contracts.instructions import InstructionSet
            self.instruction_set = InstructionSet()

        # Load preprocessing artifacts from data/ directory
        self._load_artifacts()
        self._load_query_family_registry()

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

    def _default_query_family_registry_path(self) -> Path:
        configured = os.getenv("RAVEN_QUERY_FAMILY_REGISTRY_PATH", "").strip()
        if configured:
            return Path(configured).expanduser()
        return Path(__file__).resolve().parents[2] / "data" / "query_family_registry.json"

    def _load_query_family_registry(self) -> None:
        try:
            loaded = self.family_registry.load(self.query_family_registry_path)
            if loaded:
                self._refresh_external_query_families()
                logger.info(
                    "Loaded persisted query-family registry from %s (%d families)",
                    self.query_family_registry_path,
                    loaded,
                )
        except Exception as exc:
            logger.warning("Failed to load query-family registry: %s", exc)

    def _refresh_external_query_families(self) -> None:
        external_assets = self.family_registry.export_assets(source_prefix="metabase_sync")
        self.semantic_assets.set_external_query_families(external_assets)

    async def sync_metabase_query_families(
        self,
        *,
        cards: list[dict[str, Any]],
        scope_type: str,
        scope_id: str | int,
        scope_name: str,
        persist_embeddings: bool = False,
    ) -> dict[str, Any]:
        result = await self.metabase_family_sync.sync_cards(
            cards,
            scope_type=scope_type,
            scope_id=scope_id,
            scope_name=scope_name,
            persist_embeddings=persist_embeddings,
        )
        self._refresh_external_query_families()
        return result

    async def generate(
        self,
        question: str,
        conversation_id: str | None = None,
        stage_hook: Any | None = None,
        focus: Any | None = None,
    ) -> dict:
        """
        Run the full pipeline for a user question.

        Args:
            question: Natural language question.
            conversation_id: Optional multi-turn conversation ID.
            stage_hook: Optional async callback(stage_name, event, detail_dict)
                        called with event="start" before and event="done" after each stage.
            focus: Optional FocusContext for scoped retrieval / schema selection.

        Returns a dict with: sql, data, chart, summary, confidence, timings, cost.
        """
        # ── Cache check ────────────────────────────────────────────────
        cache_start = time.monotonic()
        cached = self.cache.get(question)
        if cached is not None:
            cached["cached"] = True
            METRICS.record_cache_hit()
            METRICS.query_completed(
                difficulty=cached.get("difficulty", "unknown"),
                status=cached.get("status", "success"),
                cached=True,
                latency=time.monotonic() - cache_start,
                cost=0.0,
            )
            logger.info("Cache hit for: %s", question[:60])
            return cached
        METRICS.record_cache_miss()

        # ── Multi-turn resolution ──────────────────────────────────────
        conv_result = await self.conversation.resolve_question(question, conversation_id)
        effective_question = conv_result["resolved_question"]
        is_followup = conv_result["is_followup"]

        if is_followup:
            logger.info(
                "Follow-up resolved: '%s' → '%s'",
                question[:50], effective_question[:50],
            )

        ctx = PipelineContext(
            user_question=effective_question,
            conversation_id=conversation_id,
            focus=focus,
        )
        pipeline_start = time.monotonic()
        METRICS.query_started()

        try:
            # ── Stage 1: Router ────────────────────────────────────────
            await self._run_stage("router", self._stage_router, ctx, stage_hook)

            if ctx.difficulty == Difficulty.AMBIGUOUS:
                # Safety net 1: keyword check — if the question mentions any
                # recognisable data entity, force downgrade before retrieval.
                if self._has_data_keywords(ctx.user_question):
                    logger.info(
                        "AMBIGUOUS overridden by keyword match → SIMPLE for '%s'",
                        ctx.user_question[:60],
                    )
                    ctx.difficulty = Difficulty.SIMPLE
                else:
                    # Safety net 2: try context retrieval — if schema matches found,
                    # downgrade to SIMPLE and continue instead of rejecting.
                    await self._run_stage("retrieval", self._stage_retrieval, ctx, stage_hook)
                    if ctx.entity_matches or ctx.glossary_matches:
                        logger.info(
                            "AMBIGUOUS downgraded to SIMPLE — %d entity + %d glossary matches for '%s'",
                            len(ctx.entity_matches),
                            len(ctx.glossary_matches),
                            ctx.user_question[:60],
                        )
                        ctx.difficulty = Difficulty.SIMPLE
                    else:
                        return self._ambiguous_response(ctx)

            # ── Stage 2: Context Retrieval (skip if already done in fallback)
            if not ctx.entity_matches and not ctx.glossary_matches:
                await self._run_stage("retrieval", self._stage_retrieval, ctx, stage_hook)

            # ── Extract explicit FQN table references from question ────
            fqn_tables = _FQN_TABLE_RE.findall(ctx.user_question)
            if fqn_tables:
                logger.info("Explicit FQN tables in question: %s", fqn_tables)
                # Prepend so they take priority
                ctx.preferred_tables = list(
                    dict.fromkeys(fqn_tables + ctx.preferred_tables)
                )

            ctx.query_intent = self._infer_query_intent(ctx.user_question)
            if ctx.query_intent == "METADATA_LOOKUP":
                metadata_response = self._metadata_lookup_response(ctx)
                if metadata_response is not None:
                    ctx.stage_timings["total"] = time.monotonic() - pipeline_start
                    ctx.total_cost = self.openai.get_cost_summary().get("total_usd", 0.0)
                    metadata_response["timings"] = ctx.stage_timings
                    metadata_response["cost"] = ctx.total_cost
                    METRICS.query_completed(
                        difficulty=ctx.difficulty.value if ctx.difficulty else "unknown",
                        status=metadata_response.get("status", "success"),
                        latency=ctx.stage_timings["total"],
                        cost=ctx.total_cost,
                        confidence=metadata_response.get("confidence", ctx.confidence),
                    )
                    self.cache.put(question, metadata_response)
                    metadata_response["cached"] = False
                    return metadata_response

            if ctx.query_intent == "DISTINCT_VALUE_LOOKUP":
                ctx.query_plan = self._distinct_value_lookup_plan(ctx.user_question)
                if ctx.query_plan:
                    ctx.selected_tables = list(ctx.query_plan.get("source_tables", []))
                    ctx.confidence = ctx.query_plan.get("confidence", "HIGH")

            if ctx.query_intent == "SCHEMA_DESCRIBE" and not ctx.query_plan:
                ctx.query_plan = self._schema_describe_plan(ctx.user_question)
                if ctx.query_plan:
                    ctx.selected_tables = list(ctx.query_plan.get("source_tables", []))
                    ctx.confidence = ctx.query_plan.get("confidence", "HIGH")

            # ── Stage 3: Schema Selection ──────────────────────────────
            _bypass_paths = {"DISTINCT_VALUE_LOOKUP", "SCHEMA_DESCRIBE"}
            if not (ctx.query_plan and ctx.query_plan.get("path_type") in _bypass_paths):
                await self._run_stage("schema_selection", self._stage_schema, ctx, stage_hook)

            # ── Stage 4: Deterministic Planning ───────────────────────
            if not ctx.query_plan:
                await self._run_stage("planning", self._stage_planning, ctx, stage_hook)

            # ── Stage 5: Test Probes (complex only, unresolved path) ──
            if ctx.difficulty == Difficulty.COMPLEX and not ctx.query_plan:
                await self._run_stage("probes", self._stage_probes, ctx, stage_hook)

            # ── Stage 6: SQL Generation ────────────────────────────────
            await self._run_stage("generation", self._stage_generation, ctx, stage_hook)

            # ── Stage 7: Selection + Validation (complex only) ─────────
            if ctx.difficulty == Difficulty.COMPLEX and len(ctx.sql_candidates) > 1:
                await self._run_stage("validation", self._stage_validation, ctx, stage_hook)
                if not ctx.selected_sql:
                    ctx.stage_timings["total"] = time.monotonic() - pipeline_start
                    ctx.total_cost = self.openai.get_cost_summary().get("total_usd", 0.0)
                    METRICS.query_completed(
                        difficulty=ctx.difficulty.value if ctx.difficulty else "unknown",
                        status="ambiguous",
                        latency=ctx.stage_timings["total"],
                        cost=ctx.total_cost,
                        confidence=ctx.confidence,
                    )
                    return self._ambiguous_response(ctx)
            else:
                ctx.selected_sql = ctx.sql_candidates[0] if ctx.sql_candidates else ""
                if ctx.query_plan:
                    ctx.confidence = ctx.query_plan.get("confidence", "MEDIUM")
                elif ctx.trusted_query_match and ctx.trusted_query_match.get("exact_match"):
                    ctx.confidence = "HIGH"
                else:
                    ctx.confidence = "MEDIUM"

            # ── Stage 8: Execute + Render ──────────────────────────────
            await self._run_stage("execute_render", self._stage_execute, ctx, stage_hook)
            if not ctx.selected_sql:
                ctx.stage_timings["total"] = time.monotonic() - pipeline_start
                ctx.total_cost = self.openai.get_cost_summary().get("total_usd", 0.0)
                METRICS.query_completed(
                    difficulty=ctx.difficulty.value if ctx.difficulty else "unknown",
                    status="ambiguous",
                    latency=ctx.stage_timings["total"],
                    cost=ctx.total_cost,
                    confidence=ctx.confidence,
                )
                return self._ambiguous_response(ctx)

            # ── Stage 9: Respond + Feedback (async, non-blocking) ──────
            asyncio.create_task(self._stage_feedback(ctx))

            # ── Generate follow-up suggestions (non-blocking) ──────────
            try:
                ctx.follow_up_suggestions = await self._generate_followups(ctx)
            except Exception:
                ctx.follow_up_suggestions = []
                logger.debug("Follow-up generation failed (non-critical)", exc_info=True)

            # ── Generate focus enhancement suggestions (Living Documents) ──
            try:
                ctx.enhancement_suggestions = await suggest_enhancements(
                    focus=ctx.focus,
                    tables_used=ctx.selected_tables,
                    probe_evidence=ctx.probe_evidence,
                )
            except Exception:
                ctx.enhancement_suggestions = []
                logger.debug("Enhancement suggestion failed (non-critical)", exc_info=True)

        except Exception as e:
            logger.exception("Pipeline failed for question: %s", question)
            METRICS.query_completed(
                difficulty=ctx.difficulty.value if ctx.difficulty else "unknown",
                status="error",
                latency=time.monotonic() - pipeline_start,
                cost=ctx.total_cost,
            )
            return self._error_response(ctx, e)

        ctx.stage_timings["total"] = time.monotonic() - pipeline_start
        ctx.total_cost = self.openai.get_cost_summary().get("total_usd", 0.0)

        METRICS.query_completed(
            difficulty=ctx.difficulty.value if ctx.difficulty else "unknown",
            status="success" if ctx.selected_sql else "ambiguous",
            latency=ctx.stage_timings["total"],
            cost=ctx.total_cost,
            confidence=ctx.confidence,
        )

        result = self._success_response(ctx)

        # Add conversation metadata
        if is_followup:
            result["original_question"] = question
            result["is_followup"] = True

        # ── Store in cache ─────────────────────────────────────────────
        self.cache.put(question, result)
        result["cached"] = False

        return result

    # ── Stage Implementations ──────────────────────────────────────────

    def _has_data_keywords(self, question: str) -> bool:
        """Return True if the question contains configurable semantic/domain keywords."""
        return bool(self._data_keyword_pattern.search(question))

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
        ctx.om_table_candidates = result.get("om_table_candidates", [])

        semantic = self.semantic_assets.retrieve(ctx.user_question, focus=ctx.focus)
        ctx.trusted_query_match = semantic.get("trusted_query")
        ctx.query_family_match = semantic.get("query_family_match")
        ctx.preferred_tables = semantic.get("preferred_tables", [])
        ctx.instruction_matches = semantic.get("instruction_matches", [])
        ctx.metabase_evidence = semantic.get("metabase_evidence", [])
        ctx.similar_queries = self._merge_similar_queries(
            semantic.get("verified_queries", []),
            ctx.similar_queries,
        )
        ctx.glossary_matches = self._merge_glossary_matches(
            semantic.get("glossary_matches", []),
            ctx.glossary_matches,
        )
        ctx.doc_snippets = self._merge_doc_snippets(
            semantic.get("doc_snippets", []),
            ctx.doc_snippets,
        )
        grounding = self.value_resolver.resolve(
            question=ctx.user_question,
            content_awareness=ctx.content_awareness,
            preferred_tables=ctx.preferred_tables,
            instruction_matches=ctx.instruction_matches,
            focus=ctx.focus,
        )
        ctx.resolved_values = [flt.to_dict() for flt in grounding.filters]
        ctx.grounding_ambiguities = list(grounding.ambiguities)

    async def _stage_schema(self, ctx: PipelineContext) -> None:
        result = await self.schema_selector.select(
            question=ctx.user_question,
            entity_matches=ctx.entity_matches,
            glossary_matches=ctx.glossary_matches,
            similar_queries=ctx.similar_queries,
            doc_snippets=ctx.doc_snippets,
            content_awareness=ctx.content_awareness,
            om_table_candidates=ctx.om_table_candidates,
            preferred_tables=ctx.preferred_tables,
            metabase_evidence=ctx.metabase_evidence,
        )
        ctx.candidate_columns = result.get("candidate_columns", [])
        ctx.selected_tables = result.get("selected_tables", [])
        ctx.pruned_schema = result.get("pruned_schema", "")
        ctx.join_paths = result.get("join_paths", [])
        ctx.quality_warnings = result.get("quality_warnings", [])

    async def _stage_planning(self, ctx: PipelineContext) -> None:
        if ctx.trusted_query_match and ctx.trusted_query_match.get("exact_match"):
            ctx.query_plan = {
                "path_type": "TRUSTED_QUERY",
                "intent": "TRUSTED_QUERY",
                "confidence": "HIGH",
                "compiled_sql": ctx.trusted_query_match.get("sql", ""),
                "evidence": [],
            }
            return

        refined_family_match = self.semantic_assets.match_query_family(
            question=ctx.user_question,
            verified_queries=ctx.similar_queries,
            metabase_evidence=ctx.metabase_evidence,
            resolved_filters=ctx.resolved_values,
            glossary_matches=ctx.glossary_matches,
        )
        if refined_family_match:
            current_score = float((ctx.query_family_match or {}).get("similarity", 0.0))
            refined_score = float(refined_family_match.get("similarity", 0.0))
            if refined_score >= current_score:
                ctx.query_family_match = refined_family_match

        if ctx.query_family_match and ctx.query_family_match.get("sql"):
            # Build provenance record for audit trail
            provenance = build_provenance_from_match(
                ctx.query_family_match,
                user_question=ctx.user_question,
            )
            ctx.query_plan = {
                "path_type": "QUERY_FAMILY",
                "intent": "QUERY_FAMILY",
                "confidence": "HIGH" if ctx.query_family_match.get("source") == "metabase" else "MEDIUM",
                "compiled_sql": ctx.query_family_match.get("sql", ""),
                "source_tables": ctx.query_family_match.get("tables_used", []),
                "provenance": provenance.to_dict(),
                "evidence": [
                    {
                        "kind": "query_family",
                        "source": ctx.query_family_match.get("source", "semantic_model"),
                        "detail": ctx.query_family_match.get("question", ""),
                        "score": ctx.query_family_match.get("similarity", 0.0),
                        "evidence_strength": provenance.evidence_strength,
                    }
                ],
            }
            if not ctx.selected_tables:
                ctx.selected_tables = list(ctx.query_family_match.get("tables_used", []))
            return

        if ctx.grounding_ambiguities:
            decision = self.ambiguity_policy.evaluate(
                ambiguities=ctx.grounding_ambiguities,
                resolved_filters=ctx.resolved_values,
                focus=ctx.focus,
            )
            if decision.action == "clarify":
                ctx.validation_issues.extend(decision.suggestions)
                ctx.query_plan = None
                return
            elif decision.action == "abstain":
                ctx.validation_issues.append(decision.reason)
                ctx.query_plan = None
                return
            # "pick_best" — proceed with deterministic planning

        plan = self.planner.plan(
            question=ctx.user_question,
            glossary_matches=ctx.glossary_matches,
            selected_tables=ctx.selected_tables,
            preferred_tables=ctx.preferred_tables,
            resolved_filters=ctx.resolved_values,
            instruction_matches=ctx.instruction_matches,
            om_table_candidates=ctx.om_table_candidates,
            metabase_evidence=ctx.metabase_evidence,
            join_paths=ctx.join_paths,
        )
        if plan:
            ctx.query_plan = plan.to_dict()
            ctx.selected_tables = list(
                dict.fromkeys(ctx.selected_tables or plan.source_tables or [plan.table])
            )
            logger.info(
                "Deterministic plan selected: %s on %s for '%s'",
                plan.intent,
                plan.table,
                ctx.user_question[:60],
            )

    async def _stage_probes(self, ctx: PipelineContext) -> None:
        ctx.probe_evidence = await self.probe_runner.run_probes(
            question=ctx.user_question,
            pruned_schema=ctx.pruned_schema,
            selected_tables=ctx.selected_tables,
        )

    async def _stage_generation(self, ctx: PipelineContext) -> None:
        if ctx.trusted_query_match and ctx.trusted_query_match.get("exact_match"):
            ctx.sql_candidates = [ctx.trusted_query_match.get("sql", "")]
            if not ctx.selected_tables:
                ctx.selected_tables = list(ctx.trusted_query_match.get("tables_used", []))
            logger.info(
                "Using exact trusted query match from %s for '%s'",
                ctx.trusted_query_match.get("source", "unknown"),
                ctx.user_question[:60],
            )
            return
        if ctx.query_plan and ctx.query_plan.get("compiled_sql"):
            ctx.sql_candidates = [ctx.query_plan.get("compiled_sql", "")]
            logger.info(
                "Using deterministic plan %s for '%s'",
                ctx.query_plan.get("intent", "unknown"),
                ctx.user_question[:60],
            )
            return

        ctx.sql_candidates = await self.generator.generate(
            question=ctx.user_question,
            difficulty=ctx.difficulty,
            pruned_schema=ctx.pruned_schema,
            probe_evidence=ctx.probe_evidence,
            glossary_matches=ctx.glossary_matches,
            similar_queries=ctx.similar_queries,
            resolved_values=ctx.resolved_values,
            instruction_matches=ctx.instruction_matches,
            query_plan=ctx.query_plan,
        )

        # Constrained fallback: structural checks on LLM-generated SQL
        if ctx.sql_candidates:
            ctx.sql_candidates = self.constrained_sql.constrain(
                raw_candidates=ctx.sql_candidates,
                selected_tables=ctx.selected_tables,
            )

        # sqlglot validation: parse + dialect enforcement on survivors
        if ctx.sql_candidates and self.sql_compiler.is_available():
            validated: list[str] = []
            for sql in ctx.sql_candidates:
                result = self.sql_compiler.compile(sql)
                if result.ok:
                    validated.append(result.sql)
                else:
                    logger.debug(
                        "sqlglot rejected candidate: %s", result.errors[:3]
                    )
            if validated:
                ctx.sql_candidates = validated

    async def _stage_validation(self, ctx: PipelineContext) -> None:
        # Build retrieval quality signals for confidence calibration
        top_sim = 0.0
        if ctx.similar_queries:
            top_sim = max(sq.get("similarity", 0.0) for sq in ctx.similar_queries)

        retrieval_quality = {
            "entity_match_count": len(ctx.entity_matches),
            "glossary_match_count": len(ctx.glossary_matches),
            "similar_query_top_sim": top_sim,
            "table_count": len(ctx.selected_tables),
            "probe_count": len(ctx.probe_evidence),
            "has_few_shot": len(ctx.similar_queries) > 0,
        }

        result = await self.validator.select_best(
            question=ctx.user_question,
            candidates=ctx.sql_candidates,
            pruned_schema=ctx.pruned_schema,
            content_awareness=ctx.content_awareness,
            retrieval_quality=retrieval_quality,
            query_plan=ctx.query_plan,
        )
        ctx.selected_sql = result.get("sql", ctx.sql_candidates[0])
        ctx.confidence = result.get("confidence", "MEDIUM")
        ctx.validation_issues = list(
            result.get("rejection_reasons")
            or result.get("plan_hard_violations")
            or result.get("plan_violations")
            or []
        )

    async def _stage_execute(self, ctx: PipelineContext) -> None:
        # ── Pre-execution cost guard (for all paths) ───────────────────
        cost_guard_result: dict | None = None
        if ctx.selected_sql:
            try:
                cost_guard_result = await self.cost_guard.check(ctx.selected_sql)
                if not cost_guard_result.get("passed", True):
                    logger.warning(
                        "Cost guard blocked query: %s",
                        cost_guard_result.get("reason", "unknown"),
                    )
                    ctx.validation_issues.append(
                        f"cost_guard_blocked:{cost_guard_result.get('reason', 'expensive')}"
                    )
                    ctx.confidence = "LOW"
                    ctx.selected_sql = ""
                    return
            except Exception as e:
                logger.debug("Cost guard check failed (non-blocking): %s", e)

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

        judge_passed: bool | None = None
        judge_issues: list[str] = []
        if ctx.result_df is not None:
            judge = self.execution_judge.judge(ctx.result_df, ctx.query_plan)
            judge_passed = judge.passed
            judge_issues = judge.issues
            if not judge.passed:
                ctx.validation_issues = list(dict.fromkeys([*ctx.validation_issues, *judge.issues]))
                ctx.confidence = "LOW"
                ctx.result_df = None
                ctx.row_count = 0
                ctx.selected_sql = ""
                logger.info(
                    "Execution judge rejected result for '%s': %s",
                    ctx.user_question[:60],
                    ", ".join(judge.issues[:5]),
                )
                return

        # ── Calibrated confidence (post-execution) ────────────────────
        top_sim = 0.0
        if ctx.similar_queries:
            top_sim = max(sq.get("similarity", 0.0) for sq in ctx.similar_queries)

        conf_result = self.confidence_model.score_pipeline(
            ctx_confidence=ctx.confidence,
            query_plan=ctx.query_plan,
            validation_issues=ctx.validation_issues,
            execution_judge_passed=judge_passed,
            execution_judge_issues=judge_issues,
            entity_match_count=len(ctx.entity_matches),
            glossary_match_count=len(ctx.glossary_matches),
            similar_query_top_sim=top_sim,
            table_count=len(ctx.selected_tables),
            probe_count=len(ctx.probe_evidence),
            grounding_ambiguity_count=len(ctx.grounding_ambiguities),
            quality_warning_count=len(ctx.quality_warnings),
            has_trusted_query=bool(
                ctx.trusted_query_match and ctx.trusted_query_match.get("exact_match")
            ),
            has_query_family=bool(ctx.query_family_match and ctx.query_family_match.get("sql")),
            cost_guard_result=cost_guard_result,
            row_count=ctx.row_count,
        )
        ctx.confidence = conf_result.band
        if conf_result.should_abstain:
            logger.info(
                "Confidence model recommends ABSTAIN (score=%.3f) for '%s'",
                conf_result.score, ctx.user_question[:60],
            )
            ctx.validation_issues.append(
                f"confidence_abstain:score={conf_result.score:.3f}"
            )
            ctx.result_df = None
            ctx.row_count = 0
            ctx.selected_sql = ""
            return

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
        """Log query for feedback collection + OM write-back (fire-and-forget)."""
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

        # ── OM Write-Back: auto-create DQ test cases from probe discoveries ──
        if self.om_client and ctx.probe_evidence and ctx.selected_tables:
            try:
                for table_fqn in ctx.selected_tables[:3]:  # limit
                    await self.om_client.probe_and_report(
                        table_fqn=table_fqn,
                        probe_results=ctx.probe_evidence,
                        question=ctx.user_question,
                    )
            except Exception:
                logger.debug("OM probe write-back failed (non-critical)", exc_info=True)

    # ── Helpers ────────────────────────────────────────────────────────

    async def _generate_followups(self, ctx: PipelineContext) -> list[str]:
        """Generate 2-3 follow-up question suggestions using GPT-4o-mini."""
        tables_str = ", ".join(ctx.selected_tables[:5]) if ctx.selected_tables else "unknown"
        prompt = (
            f"The user asked: \"{ctx.user_question}\"\n"
            f"Tables used: {tables_str}\n"
            f"Result: {ctx.row_count} rows returned, chart type: {ctx.chart_type}\n"
            f"Summary: {ctx.nl_summary[:200] if ctx.nl_summary else 'N/A'}\n\n"
            "Suggest exactly 3 natural follow-up questions the user might ask next. "
            "Each should be a different angle: drill-down, comparison, or time-based. "
            "Return ONLY the 3 questions, one per line, no numbering or bullets."
        )
        try:
            resp = await self.openai.complete(
                messages=[{"role": "user", "content": prompt}],
                stage="followup_suggestions",
                max_tokens=150,
                temperature=0.7,
            )
            lines = [l.strip() for l in resp.strip().split("\n") if l.strip()]
            return lines[:3]
        except Exception:
            return []

    async def _run_stage(self, name: str, fn, ctx: PipelineContext, stage_hook: Any | None = None) -> None:
        """Run a stage with timing, Prometheus instrumentation, and optional SSE hook."""
        if stage_hook:
            try:
                await stage_hook(name, "start", {})
            except Exception:
                pass
        start = time.monotonic()
        try:
            await fn(ctx)
        except Exception as e:
            METRICS.record_stage_error(name, e)
            raise
        finally:
            elapsed = time.monotonic() - start
            ctx.stage_timings[name] = round(elapsed, 3)
            METRICS.observe_stage(name, elapsed)
            logger.info("Stage [%s] completed in %.2fs", name, elapsed)
            if stage_hook:
                try:
                    detail = {"time": round(elapsed, 2)}
                    # Include stage-specific detail for streaming UI
                    if name == "router" and ctx.difficulty:
                        detail["difficulty"] = ctx.difficulty.value
                    elif name == "retrieval":
                        detail["entities"] = len(ctx.entity_matches)
                        detail["glossary"] = len(ctx.glossary_matches)
                    elif name == "schema_selection":
                        detail["tables"] = ctx.selected_tables[:5] if ctx.selected_tables else []
                    elif name == "planning" and ctx.query_plan:
                        detail["intent"] = ctx.query_plan.get("intent", "")
                        detail["path_type"] = ctx.query_plan.get("path_type", "")
                    elif name == "generation":
                        detail["candidates"] = len(ctx.sql_candidates)
                    elif name == "execute_render":
                        detail["rows"] = ctx.row_count
                        detail["chart"] = ctx.chart_type
                    await stage_hook(name, "done", detail)
                except Exception:
                    pass

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
            # Phase 5: debug info for UI Debug tab
            "debug": {
                "selected_tables": ctx.selected_tables,
                "candidates_count": len(ctx.sql_candidates),
                "probe_count": len(ctx.probe_evidence),
                "entity_matches": len(ctx.entity_matches),
                "glossary_matches": len(ctx.glossary_matches),
                "similar_queries": len(ctx.similar_queries),
                "preferred_tables": ctx.preferred_tables[:8],
                "resolved_values": ctx.resolved_values[:10],
                "grounding_ambiguities": ctx.grounding_ambiguities[:5],
                "instruction_matches": ctx.instruction_matches[:8],
                "metabase_evidence": ctx.metabase_evidence[:5],
                "query_plan": ctx.query_plan,
                "trusted_query_source": (
                    ctx.trusted_query_match.get("source")
                    if ctx.trusted_query_match
                    else None
                ),
                "query_family_match": ctx.query_family_match,
            },
            # Phase 5: follow-up suggestions (populated by Stage 8)
            "suggestions": ctx.follow_up_suggestions,
            # Phase 5.4: Focus mode + living document enhancements
            "focus": ctx.focus.to_dict() if ctx.focus else None,
            "enhancements": ctx.enhancement_suggestions,
            # Phase 6: OpenMetadata quality warnings
            "quality_warnings": ctx.quality_warnings,
        }

    @staticmethod
    def _merge_similar_queries(primary: list[dict], secondary: list[dict]) -> list[dict]:
        merged = []
        seen: set[tuple[str, str]] = set()
        for item in sorted(
            [*primary, *secondary],
            key=lambda entry: (entry.get("exact_match", False), entry.get("similarity", 0.0)),
            reverse=True,
        ):
            key = (
                " ".join(item.get("question", "").lower().split()),
                " ".join(item.get("sql", "").lower().split()),
            )
            if key in seen:
                continue
            seen.add(key)
            merged.append(item)
        return merged[:6]

    @staticmethod
    def _merge_glossary_matches(primary: list[dict], secondary: list[dict]) -> list[dict]:
        merged = []
        seen: set[str] = set()
        for item in sorted(
            [*primary, *secondary],
            key=lambda entry: entry.get("similarity", 0.0),
            reverse=True,
        ):
            term = item.get("term", "").lower()
            if not term or term in seen:
                continue
            seen.add(term)
            merged.append(item)
        return merged[:10]

    @staticmethod
    def _merge_doc_snippets(primary: list[dict], secondary: list[dict]) -> list[dict]:
        merged = []
        seen: set[tuple[str, str]] = set()
        for item in [*primary, *secondary]:
            key = (item.get("source", ""), item.get("content", ""))
            if key in seen:
                continue
            seen.add(key)
            merged.append(item)
        return merged[:10]

    @staticmethod
    def _infer_query_intent(question: str) -> str:
        normalized = " ".join(str(question).lower().split())
        for pattern in _DISTINCT_VALUE_LOOKUP_PATTERNS:
            if pattern.search(normalized):
                return "DISTINCT_VALUE_LOOKUP"
        for pattern in _SCHEMA_DESCRIBE_PATTERNS:
            if pattern.search(normalized):
                return "SCHEMA_DESCRIBE"
        for pattern in _METADATA_LOOKUP_PATTERNS:
            if pattern.search(normalized):
                return "METADATA_LOOKUP"
        return "DATA_QUERY"

    def _distinct_value_lookup_plan(self, question: str) -> dict[str, Any] | None:
        match = self._distinct_value_lookup_match(question)
        if not match:
            return None

        table_name = match["table"]
        column_name = match["column"]
        if not self._catalog_has_column(table_name, column_name):
            return None

        return {
            "path_type": "DISTINCT_VALUE_LOOKUP",
            "intent": "DISTINCT_VALUE_LOOKUP",
            "confidence": "HIGH",
            "compiled_sql": (
                f"SELECT DISTINCT {column_name}\n"
                f"FROM {table_name}\n"
                f"WHERE {column_name} IS NOT NULL\n"
                f"ORDER BY {column_name}\n"
                "LIMIT 100"
            ),
            "source_tables": [table_name],
            "target_column": column_name,
        }

    @staticmethod
    def _distinct_value_lookup_match(question: str) -> dict[str, str] | None:
        normalized = " ".join(str(question or "").split())
        for pattern in _DISTINCT_VALUE_LOOKUP_PATTERNS:
            matched = pattern.search(normalized)
            if not matched:
                continue
            return {
                "column": str(matched.group("column") or "").strip(),
                "table": str(matched.group("table") or "").strip(),
            }
        return None

    def _schema_describe_plan(self, question: str) -> dict[str, Any] | None:
        """Generate a SHOW COLUMNS plan when user asks about a table's schema."""
        table_name = self._schema_describe_match(question)
        if not table_name:
            return None

        # Only trust explicit 3-part FQN references
        parts = table_name.split(".")
        if len(parts) != 3 or not all(p.strip() for p in parts):
            return None

        logger.info("Schema describe shortcut for table: %s", table_name)
        return {
            "path_type": "SCHEMA_DESCRIBE",
            "intent": "SCHEMA_DESCRIBE",
            "confidence": "HIGH",
            "compiled_sql": f"SHOW COLUMNS FROM {table_name}",
            "source_tables": [table_name],
        }

    @staticmethod
    def _schema_describe_match(question: str) -> str | None:
        """Extract FQN table name from a schema-describe question."""
        normalized = " ".join(str(question or "").split())
        for pattern in _SCHEMA_DESCRIBE_PATTERNS:
            matched = pattern.search(normalized)
            if matched:
                return str(matched.group("table") or "").strip()
        return None

    def _catalog_has_column(self, table_name: str, column_name: str) -> bool:
        catalog = getattr(self.schema_selector, "_full_column_catalog", None) or {}
        if not catalog:
            return True

        resolved_table = self.semantic_assets.resolve_table_name(
            table_name,
            set(catalog.keys()),
        )
        columns = catalog.get(resolved_table)
        if not columns:
            # If the table is a valid 3-part FQN (catalog.schema.table) but
            # isn't in our preprocessed catalog, trust the user's explicit
            # reference. Trino will validate at execution time.
            parts = table_name.split(".")
            if len(parts) == 3 and all(p.strip() for p in parts):
                logger.info(
                    "Table %s not in catalog; trusting explicit FQN reference",
                    table_name,
                )
                return True
            return False

        known_columns = {
            str(col.get("name") or col.get("column_name") or "").strip().lower()
            for col in columns
            if col.get("name") or col.get("column_name")
        }
        return str(column_name or "").strip().lower() in known_columns

    def _metadata_lookup_response(self, ctx: PipelineContext) -> dict | None:
        rows = self._metadata_lookup_rows(ctx)
        if not rows:
            ctx.validation_issues.append("metadata_lookup_no_candidates")
            return {
                "status": "ambiguous",
                "question": ctx.user_question,
                "message": (
                    "I treated this as a metadata lookup question, but I couldn't find a "
                    "confident table candidate. Try adding more keywords like service, domain, "
                    "or log type."
                ),
                "suggestions": [],
                "validation_issues": ctx.validation_issues[:5],
                "difficulty": ctx.difficulty.value if ctx.difficulty else "unknown",
                "confidence": "LOW",
            }

        import pandas as pd

        ctx.query_plan = {
            "path_type": "METADATA_LOOKUP",
            "intent": "METADATA_LOOKUP",
            "confidence": "HIGH" if rows[0].get("source") == "openmetadata" else "MEDIUM",
            "compiled_sql": "",
            "source_tables": [row["table_name"] for row in rows],
            "metadata_lookup": True,
        }
        ctx.selected_tables = [row["table_name"] for row in rows]
        ctx.result_df = pd.DataFrame(rows)
        ctx.row_count = len(rows)
        ctx.selected_sql = "-- metadata lookup request; no SQL executed"
        ctx.chart_type = "TABLE"
        ctx.chart_config = {}
        ctx.confidence = ctx.query_plan["confidence"]
        top_table = rows[0]["table_name"]
        ctx.nl_summary = (
            f"I treated this as a metadata lookup question and found {len(rows)} candidate "
            f"table{'s' if len(rows) != 1 else ''}. The strongest match is `{top_table}`. "
            "Review the candidates in the data tab, then ask for schema details or rows from one "
            "of them."
        )
        ctx.follow_up_suggestions = [
            f"Describe the schema of {top_table}",
            f"Show today's rows from {top_table}",
            f"What columns in {top_table} indicate query time or status?",
        ]
        return self._success_response(ctx)

    def _metadata_lookup_rows(self, ctx: PipelineContext) -> list[dict]:
        aggregated: dict[str, dict[str, Any]] = {}
        question_tokens = self._metadata_tokens(ctx.user_question)

        def add_candidate(
            table_name: str,
            source: str,
            score: float,
            reason: str,
            evidence_text: str = "",
        ) -> None:
            normalized = table_name.strip()
            if not normalized:
                return
            key = normalized.lower()
            lexical_score = self._metadata_lexical_score(
                question_tokens,
                normalized,
                evidence_text or reason,
            )
            adjusted_score = self._metadata_adjusted_score(
                base_score=float(score),
                lexical_score=lexical_score,
                source=source,
            )
            existing = aggregated.get(key)
            if not existing:
                aggregated[key] = {
                    "table_name": normalized,
                    "score": adjusted_score,
                    "source": source,
                    "reasons": [reason[:220]] if reason else [],
                    "sources": {source},
                }
                return

            existing["score"] = max(existing["score"], adjusted_score)
            existing["sources"].add(source)
            if reason and reason[:220] not in existing["reasons"]:
                existing["reasons"].append(reason[:220])
            if source == "openmetadata" or (
                source.startswith("documentation") and existing["source"] != "openmetadata"
            ):
                existing["source"] = source

        for candidate in sorted(
            ctx.om_table_candidates,
            key=lambda item: item.get("score", 0.0),
            reverse=True,
        )[:8]:
            table_name = candidate.get("fqn") or candidate.get("name", "")
            description = candidate.get("description", "") or ""
            domain = candidate.get("domain", "")
            reason_parts = []
            if description:
                reason_parts.append(description.strip().replace("\n", " ")[:140])
            if domain:
                reason_parts.append(f"domain={domain}")
            add_candidate(
                table_name,
                source="openmetadata",
                score=float(candidate.get("score", 0.0) or 0.0),
                reason="; ".join(reason_parts) or "Matched OpenMetadata semantic search",
                evidence_text=f"{table_name} {description} {domain}",
            )

        for table_name in ctx.preferred_tables[:8]:
            add_candidate(
                table_name,
                source="semantic_assets",
                score=0.72,
                reason="Matched semantic assets or verified examples",
                evidence_text=table_name,
            )

        for snippet in ctx.doc_snippets[:8]:
            related_tables = list(snippet.get("related_tables") or [])
            if snippet.get("table"):
                related_tables.append(snippet["table"])
            reason = snippet.get("title") or snippet.get("content", "")
            source = f"documentation:{snippet.get('trust_level', 'reference')}"
            score = float(snippet.get("similarity", 0.0) or 0.0)
            for table_name in related_tables:
                add_candidate(
                    table_name,
                    source=source,
                    score=score,
                    reason=reason,
                    evidence_text=(
                        f"{table_name} {snippet.get('title', '')} "
                        f"{snippet.get('content', '')} "
                        f"{' '.join(snippet.get('related_metrics', []) or [])}"
                    ),
                )

        for table_name, score in self._catalog_table_candidates(ctx.user_question)[:8]:
            add_candidate(
                table_name,
                source="schema_catalog",
                score=score,
                reason="Matched table name in local schema catalog",
                evidence_text=table_name,
            )

        candidates = []
        for item in aggregated.values():
            multi_signal_bonus = min(0.06, 0.02 * max(0, len(item["sources"]) - 1))
            item["score"] = round(min(1.0, item["score"] + multi_signal_bonus), 3)
            candidates.append(
                {
                    "table_name": item["table_name"],
                    "source": item["source"],
                    "score": item["score"],
                    "reason": " | ".join(item["reasons"][:3]),
                }
            )

        candidates.sort(key=lambda item: item["score"], reverse=True)
        return candidates[:8]

    def _catalog_table_candidates(self, question: str) -> list[tuple[str, float]]:
        catalog = getattr(self.schema_selector, "_full_column_catalog", None) or {}
        if not catalog:
            return []

        question_tokens = self._metadata_tokens(question)
        if not question_tokens:
            return []

        scored: list[tuple[str, float]] = []
        for table_name in catalog.keys():
            table_tokens = self._metadata_tokens(table_name)
            overlap = question_tokens & table_tokens
            if not overlap:
                continue
            score = self._metadata_lexical_score(question_tokens, table_name)
            scored.append((table_name, min(0.85, 0.45 + score)))

        scored.sort(key=lambda item: item[1], reverse=True)
        return scored

    @staticmethod
    def _metadata_tokens(text: str) -> set[str]:
        tokens = {
            token
            for token in re.findall(r"[a-z0-9_]+", str(text).lower())
            if len(token) > 2 and token not in _METADATA_SKIP_TOKENS
        }
        expanded = set(tokens)
        for token in list(tokens):
            expanded.update(part for part in token.split("_") if len(part) > 2)
            if token.endswith("ies") and len(token) > 4:
                expanded.add(token[:-3] + "y")
            elif token.endswith("s") and len(token) > 3:
                expanded.add(token[:-1])
        return expanded

    @classmethod
    def _metadata_lexical_score(
        cls,
        question_tokens: set[str],
        table_name: str,
        evidence_text: str = "",
    ) -> float:
        if not question_tokens:
            return 0.0
        candidate_tokens = cls._metadata_tokens(f"{table_name} {evidence_text}")
        overlap = question_tokens & candidate_tokens
        if not overlap:
            return 0.0
        coverage = len(overlap) / max(len(question_tokens), 1)
        exact_table_overlap = len(question_tokens & cls._metadata_tokens(table_name))
        return min(0.35, coverage * 0.28 + exact_table_overlap * 0.03)

    @staticmethod
    def _metadata_adjusted_score(base_score: float, lexical_score: float, source: str) -> float:
        source_bonus = 0.0
        if source == "openmetadata":
            source_bonus = 0.05
        elif source.startswith("documentation:canonical"):
            source_bonus = 0.04
        elif source.startswith("documentation:reviewed"):
            source_bonus = 0.03
        elif source == "schema_catalog":
            source_bonus = 0.02

        if lexical_score == 0.0:
            adjusted = min(0.55, (0.25 * base_score) + source_bonus)
        else:
            adjusted = (0.35 * base_score) + lexical_score + source_bonus
        return min(1.0, adjusted)

    def _ambiguous_response(self, ctx: PipelineContext) -> dict:
        # Build "Did you mean?" suggestions from retrieval results
        suggestions: list[str] = []

        # 1. Similar past questions (highest signal)
        for sq in ctx.similar_queries[:5]:
            q = sq.get("question", "").strip()
            sim = sq.get("similarity", 0.0)
            if q and sim >= 0.40 and q.lower() != ctx.user_question.lower():
                suggestions.append(q)

        # 2. Glossary terms that partially matched
        seen = {s.lower() for s in suggestions}
        for gm in ctx.glossary_matches[:5]:
            term = gm.get("term", "").strip()
            definition = gm.get("definition", "").strip()
            if term and term.lower() not in seen:
                hint = f"Ask about '{term}'"
                if definition:
                    hint += f" — {definition[:80]}"
                suggestions.append(hint)
                seen.add(term.lower())

        # Cap at 5 suggestions
        suggestions = suggestions[:5]

        message = (
            "Your question is ambiguous. Could you be more specific about what data you need?"
        )
        if ctx.validation_issues:
            message = (
                "I couldn't validate the SQL or result confidently enough to return it. "
                "Try narrowing the metric, dimension, or time range."
            )
        if suggestions:
            if ctx.validation_issues:
                message = (
                    "I couldn't validate the SQL or result confidently enough to return it. "
                    "One of these narrower questions may work:"
                )
            else:
                message = "I'm not sure what you mean. Did you mean one of these?"

        return {
            "status": "ambiguous",
            "question": ctx.user_question,
            "message": message,
            "suggestions": suggestions,
            "validation_issues": ctx.validation_issues[:5],
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
