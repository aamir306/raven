"""
RAVEN Metrics — Prometheus Instrumentation
============================================
Centralized metrics registry for the RAVEN pipeline.

Metrics tracked:
  - Query latency (total + per-stage), labeled by difficulty
  - Query counter (total, by difficulty, by status)
  - Cache hit/miss counter
  - Error counter by stage and error type
  - Cost gauge (per-query, cumulative)
  - Token usage counter
  - Confidence distribution

Usage::

    from src.raven.metrics import METRICS
    METRICS.query_started("SIMPLE")
    with METRICS.stage_timer("router"):
        await router.classify(...)
    METRICS.query_completed("SIMPLE", "success", cost=0.15, tokens=1200)
"""

from __future__ import annotations

import time
import logging
from contextlib import contextmanager
from typing import Generator

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    Summary,
    generate_latest,
    CONTENT_TYPE_LATEST,
)

logger = logging.getLogger(__name__)

# ── Custom registry (avoid polluting default with process/gc metrics) ─
REGISTRY = CollectorRegistry()

# ── Buckets tuned for text-to-SQL latencies ───────────────────────────
LATENCY_BUCKETS = (0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 30.0, 60.0, 90.0, 120.0, 180.0)
STAGE_BUCKETS = (0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0)
COST_BUCKETS = (0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0)


class RavenMetrics:
    """Singleton-style metrics container for the RAVEN pipeline."""

    def __init__(self, registry: CollectorRegistry = REGISTRY):
        self.registry = registry

        # ── Query-level metrics ───────────────────────────────────────

        self.queries_total = Counter(
            "raven_queries_total",
            "Total number of queries processed",
            labelnames=["difficulty", "status"],
            registry=registry,
        )

        self.queries_in_flight = Gauge(
            "raven_queries_in_flight",
            "Number of queries currently being processed",
            registry=registry,
        )

        self.query_latency = Histogram(
            "raven_query_latency_seconds",
            "End-to-end query latency in seconds",
            labelnames=["difficulty", "cached"],
            buckets=LATENCY_BUCKETS,
            registry=registry,
        )

        self.query_cost = Histogram(
            "raven_query_cost_usd",
            "Cost per query in USD",
            labelnames=["difficulty"],
            buckets=COST_BUCKETS,
            registry=registry,
        )

        self.cost_total = Counter(
            "raven_cost_usd_total",
            "Cumulative cost in USD",
            registry=registry,
        )

        # ── Stage-level metrics ───────────────────────────────────────

        self.stage_latency = Histogram(
            "raven_stage_latency_seconds",
            "Latency per pipeline stage in seconds",
            labelnames=["stage"],
            buckets=STAGE_BUCKETS,
            registry=registry,
        )

        self.stage_errors = Counter(
            "raven_stage_errors_total",
            "Errors per pipeline stage",
            labelnames=["stage", "error_type"],
            registry=registry,
        )

        # ── Cache metrics ─────────────────────────────────────────────

        self.cache_hits = Counter(
            "raven_cache_hits_total",
            "Number of cache hits",
            registry=registry,
        )

        self.cache_misses = Counter(
            "raven_cache_misses_total",
            "Number of cache misses",
            registry=registry,
        )

        # ── Token usage ───────────────────────────────────────────────

        self.tokens_total = Counter(
            "raven_tokens_total",
            "Total tokens consumed",
            labelnames=["model", "type"],  # type: prompt / completion
            registry=registry,
        )

        # ── Confidence distribution ───────────────────────────────────

        self.confidence_total = Counter(
            "raven_confidence_total",
            "Queries by confidence level",
            labelnames=["confidence"],
            registry=registry,
        )

        # ── Feedback ──────────────────────────────────────────────────

        self.feedback_total = Counter(
            "raven_feedback_total",
            "User feedback submissions",
            labelnames=["feedback_type"],  # thumbs_up / thumbs_down
            registry=registry,
        )

    # ── Convenience methods ───────────────────────────────────────────

    def query_started(self) -> None:
        """Call when a query enters the pipeline."""
        self.queries_in_flight.inc()

    def query_completed(
        self,
        difficulty: str,
        status: str,
        *,
        cached: bool = False,
        latency: float = 0.0,
        cost: float = 0.0,
        confidence: str = "",
    ) -> None:
        """Call after a query finishes (success, error, or ambiguous)."""
        self.queries_in_flight.dec()
        self.queries_total.labels(difficulty=difficulty, status=status).inc()
        self.query_latency.labels(
            difficulty=difficulty, cached=str(cached).lower()
        ).observe(latency)

        if cost > 0:
            self.query_cost.labels(difficulty=difficulty).observe(cost)
            self.cost_total.inc(cost)

        if confidence:
            self.confidence_total.labels(confidence=confidence).inc()

    def observe_stage(self, stage: str, elapsed: float) -> None:
        """Record a stage's latency."""
        self.stage_latency.labels(stage=stage).observe(elapsed)

    def record_stage_error(self, stage: str, error: Exception) -> None:
        """Record that a stage raised an error."""
        error_type = type(error).__name__
        self.stage_errors.labels(stage=stage, error_type=error_type).inc()

    def record_cache_hit(self) -> None:
        self.cache_hits.inc()

    def record_cache_miss(self) -> None:
        self.cache_misses.inc()

    def record_tokens(self, model: str, prompt_tokens: int, completion_tokens: int) -> None:
        """Record token usage from an LLM call."""
        if prompt_tokens > 0:
            self.tokens_total.labels(model=model, type="prompt").inc(prompt_tokens)
        if completion_tokens > 0:
            self.tokens_total.labels(model=model, type="completion").inc(completion_tokens)

    def record_feedback(self, feedback_type: str) -> None:
        """Record thumbs_up or thumbs_down."""
        self.feedback_total.labels(feedback_type=feedback_type).inc()

    @contextmanager
    def stage_timer(self, stage: str) -> Generator[None, None, None]:
        """Context manager to time a pipeline stage."""
        start = time.monotonic()
        try:
            yield
        except Exception as e:
            self.record_stage_error(stage, e)
            raise
        finally:
            self.observe_stage(stage, time.monotonic() - start)

    def generate_metrics(self) -> bytes:
        """Generate Prometheus exposition format."""
        return generate_latest(self.registry)

    @property
    def content_type(self) -> str:
        return CONTENT_TYPE_LATEST


# ── Module-level singleton ────────────────────────────────────────────
METRICS = RavenMetrics()
