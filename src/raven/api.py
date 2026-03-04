"""
RAVEN — FastAPI Application
============================
REST API for the text-to-SQL pipeline.
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from .connectors.openai_client import OpenAIClient
from .connectors.pgvector_store import PgVectorStore
from .connectors.trino_connector import TrinoConnector
from .pipeline import Pipeline

logger = logging.getLogger(__name__)

# ── Global pipeline instance ──────────────────────────────────────────
_pipeline: Pipeline | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize connectors and pipeline on startup."""
    global _pipeline

    trino = TrinoConnector(
        host=os.getenv("TRINO_HOST", "localhost"),
        port=int(os.getenv("TRINO_PORT", "8080")),
        user=os.getenv("TRINO_USER", "raven"),
        catalog=os.getenv("TRINO_CATALOG", "iceberg"),
        schema=os.getenv("TRINO_SCHEMA", "gold_dbt"),
    )

    pgvector = PgVectorStore(
        host=os.getenv("PGVECTOR_HOST", "localhost"),
        port=int(os.getenv("PGVECTOR_PORT", "5433")),
        database=os.getenv("PGVECTOR_DB", "raven"),
        user=os.getenv("PGVECTOR_USER", "raven"),
        password=os.getenv("PGVECTOR_PASSWORD", "raven_dev"),
    )

    openai_client = OpenAIClient()

    _pipeline = Pipeline(trino=trino, pgvector=pgvector, openai=openai_client)

    logger.info("RAVEN pipeline initialized")
    yield
    logger.info("RAVEN shutting down")


app = FastAPI(
    title="RAVEN",
    description="Retrieval-Augmented Validated Engine for Natural-language SQL",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Request / Response Models ─────────────────────────────────────────


class QueryRequest(BaseModel):
    question: str = Field(..., min_length=1, max_length=2000, description="Natural language question")
    conversation_id: str | None = Field(None, description="Optional conversation ID for context")


class QueryResponse(BaseModel):
    status: str
    question: str
    sql: str = ""
    data: list[dict] = []
    row_count: int = 0
    chart_type: str = "TABLE"
    chart_config: dict = {}
    summary: str = ""
    confidence: str = "LOW"
    difficulty: str = "unknown"
    timings: dict = {}
    cost: float = 0.0
    message: str = ""
    error: str = ""


class FeedbackRequest(BaseModel):
    query_id: str
    feedback: str = Field(..., pattern="^(thumbs_up|thumbs_down)$")
    correction_sql: str | None = None
    correction_notes: str | None = None


class FeedbackResponse(BaseModel):
    query_id: str
    feedback: str
    action: str


# ── Endpoints ─────────────────────────────────────────────────────────


@app.get("/health")
async def health():
    return {"status": "ok", "service": "raven"}


@app.post("/api/query", response_model=QueryResponse)
async def query(request: QueryRequest):
    """Submit a natural language question to the text-to-SQL pipeline."""
    if _pipeline is None:
        raise HTTPException(status_code=503, detail="Pipeline not initialized")

    result = await _pipeline.generate(
        question=request.question,
        conversation_id=request.conversation_id,
    )

    return QueryResponse(**result)


@app.post("/api/feedback", response_model=FeedbackResponse)
async def feedback(request: FeedbackRequest):
    """Submit feedback for a query result."""
    if _pipeline is None:
        raise HTTPException(status_code=503, detail="Pipeline not initialized")

    result = await _pipeline.feedback.submit_feedback(
        query_id=request.query_id,
        feedback=request.feedback,
        correction_sql=request.correction_sql,
        correction_notes=request.correction_notes,
    )

    return FeedbackResponse(**result)


@app.get("/api/stats")
async def stats():
    """Get pipeline cost and performance stats."""
    if _pipeline is None:
        raise HTTPException(status_code=503, detail="Pipeline not initialized")

    return {
        "cost_summary": _pipeline.openai.get_cost_summary(),
    }
