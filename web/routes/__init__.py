"""
RAVEN Web — Route Handlers
============================
Modular route definitions for the FastAPI application.
"""

from __future__ import annotations

import logging
import uuid
from pathlib import Path

from fastapi import APIRouter, HTTPException, UploadFile, File, Depends
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

# ── Routers ───────────────────────────────────────────────────────────

query_router = APIRouter(prefix="/api", tags=["query"])
admin_router = APIRouter(prefix="/api/admin", tags=["admin"])
metrics_router = APIRouter(prefix="/api", tags=["metrics"])


# ── Models ────────────────────────────────────────────────────────────


class QueryRequest(BaseModel):
    question: str = Field(..., min_length=1, max_length=2000)
    conversation_id: str | None = None


class QueryResponse(BaseModel):
    status: str
    query_id: str = ""
    question: str = ""
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


class RefreshRequest(BaseModel):
    stages: list[str] = Field(
        default=["all"],
        description="Preprocessing stages to refresh: dbt, lsh, glossary, docs, graph, content, all",
    )
    dry_run: bool = False


class RefreshResponse(BaseModel):
    status: str
    stages_triggered: list[str]
    message: str


class UploadDocResponse(BaseModel):
    status: str
    filename: str
    chunks_created: int
    message: str


# ── Dependency: get pipeline ──────────────────────────────────────────

def get_pipeline():
    """Dependency to get the global pipeline instance."""
    from src.raven.api import _pipeline
    if _pipeline is None:
        raise HTTPException(status_code=503, detail="Pipeline not initialized")
    return _pipeline


# ── Query Routes ──────────────────────────────────────────────────────


@query_router.post("/query", response_model=QueryResponse)
async def query(request: QueryRequest, pipeline=Depends(get_pipeline)):
    """Submit a natural language question to the text-to-SQL pipeline."""
    query_id = str(uuid.uuid4())[:8]
    result = await pipeline.generate(
        question=request.question,
        conversation_id=request.conversation_id,
    )
    return QueryResponse(query_id=query_id, **result)


@query_router.post("/feedback", response_model=FeedbackResponse)
async def feedback(request: FeedbackRequest, pipeline=Depends(get_pipeline)):
    """Submit feedback for a query result."""
    result = await pipeline.feedback.submit_feedback(
        query_id=request.query_id,
        feedback=request.feedback,
        correction_sql=request.correction_sql,
        correction_notes=request.correction_notes,
    )
    return FeedbackResponse(**result)


# ── Metrics Routes ────────────────────────────────────────────────────


@metrics_router.get("/metrics")
async def metrics(pipeline=Depends(get_pipeline)):
    """Prometheus-compatible metrics endpoint."""
    cost = pipeline.openai.get_cost_summary()
    lines = [
        "# HELP raven_queries_total Total queries processed",
        "# TYPE raven_queries_total counter",
        f'raven_queries_total {cost.get("total_queries", 0)}',
        "",
        "# HELP raven_cost_usd_total Total cost in USD",
        "# TYPE raven_cost_usd_total counter",
        f'raven_cost_usd_total {cost.get("total_cost", 0.0)}',
        "",
        "# HELP raven_tokens_total Total tokens used",
        "# TYPE raven_tokens_total counter",
        f'raven_tokens_total {cost.get("total_tokens", 0)}',
    ]
    from fastapi.responses import PlainTextResponse
    return PlainTextResponse("\n".join(lines), media_type="text/plain")


@metrics_router.get("/stats")
async def stats(pipeline=Depends(get_pipeline)):
    """Get pipeline cost and performance stats."""
    return {"cost_summary": pipeline.openai.get_cost_summary()}


# ── Admin Routes ──────────────────────────────────────────────────────

UPLOAD_DIR = Path("data/uploads")


@admin_router.post("/upload-doc", response_model=UploadDocResponse)
async def upload_doc(
    file: UploadFile = File(...),
    pipeline=Depends(get_pipeline),
):
    """Upload a documentation file (Markdown, PDF, Word)."""
    allowed = {".md", ".txt", ".pdf", ".docx", ".doc", ".yaml", ".yml"}
    suffix = Path(file.filename or "").suffix.lower()
    if suffix not in allowed:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type: {suffix}. Allowed: {', '.join(sorted(allowed))}",
        )

    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    dest = UPLOAD_DIR / (file.filename or "uploaded_doc")

    content = await file.read()
    dest.write_bytes(content)
    logger.info("Uploaded doc: %s (%d bytes)", dest.name, len(content))

    # TODO: Phase 3 — auto-ingest uploaded doc into pgvector
    chunks_created = 0

    return UploadDocResponse(
        status="uploaded",
        filename=dest.name,
        chunks_created=chunks_created,
        message=f"File saved. Ingestion into vector store is pending (Phase 3).",
    )


@admin_router.post("/refresh", response_model=RefreshResponse)
async def refresh(request: RefreshRequest, pipeline=Depends(get_pipeline)):
    """Trigger preprocessing refresh for specified stages."""
    valid_stages = {"dbt", "lsh", "glossary", "docs", "graph", "content", "all"}
    for s in request.stages:
        if s not in valid_stages:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown stage: {s}. Valid: {', '.join(sorted(valid_stages))}",
            )

    stages = request.stages
    if "all" in stages:
        stages = sorted(valid_stages - {"all"})

    # TODO: Phase 3 — actually run preprocessing stages
    logger.info("Refresh requested: %s (dry_run=%s)", stages, request.dry_run)

    return RefreshResponse(
        status="accepted" if not request.dry_run else "dry_run",
        stages_triggered=stages,
        message="Refresh queued." if not request.dry_run else "Dry run — no changes made.",
    )
