"""
RAVEN Web — Route Handlers
============================
Modular route definitions for the FastAPI application.
"""

from __future__ import annotations

import logging
import json as json_module
import uuid
import asyncio
from pathlib import Path

from fastapi import APIRouter, HTTPException, UploadFile, File, Depends
from fastapi.responses import StreamingResponse
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
    suggestions: list[str] = []
    debug: dict = {}
    cached: bool = False
    is_followup: bool = False
    original_question: str = ""
    verified: bool = False


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


# ── Stage name → display label mapping
_STAGE_LABELS = {
    "router": "Understanding question",
    "retrieval": "Finding relevant context",
    "schema_selection": "Selecting tables",
    "probes": "Running test probes",
    "generation": "Generating SQL",
    "validation": "Validating candidates",
    "execute_render": "Executing query",
}


@query_router.post("/query/stream")
async def query_stream(request: QueryRequest, pipeline=Depends(get_pipeline)):
    """SSE streaming endpoint — sends stage progress events then final result."""
    query_id = str(uuid.uuid4())[:8]
    event_queue: asyncio.Queue = asyncio.Queue()

    async def stage_hook(stage_name: str, event: str, detail: dict):
        label = _STAGE_LABELS.get(stage_name, stage_name)
        await event_queue.put({
            "type": "stage",
            "stage": stage_name,
            "label": label,
            "event": event,
            "detail": detail,
        })

    async def run_pipeline():
        try:
            result = await pipeline.generate(
                question=request.question,
                conversation_id=request.conversation_id,
                stage_hook=stage_hook,
            )
            result["query_id"] = query_id
            await event_queue.put({"type": "result", "data": result})
        except Exception as e:
            await event_queue.put({
                "type": "error",
                "error": str(e),
            })
        finally:
            await event_queue.put(None)  # sentinel

    async def event_generator():
        task = asyncio.create_task(run_pipeline())
        try:
            while True:
                event = await event_queue.get()
                if event is None:
                    break
                event_type = event.pop("type", "message")
                yield f"event: {event_type}\ndata: {json_module.dumps(event, default=str)}\n\n"
        except asyncio.CancelledError:
            task.cancel()
            raise
        finally:
            if not task.done():
                task.cancel()

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@query_router.post("/feedback", response_model=FeedbackResponse)
async def feedback(request: FeedbackRequest, pipeline=Depends(get_pipeline)):
    """Submit feedback for a query result."""
    from src.raven.metrics import METRICS
    result = await pipeline.feedback.submit_feedback(
        query_id=request.query_id,
        feedback=request.feedback,
        correction_sql=request.correction_sql,
        correction_notes=request.correction_notes,
    )
    METRICS.record_feedback(request.feedback)
    return FeedbackResponse(**result)


@query_router.get("/suggestions")
async def suggestions():
    """Return onboarding question suggestions from semantic model."""
    import yaml
    try:
        model_path = Path("config/semantic_model.yaml")
        if not model_path.exists():
            model_path = Path("config/semantic_model.example.yaml")
        with open(model_path) as f:
            model = yaml.safe_load(f)

        items = []
        for vq in model.get("verified_queries", []):
            if vq.get("use_as_onboarding"):
                items.append({
                    "question": vq["question"],
                    "category": vq.get("category", "general"),
                })
        return {"suggestions": items[:8]}
    except Exception as e:
        logger.warning("Failed to load suggestions: %s", e)
        return {"suggestions": []}


@query_router.get("/schema/tables")
async def schema_tables():
    """Return table list and relationships for schema explorer."""
    import json as json_mod
    try:
        catalog_path = Path("data/schema_catalog.json")
        if not catalog_path.exists():
            return {"tables": [], "relationships": []}
        with open(catalog_path) as f:
            catalog = json_mod.load(f)

        tables = []
        for entry in catalog:
            name = entry.get("table_name", "")
            cols = []
            for c in entry.get("columns", []):
                cols.append({
                    "name": c.get("column_name", c.get("name", "")),
                    "type": c.get("data_type", c.get("type", "")),
                })
            tables.append({"table_name": name, "columns": cols})

        # Load relationships from graph if available
        relationships = []
        graph_path = Path("data/table_graph.gpickle")
        if graph_path.exists():
            import pickle
            with open(graph_path, "rb") as f:
                G = pickle.load(f)
            for u, v, data in G.edges(data=True):
                relationships.append({
                    "from_table": u,
                    "to_table": v,
                    "join_key": data.get("join_key", data.get("label", "")),
                })

        return {"tables": tables, "relationships": relationships}
    except Exception as e:
        logger.warning("Failed to load schema: %s", e)
        return {"tables": [], "relationships": []}


# ── Metrics Routes ────────────────────────────────────────────────────


@metrics_router.get("/metrics")
async def metrics():
    """Prometheus-compatible metrics endpoint."""
    from src.raven.metrics import METRICS
    from fastapi.responses import Response
    return Response(
        content=METRICS.generate_metrics(),
        media_type=METRICS.content_type,
    )


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


# ── Glossary Routes ───────────────────────────────────────────────


GLOSSARY_FILE = Path("config/glossary_terms.json")


def _load_glossary() -> list[dict]:
    """Load glossary terms from JSON file."""
    if not GLOSSARY_FILE.exists():
        return []
    try:
        import json
        return json.loads(GLOSSARY_FILE.read_text())
    except Exception:
        return []


def _save_glossary(terms: list[dict]):
    """Persist glossary terms to JSON file."""
    import json
    GLOSSARY_FILE.parent.mkdir(parents=True, exist_ok=True)
    GLOSSARY_FILE.write_text(json.dumps(terms, indent=2))


@admin_router.get("/glossary")
async def list_glossary():
    """List all business glossary terms."""
    return {"terms": _load_glossary()}


@admin_router.post("/glossary")
async def add_glossary_term(term: dict):
    """Add a new glossary term."""
    terms = _load_glossary()
    term.setdefault("id", int(uuid.uuid4().int % 1e9))
    terms.append(term)
    _save_glossary(terms)
    logger.info("Glossary term added: %s", term.get("term"))
    return {"status": "created", "term": term}


@admin_router.put("/glossary/{term_id}")
async def update_glossary_term(term_id: int, term: dict):
    """Update an existing glossary term."""
    terms = _load_glossary()
    for i, t in enumerate(terms):
        if t.get("id") == term_id:
            terms[i] = {**term, "id": term_id}
            _save_glossary(terms)
            return {"status": "updated", "term": terms[i]}
    raise HTTPException(status_code=404, detail="Term not found")


@admin_router.delete("/glossary/{term_id}")
async def delete_glossary_term(term_id: int):
    """Delete a glossary term."""
    terms = _load_glossary()
    filtered = [t for t in terms if t.get("id") != term_id]
    if len(filtered) == len(terms):
        raise HTTPException(status_code=404, detail="Term not found")
    _save_glossary(filtered)
    return {"status": "deleted"}

