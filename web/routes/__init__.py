"""
RAVEN Web — Route Handlers
============================
Modular route definitions for the FastAPI application.
"""

from __future__ import annotations

import logging
import json as json_module
import os
import uuid
import asyncio
from pathlib import Path

import re as _re

from fastapi import APIRouter, HTTPException, UploadFile, File, Depends
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

# ── URL / question helpers ────────────────────────────────────────────

_URL_RE = _re.compile(r'https?://\S+')
_META_Q_RE = _re.compile(
    r'^(what\s+is|describe|show|tell\s+me\s+about|explain|summarize|summarise|info\s+on)'
    r'\s+(this|the|that)?\s*(dashboard|report|question|link)?\s*[\?\s]*$',
    _re.IGNORECASE,
)


def _clean_question(question: str, focus=None) -> str:
    """Strip URLs from a question and, if the remainder is a meta-question
    about a dashboard, synthesize a concrete data question from focus context."""
    cleaned = _URL_RE.sub('', question).strip()
    # If nothing meaningful remains after URL removal, use focus context
    if not cleaned or _META_Q_RE.match(cleaned) or cleaned in ('?', ''):
        if focus and getattr(focus, 'tables', None):
            table_str = ', '.join(focus.tables[:10])
            name = getattr(focus, 'name', 'this dashboard')
            cleaned = f"Show summary data from {name} covering tables: {table_str}"
        elif focus and getattr(focus, 'name', None):
            cleaned = f"Show data from {focus.name}"
        else:
            # Fall back to original (let the router decide)
            return question
    return cleaned

# ── Routers ───────────────────────────────────────────────────────────

query_router = APIRouter(prefix="/api", tags=["query"])
admin_router = APIRouter(prefix="/api/admin", tags=["admin"])
metrics_router = APIRouter(prefix="/api", tags=["metrics"])
focus_router = APIRouter(prefix="/api/focus", tags=["focus"])
metabase_router = APIRouter(prefix="/api/metabase", tags=["metabase"])


# ── Models ────────────────────────────────────────────────────────────


class QueryRequest(BaseModel):
    question: str = Field(..., min_length=1, max_length=2000)
    conversation_id: str | None = None
    focus_id: str | None = None  # UUID of a focus document to scope context
    metabase_url: str | None = None  # Metabase link pasted in chat → auto-focus


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
    focus: dict | None = None
    enhancements: list[dict] = []


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


def _resolve_focus(focus_id: str | None, metabase_url: str | None = None):
    """Resolve a focus document ID or Metabase URL into a FocusContext, or None."""
    # Priority 1: explicit focus document
    if focus_id:
        from src.raven.focus import FocusStore
        store = FocusStore()
        doc = store.get_document(focus_id)
        if doc:
            return doc.to_focus_context()
    # Priority 2: Metabase URL → auto-build FocusContext from link preview
    if metabase_url:
        return _focus_from_metabase_url(metabase_url)
    return None


def _focus_from_metabase_url(url: str):
    """Build a FocusContext from a Metabase URL (sync wrapper for startup)."""
    from src.raven.focus import parse_metabase_url, FocusContext
    info = parse_metabase_url(url)
    if not info:
        return None
    # We can't await here in a sync function, so return a partial FocusContext
    # with the URL metadata. The pipeline will enrich it lazily if needed.
    return FocusContext(
        type=info["type"],
        name=f"Metabase {info['type']} #{info['id']}",
        source_id=str(info["id"]),
    )


async def _resolve_focus_async(focus_id: str | None, metabase_url: str | None = None):
    """Async version: resolves focus, enriching Metabase URLs with live API data."""
    # Priority 1: explicit focus document
    if focus_id:
        from src.raven.focus import FocusStore
        store = FocusStore()
        doc = store.get_document(focus_id)
        if doc:
            return doc.to_focus_context()
    # Priority 2: Metabase URL → full async enrichment
    if metabase_url:
        return await _focus_from_metabase_url_async(metabase_url)
    return None


async def _focus_from_metabase_url_async(url: str):
    """Build a fully-enriched FocusContext from a Metabase URL."""
    from src.raven.focus import parse_metabase_url, FocusContext
    info = parse_metabase_url(url)
    if not info:
        return None
    try:
        client = _get_metabase_client()
    except Exception:
        # Metabase not configured — return minimal context
        return FocusContext(
            type=info["type"],
            name=f"Metabase {info['type']} #{info['id']}",
            source_id=str(info["id"]),
        )
    try:
        if info["type"] == "dashboard":
            meta = await client.get_dashboard_meta(info["id"])
            cards = await client.get_dashboard_cards(info["id"])
            tables = list({t for c in cards for t in c.get("tables", [])})
            return FocusContext(
                type="dashboard",
                name=meta.get("name", f"Dashboard #{info['id']}"),
                source_id=str(info["id"]),
                tables=tables,
                verified_queries=[
                    {"question": c["name"], "sql": c["sql"]} for c in cards if c.get("sql")
                ],
                dashboard_cards=cards,
                dashboard_filters=meta.get("filters", []),
                table_count=len(tables),
                rule_count=0,
                query_count=len(cards),
            )
        elif info["type"] == "question":
            card = await client.get_question(info["id"])
            tables = card.get("tables", [])
            return FocusContext(
                type="question",
                name=card.get("name", f"Question #{info['id']}"),
                source_id=str(info["id"]),
                tables=tables,
                verified_queries=[
                    {"question": card["name"], "sql": card["sql"]} for _ in [1] if card.get("sql")
                ],
                dashboard_cards=[card],
                table_count=len(tables),
                query_count=1 if card.get("sql") else 0,
            )
    except Exception as exc:
        logger.warning("Failed to enrich Metabase focus from %s: %s", url, exc)
        return FocusContext(
            type=info["type"],
            name=f"Metabase {info['type']} #{info['id']}",
            source_id=str(info["id"]),
        )
    return None


@query_router.post("/query", response_model=QueryResponse)
async def query(request: QueryRequest, pipeline=Depends(get_pipeline)):
    """Submit a natural language question to the text-to-SQL pipeline."""
    query_id = str(uuid.uuid4())[:8]
    focus = await _resolve_focus_async(request.focus_id, request.metabase_url)
    question = _clean_question(request.question, focus)
    result = await pipeline.generate(
        question=question,
        conversation_id=request.conversation_id,
        focus=focus,
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
            focus = await _resolve_focus_async(request.focus_id, request.metabase_url)
            question = _clean_question(request.question, focus)
            result = await pipeline.generate(
                question=question,
                conversation_id=request.conversation_id,
                stage_hook=stage_hook,
                focus=focus,
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
    """Upload a documentation file (Markdown, PDF, Word), chunk it, embed, and store."""
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

    # ── Chunk the file ────────────────────────────────────────────────
    from preprocessing.ingest_documentation import (
        chunk_docx, chunk_markdown, chunk_text, chunk_pdf, chunk_annotations,
    )

    _chunkers = {
        ".md": chunk_markdown,
        ".txt": chunk_text,
        ".doc": chunk_text,
        ".docx": chunk_docx,
        ".pdf": chunk_pdf,
        ".yaml": chunk_annotations,
        ".yml": chunk_annotations,
    }
    chunker = _chunkers.get(suffix)
    chunks: list[dict] = []
    if chunker:
        try:
            chunks = chunker(dest)
            logger.info("Chunked %s → %d chunks", dest.name, len(chunks))
        except Exception as exc:
            logger.error("Chunking failed for %s: %s", dest.name, exc)

    # ── Embed & store in pgvector (incremental — no table wipe) ───────
    chunks_stored = 0
    if chunks:
        try:
            texts = [c["text"] for c in chunks]
            embeddings = await pipeline.openai.batch_embed(texts)
            for chunk, emb in zip(chunks, embeddings):
                pipeline.pgvector.insert(
                    table="doc_embeddings",
                    text="",
                    embedding=emb,
                    metadata=chunk.get("metadata", {}),
                    source_file=str(dest),
                    table_ref=chunk.get("section", ""),
                    content=chunk["text"],
                    doc_type=chunk.get("metadata", {}).get("file_type", "unknown"),
                )
                chunks_stored += 1
            logger.info("Stored %d embeddings for %s", chunks_stored, dest.name)
        except Exception as exc:
            logger.error("Embedding/storage failed for %s: %s", dest.name, exc)

    return UploadDocResponse(
        status="indexed" if chunks_stored > 0 else "uploaded",
        filename=dest.name,
        chunks_created=chunks_stored,
        message=(
            f"Indexed {chunks_stored} chunks into vector store."
            if chunks_stored > 0
            else f"File saved but chunking produced 0 chunks (check file content or format)."
        ),
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


# ── Focus Document Routes ─────────────────────────────────────────


def _get_focus_store():
    from src.raven.focus import FocusStore
    return FocusStore()


@focus_router.get("/documents")
async def list_focus_documents():
    """List all focus documents (for / command dropdown)."""
    store = _get_focus_store()
    docs = store.list_documents()
    return {"documents": docs}


@focus_router.get("/documents/{doc_id}")
async def get_focus_document(doc_id: str):
    """Get a single focus document by ID."""
    store = _get_focus_store()
    doc = store.get_document(doc_id)
    if not doc:
        raise HTTPException(status_code=404, detail="Focus document not found")
    return doc.to_dict()


@focus_router.post("/documents")
async def create_focus_document(body: dict):
    """Create a new focus document."""
    from src.raven.focus import FocusDocument
    doc = FocusDocument(
        name=body.get("name", "Untitled"),
        description=body.get("description", ""),
        type=body.get("type", "manual"),
        tables=body.get("tables", []),
        glossary_terms=body.get("glossary_terms", []),
        verified_queries=body.get("verified_queries", []),
        business_rules=body.get("business_rules", []),
        column_notes=body.get("column_notes", {}),
        metabase_dashboard_id=body.get("metabase_dashboard_id"),
        created_by=body.get("created_by", "admin"),
    )
    store = _get_focus_store()
    created = store.create_document(doc)
    return {"status": "created", "document": created.to_dict()}


@focus_router.put("/documents/{doc_id}")
async def update_focus_document(doc_id: str, body: dict):
    """Update a focus document."""
    store = _get_focus_store()
    updated = store.update_document(doc_id, body)
    if not updated:
        raise HTTPException(status_code=404, detail="Focus document not found")
    return {"status": "updated", "document": updated.to_dict()}


@focus_router.delete("/documents/{doc_id}")
async def delete_focus_document(doc_id: str):
    """Delete a focus document."""
    store = _get_focus_store()
    if not store.delete_document(doc_id):
        raise HTTPException(status_code=404, detail="Focus document not found")
    return {"status": "deleted"}


# ── Focus Document Enhancement Suggestions ────────────────────────


@focus_router.get("/suggestions")
async def list_focus_suggestions(document_id: str | None = None, status: str | None = None):
    """List enhancement suggestions, optionally filtered."""
    store = _get_focus_store()
    return {"suggestions": store.list_suggestions(document_id=document_id, status=status)}


@focus_router.post("/suggestions")
async def add_focus_suggestion(body: dict):
    """Create a new enhancement suggestion."""
    store = _get_focus_store()
    entry = store.add_suggestion(
        document_id=body["document_id"],
        suggestion_type=body["suggestion_type"],
        suggestion_data=body.get("suggestion_data", {}),
        source_query_id=body.get("source_query_id"),
    )
    return {"status": "created", "suggestion": entry}


@focus_router.post("/suggestions/{suggestion_id}/review")
async def review_focus_suggestion(suggestion_id: int, body: dict):
    """Accept or reject an enhancement suggestion."""
    action = body.get("action", "")
    if action not in ("accepted", "rejected"):
        raise HTTPException(status_code=400, detail="action must be 'accepted' or 'rejected'")
    store = _get_focus_store()
    result = store.review_suggestion(suggestion_id, action, reviewer=body.get("reviewer", "admin"))
    if not result:
        raise HTTPException(status_code=404, detail="Suggestion not found")
    return {"status": action, "suggestion": result}


# ── Metabase Bridge Routes ────────────────────────────────────────


def _get_metabase_config(browser_overrides: dict | None = None) -> dict:
    """Read Metabase config from env + optional browser-side overrides.

    Priority: browser override > env var > default.
    Browser-sourced values: session_id, database_id, collection_name.
    Server-only values: url, api_key (never stored in browser).
    """
    overrides = browser_overrides or {}
    return {
        "url": os.environ.get("METABASE_URL", ""),
        "api_key": os.environ.get("METABASE_API_KEY", ""),
        "session_id": overrides.get("session_id") or os.environ.get("METABASE_SESSION_ID", ""),
        "database_id": int(overrides.get("database_id") or os.environ.get("METABASE_DATABASE_ID", "0") or "0"),
        "collection_name": overrides.get("collection_name") or os.environ.get("METABASE_COLLECTION", "RAVEN Generated"),
    }


def _get_metabase_client(browser_overrides: dict | None = None):
    from src.raven.connectors.metabase_client import MetabaseClient
    cfg = _get_metabase_config(browser_overrides)
    if not cfg["url"]:
        raise HTTPException(status_code=400, detail="Metabase URL not configured. Set METABASE_URL env var.")
    return MetabaseClient(
        url=cfg["url"],
        api_key=cfg["api_key"] or None,
        session_id=cfg["session_id"] or None,
    )


def _extract_browser_overrides(body: dict) -> dict:
    """Extract browser-sourced Metabase config from request body."""
    out = {}
    if body.get("_mb_session_id"):
        out["session_id"] = body["_mb_session_id"]
    if body.get("_mb_database_id"):
        out["database_id"] = body["_mb_database_id"]
    if body.get("_mb_collection_name"):
        out["collection_name"] = body["_mb_collection_name"]
    return out


RAVEN_NAME_PREFIX = "RAVEN_"


@metabase_router.get("/config")
async def metabase_config():
    """Return current Metabase configuration (masked). Browser overrides are client-side only."""
    cfg = _get_metabase_config()
    return {
        "url": cfg["url"],
        "has_api_key": bool(cfg["api_key"]),
        "has_session_id": bool(cfg["session_id"]),
        "database_id": cfg["database_id"],
        "collection_name": cfg["collection_name"],
    }


@metabase_router.post("/test-connection")
async def metabase_test_connection(body: dict = None):
    """Test Metabase connection. Accepts optional browser overrides."""
    overrides = _extract_browser_overrides(body) if body else {}
    client = _get_metabase_client(overrides)
    result = await client.test_connection()
    return result


@metabase_router.get("/dashboards")
async def metabase_list_dashboards():
    """List all Metabase dashboards."""
    client = _get_metabase_client()
    return {"dashboards": await client.list_dashboards()}


@metabase_router.get("/dashboards/{dashboard_id}/cards")
async def metabase_dashboard_cards(dashboard_id: int):
    """Get cards from a Metabase dashboard (for Focus Mode context)."""
    client = _get_metabase_client()
    cards = await client.get_dashboard_cards(dashboard_id)
    meta = await client.get_dashboard_meta(dashboard_id)
    tables = list({t for c in cards for t in c.get("tables", [])})
    return {
        "dashboard": meta,
        "cards": cards,
        "tables": tables,
    }


@metabase_router.post("/preview-link")
async def metabase_preview_link(body: dict):
    """Fetch metadata for a Metabase URL (inline preview)."""
    from src.raven.focus import parse_metabase_url
    url = body.get("url", "")
    info = parse_metabase_url(url)
    if not info:
        raise HTTPException(status_code=400, detail="Not a valid Metabase URL")
    client = _get_metabase_client()
    if info["type"] == "dashboard":
        meta = await client.get_dashboard_meta(info["id"])
        cards = await client.get_dashboard_cards(info["id"])
        tables = list({t for c in cards for t in c.get("tables", [])})
        return {
            "type": "dashboard",
            "id": info["id"],
            "name": meta.get("name", ""),
            "card_count": meta.get("card_count", len(cards)),
            "table_count": len(tables),
            "tables": tables,
            "owner": meta.get("owner", ""),
            "database_id": meta.get("database_id"),
        }
    elif info["type"] == "question":
        card = await client.get_question(info["id"])
        return {
            "type": "question",
            "id": info["id"],
            "name": card.get("name", ""),
            "sql": card.get("sql", ""),
            "tables": card.get("tables", []),
            "display": card.get("display", "table"),
        }
    else:
        return {"type": info["type"], "id": info["id"]}


@metabase_router.post("/push-question")
async def metabase_push_question(body: dict):
    """Save a RAVEN-generated SQL as a Metabase question."""
    overrides = _extract_browser_overrides(body)
    client = _get_metabase_client(overrides)
    cfg = _get_metabase_config(overrides)
    raw_name = body.get("name", "RAVEN Query")
    name = raw_name if raw_name.startswith(RAVEN_NAME_PREFIX) else f"{RAVEN_NAME_PREFIX}{raw_name}"
    result = await client.create_question(
        name=name,
        sql=body["sql"],
        display=body.get("display", "table"),
        database_id=body.get("database_id") or cfg["database_id"] or 1,
        collection_id=body.get("collection_id"),
        description=body.get("description"),
    )
    return result


@metabase_router.post("/push-dashboard")
async def metabase_push_dashboard(body: dict):
    """Create a Metabase dashboard from multiple questions."""
    overrides = _extract_browser_overrides(body)
    client = _get_metabase_client(overrides)
    cfg = _get_metabase_config(overrides)
    db_id = body.get("database_id") or cfg["database_id"] or 1
    # First create each question
    card_ids = []
    for card in body.get("cards", []):
        raw_name = card.get("name", "RAVEN Card")
        card_name = raw_name if raw_name.startswith(RAVEN_NAME_PREFIX) else f"{RAVEN_NAME_PREFIX}{raw_name}"
        q = await client.create_question(
            name=card_name,
            sql=card["sql"],
            display=card.get("display", "table"),
            database_id=db_id,
            collection_id=body.get("collection_id"),
        )
        card_ids.append(q["id"])
    # Then create dashboard with those cards
    raw_dash = body.get("name", "RAVEN Dashboard")
    dash_name = raw_dash if raw_dash.startswith(RAVEN_NAME_PREFIX) else f"{RAVEN_NAME_PREFIX}{raw_dash}"
    dashboard = await client.create_dashboard(
        name=dash_name,
        card_ids=card_ids,
        collection_id=body.get("collection_id"),
        description=body.get("description"),
    )
    return dashboard


@metabase_router.post("/add-to-dashboard")
async def metabase_add_to_dashboard(body: dict):
    """Add a question to an existing Metabase dashboard."""
    overrides = _extract_browser_overrides(body)
    client = _get_metabase_client(overrides)
    cfg = _get_metabase_config(overrides)
    # Create the question first
    raw_name = body.get("name", "RAVEN Card")
    name = raw_name if raw_name.startswith(RAVEN_NAME_PREFIX) else f"{RAVEN_NAME_PREFIX}{raw_name}"
    q = await client.create_question(
        name=name,
        sql=body["sql"],
        display=body.get("display", "table"),
        database_id=body.get("database_id") or cfg["database_id"] or 1,
    )
    # Add to dashboard
    result = await client.add_card_to_dashboard(
        dashboard_id=body["dashboard_id"],
        card_id=q["id"],
    )
    return result


@metabase_router.get("/collections")
async def metabase_list_collections():
    """List Metabase collections."""
    client = _get_metabase_client()
    return {"collections": await client.list_collections()}

