"""
RAVEN — FastAPI Application
============================
REST API for the text-to-SQL pipeline.
Includes modular routes, middleware, and admin endpoints.
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from .connectors.openai_client import OpenAIClient
from .connectors.pgvector_store import PgVectorStore
from .connectors.trino_connector import TrinoConnector
from .pipeline import Pipeline

# Load .env from project root
load_dotenv(Path(__file__).resolve().parents[2] / ".env")

logger = logging.getLogger(__name__)

# ── Global pipeline instance ──────────────────────────────────────────
_pipeline: Pipeline | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize connectors and pipeline on startup."""
    global _pipeline

    trino = TrinoConnector(
        host=os.getenv("TRINO_HOST", "localhost"),
        port=int(os.getenv("TRINO_PORT", "443")),
        user=os.getenv("TRINO_USER", "admin"),
        catalog=os.getenv("TRINO_CATALOG", "cdp"),
        schema=os.getenv("TRINO_SCHEMA", "default"),
        http_scheme=os.getenv("TRINO_HTTP_SCHEME", "https"),
        password=os.getenv("TRINO_PASSWORD"),
        ssl_insecure=os.getenv("TRINO_SSL_INSECURE", "").lower() in ("true", "1", "yes"),
    )

    pgvector = PgVectorStore(
        host=os.getenv("PGVECTOR_HOST", "localhost"),
        port=int(os.getenv("PGVECTOR_PORT", "5432")),
        dbname=os.getenv("PGVECTOR_DB", "raven"),
        user=os.getenv("PGVECTOR_USER", "postgres"),
        password=os.getenv("PGVECTOR_PASSWORD", "changeme"),
    )

    openai_client = OpenAIClient()

    _pipeline = Pipeline(trino=trino, pgvector=pgvector, openai=openai_client)

    logger.info("RAVEN pipeline initialized")
    yield
    logger.info("RAVEN shutting down")


app = FastAPI(
    title="RAVEN",
    description="Retrieval-Augmented Validated Engine for Natural-language SQL",
    version="0.2.0",
    lifespan=lifespan,
)

# ── Middleware (order matters: last added = first executed) ────────────

app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("RAVEN_CORS_ORIGINS", "*").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

try:
    from web.middleware import BasicAuthMiddleware, RateLimitMiddleware, RequestTimingMiddleware
    app.add_middleware(RequestTimingMiddleware)
    app.add_middleware(RateLimitMiddleware)
    app.add_middleware(BasicAuthMiddleware)
    logger.info("Middleware loaded: auth, rate-limit, timing")
except ImportError:
    logger.warning("web.middleware not found — running without custom middleware")


# ── Routers ───────────────────────────────────────────────────────────

try:
    from web.routes import query_router, admin_router, metrics_router, focus_router, metabase_router
    app.include_router(query_router)
    app.include_router(admin_router)
    app.include_router(metrics_router)
    app.include_router(focus_router)
    app.include_router(metabase_router)
    logger.info("Routers loaded: query, admin, metrics, focus, metabase")
except ImportError:
    logger.warning("web.routes not found — using inline routes only")


# ── Core Endpoints (always available) ─────────────────────────────────


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "service": "raven",
        "version": "0.2.0",
        "pipeline_ready": _pipeline is not None,
    }


# ── Static UI (serve React build if available) ────────────────────────
# IMPORTANT: This must be LAST — the catch-all mount intercepts all unmatched paths.

UI_DIR = Path("web/ui/build")
if UI_DIR.exists():
    app.mount("/", StaticFiles(directory=str(UI_DIR), html=True), name="ui")

