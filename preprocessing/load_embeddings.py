"""
Preprocessing: Unified Embedding Loader
=========================================
Reads all preprocessing artifacts, embeds them via Azure OpenAI,
and loads into pgvector (RDS).

Handles all 4 tables:
  1. schema_embeddings   — from data/schema_embedding_texts.json
  2. question_embeddings — from data/question_embedding_texts.json
  3. doc_embeddings      — from data/doc_chunks.json
  4. glossary_embeddings — from data/glossary_entries.json (if exists)

Usage:
    python -m preprocessing.load_embeddings \
        --schema-texts data/schema_embedding_texts.json \
        --question-texts data/question_embedding_texts.json \
        --doc-chunks data/doc_chunks.json \
        --batch-size 100
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from pathlib import Path

import psycopg2
from openai import AsyncAzureOpenAI

logger = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────

EMBEDDING_DIM = 3072  # text-embedding-3-large
BATCH_SIZE = 100  # texts per API call (stay under token limits)


def get_embed_client():
    """Create Azure OpenAI embedding client from env vars."""
    endpoint = os.getenv("AZURE_OPENAI_EMBED_ENDPOINT", "")
    key = os.getenv("AZURE_OPENAI_EMBED_KEY", "")
    version = os.getenv("AZURE_OPENAI_EMBED_API_VERSION", "2023-05-15")
    deployment = os.getenv("AZURE_OPENAI_EMBED_DEPLOYMENT", "embedlarge")

    if not endpoint or not key:
        logger.error("AZURE_OPENAI_EMBED_ENDPOINT and AZURE_OPENAI_EMBED_KEY must be set")
        sys.exit(1)

    client = AsyncAzureOpenAI(
        api_key=key,
        azure_endpoint=endpoint,
        api_version=version,
    )
    return client, deployment


def get_pgvector_conn():
    """Create psycopg2 connection from env vars."""
    return psycopg2.connect(
        host=os.getenv("PGVECTOR_HOST"),
        port=int(os.getenv("PGVECTOR_PORT", "5432")),
        dbname=os.getenv("PGVECTOR_DB"),
        user=os.getenv("PGVECTOR_USER"),
        password=os.getenv("PGVECTOR_PASSWORD"),
    )


def to_pgvector(embedding: list[float]) -> str:
    """Convert list to pgvector literal."""
    return "[" + ",".join(str(round(v, 8)) for v in embedding) + "]"


# ── Table Init ─────────────────────────────────────────────────────────


def init_tables(conn):
    """Create all embedding tables if they don't exist."""
    dim = EMBEDDING_DIM
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")

        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS schema_embeddings (
                id SERIAL PRIMARY KEY,
                table_name TEXT NOT NULL,
                column_name TEXT,
                description TEXT,
                embedding vector({dim}),
                metadata JSONB,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)

        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS question_embeddings (
                id SERIAL PRIMARY KEY,
                question_text TEXT NOT NULL,
                sql_query TEXT,
                embedding vector({dim}),
                source VARCHAR(100),
                metadata JSONB,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)

        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS doc_embeddings (
                id SERIAL PRIMARY KEY,
                source_file TEXT,
                table_ref TEXT,
                content TEXT NOT NULL,
                embedding vector({dim}),
                doc_type VARCHAR(50),
                metadata JSONB,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)

        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS glossary_embeddings (
                id SERIAL PRIMARY KEY,
                term TEXT NOT NULL,
                definition TEXT,
                sql_fragment TEXT,
                synonyms TEXT[],
                embedding vector({dim}),
                metadata JSONB,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)

    conn.commit()
    logger.info("Tables initialized (dim=%d)", dim)


# ── Embedding ──────────────────────────────────────────────────────────


async def embed_batch(client, deployment: str, texts: list[str]) -> list[list[float]]:
    """Embed a batch of texts with retry."""
    for attempt in range(3):
        try:
            resp = await client.embeddings.create(model=deployment, input=texts)
            return [item.embedding for item in resp.data]
        except Exception as e:
            if attempt < 2:
                wait = 2 ** (attempt + 1)
                logger.warning("Embed retry %d: %s (waiting %ds)", attempt + 1, e, wait)
                await asyncio.sleep(wait)
            else:
                raise


# ── Loaders ────────────────────────────────────────────────────────────


async def load_schema_embeddings(
    client, deployment: str, conn, texts_path: Path, batch_size: int
) -> int:
    """Load schema embedding texts into schema_embeddings table."""
    with open(texts_path) as f:
        items = json.load(f)  # list of {"text": "...", "metadata": {...}}

    # Truncate and re-load
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE schema_embeddings;")
    conn.commit()

    total = 0
    for i in range(0, len(items), batch_size):
        batch_items = items[i : i + batch_size]
        texts = [item["text"][:8000] for item in batch_items]

        embeddings = await embed_batch(client, deployment, texts)

        with conn.cursor() as cur:
            for item, emb in zip(batch_items, embeddings):
                meta = item.get("metadata", {})
                cur.execute(
                    """INSERT INTO schema_embeddings (table_name, description, embedding, metadata)
                    VALUES (%s, %s, %s::vector, %s)""",
                    (
                        meta.get("table_name", ""),
                        item["text"][:8000],
                        to_pgvector(emb),
                        json.dumps(meta),
                    ),
                )
        conn.commit()
        total += len(batch_items)
        logger.info("schema_embeddings: %d/%d", total, len(items))

    return total


async def load_question_embeddings(
    client, deployment: str, conn, texts_path: Path, batch_size: int
) -> int:
    """Load question embedding texts into question_embeddings table."""
    with open(texts_path) as f:
        items = json.load(f)  # list of {"text": "...", "metadata": {...}}

    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE question_embeddings;")
    conn.commit()

    total = 0
    for i in range(0, len(items), batch_size):
        batch_items = items[i : i + batch_size]
        texts = [item["text"][:8000] for item in batch_items]

        embeddings = await embed_batch(client, deployment, texts)

        with conn.cursor() as cur:
            for item, emb in zip(batch_items, embeddings):
                meta = item.get("metadata", {})
                cur.execute(
                    """INSERT INTO question_embeddings (question_text, sql_query, embedding, source, metadata)
                    VALUES (%s, %s, %s::vector, %s, %s)""",
                    (
                        meta.get("question_text", item["text"])[:2000],
                        meta.get("sql_query", "")[:50000],
                        to_pgvector(emb),
                        meta.get("source", "metabase"),
                        json.dumps(meta),
                    ),
                )
        conn.commit()
        total += len(batch_items)
        logger.info("question_embeddings: %d/%d", total, len(items))

    return total


async def load_doc_embeddings(
    client, deployment: str, conn, chunks_path: Path, batch_size: int
) -> int:
    """Load doc chunks into doc_embeddings table."""
    with open(chunks_path) as f:
        chunks = json.load(f)

    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE doc_embeddings;")
    conn.commit()

    total = 0
    for i in range(0, len(chunks), batch_size):
        batch_chunks = chunks[i : i + batch_size]
        texts = [c.get("text", "") for c in batch_chunks]

        embeddings = await embed_batch(client, deployment, texts)

        with conn.cursor() as cur:
            for chunk, emb in zip(batch_chunks, embeddings):
                meta = chunk.get("metadata", {})
                cur.execute(
                    """INSERT INTO doc_embeddings (source_file, content, embedding, doc_type, metadata)
                    VALUES (%s, %s, %s::vector, %s, %s)""",
                    (
                        chunk.get("source", ""),
                        chunk.get("text", "")[:50000],
                        to_pgvector(emb),
                        meta.get("file_type", ""),
                        json.dumps(meta),
                    ),
                )
        conn.commit()
        total += len(batch_chunks)
        logger.info("doc_embeddings: %d/%d", total, len(chunks))

    return total


# ── CLI ────────────────────────────────────────────────────────────────


async def run(args):
    # Load env
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    client, deployment = get_embed_client()
    conn = get_pgvector_conn()

    logger.info("Initializing tables...")
    init_tables(conn)

    results = {}

    # 1. Schema embeddings
    schema_path = Path(args.schema_texts)
    if schema_path.exists():
        logger.info("Loading schema embeddings from %s", schema_path)
        results["schema"] = await load_schema_embeddings(
            client, deployment, conn, schema_path, args.batch_size
        )
    else:
        logger.warning("Schema texts not found: %s", schema_path)

    # 2. Question embeddings
    question_path = Path(args.question_texts)
    if question_path.exists():
        logger.info("Loading question embeddings from %s", question_path)
        results["questions"] = await load_question_embeddings(
            client, deployment, conn, question_path, args.batch_size
        )
    else:
        logger.warning("Question texts not found: %s", question_path)

    # 3. Doc embeddings
    doc_path = Path(args.doc_chunks)
    if doc_path.exists():
        logger.info("Loading doc embeddings from %s", doc_path)
        results["docs"] = await load_doc_embeddings(
            client, deployment, conn, doc_path, args.batch_size
        )
    else:
        logger.warning("Doc chunks not found: %s", doc_path)

    conn.close()
    logger.info("Embedding load complete: %s", results)


def main():
    parser = argparse.ArgumentParser(description="Load all RAVEN embeddings into pgvector")
    parser.add_argument("--schema-texts", default="data/schema_embedding_texts.json")
    parser.add_argument("--question-texts", default="data/question_embedding_texts.json")
    parser.add_argument("--doc-chunks", default="data/doc_chunks.json")
    parser.add_argument("--batch-size", type=int, default=100)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
