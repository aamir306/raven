"""
pgvector wrapper for RAVEN embedding storage and similarity search.

Manages four tables:
- schema_embeddings   — table/column descriptions from dbt
- question_embeddings — Metabase Q-SQL pairs for few-shot retrieval
- glossary_embeddings — semantic model / business glossary terms
- doc_embeddings      — documentation chunks (Word/PDF/Markdown/OpenMetadata)

Plus operational tables:
- query_log           — stores every pipeline query for feedback loop

All embeddings use text-embedding-3-large (3072-dim).
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import numpy as np
import psycopg2
import psycopg2.extras
import psycopg2.pool
import structlog

logger = structlog.get_logger(__name__)

EMBEDDING_DIM = 3072

# Table definitions: (table_name, extra_columns_sql)
_TABLES: dict[str, str] = {
    "schema_embeddings": """
        id SERIAL PRIMARY KEY,
        table_name TEXT NOT NULL,
        column_name TEXT,
        description TEXT,
        embedding vector({dim}),
        metadata JSONB,
        created_at TIMESTAMPTZ DEFAULT NOW()
    """,
    "question_embeddings": """
        id SERIAL PRIMARY KEY,
        question_text TEXT NOT NULL,
        sql_query TEXT,
        embedding vector({dim}),
        source VARCHAR(100),
        metadata JSONB,
        created_at TIMESTAMPTZ DEFAULT NOW()
    """,
    "glossary_embeddings": """
        id SERIAL PRIMARY KEY,
        term TEXT NOT NULL,
        definition TEXT,
        sql_fragment TEXT,
        synonyms TEXT[],
        embedding vector({dim}),
        metadata JSONB,
        created_at TIMESTAMPTZ DEFAULT NOW()
    """,
    "doc_embeddings": """
        id SERIAL PRIMARY KEY,
        source_file TEXT,
        table_ref TEXT,
        content TEXT NOT NULL,
        embedding vector({dim}),
        doc_type VARCHAR(50),
        metadata JSONB,
        created_at TIMESTAMPTZ DEFAULT NOW()
    """,
    "query_log": """
        id SERIAL PRIMARY KEY,
        query_id VARCHAR(36) NOT NULL UNIQUE,
        question TEXT NOT NULL,
        sql_text TEXT,
        difficulty VARCHAR(20),
        confidence VARCHAR(20),
        row_count INTEGER DEFAULT 0,
        conversation_id VARCHAR(36),
        feedback VARCHAR(20),
        correction_sql TEXT,
        correction_notes TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        feedback_at TIMESTAMPTZ
    """,
}


class PgVectorStore:
    """pgvector CRUD client with connection pooling and cosine similarity search."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 5433,
        dbname: str = "raven",
        user: str = "raven",
        password: str = "changeme",
        min_connections: int = 2,
        max_connections: int = 10,
    ) -> None:
        self._pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=min_connections,
            maxconn=max_connections,
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password,
        )
        logger.info("pgvector_pool_created", host=host, port=port, dbname=dbname)

    # ------------------------------------------------------------------ #
    # Initialization
    # ------------------------------------------------------------------ #

    def init_tables(self) -> None:
        """Create the pgvector extension and all embedding tables if they don't exist."""
        conn = self._pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
                for table_name, columns_sql in _TABLES.items():
                    ddl = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_sql.format(dim=EMBEDDING_DIM)});"
                    cur.execute(ddl)
                    # Vector index — pgvector 0.8.0 limits indexes to ≤2000 dims
                    # For 3072-dim (text-embedding-3-large), skip index creation;
                    # sequential scan is fast enough for <10K rows.
                    if "embedding" in columns_sql and EMBEDDING_DIM <= 2000:
                        idx_name = f"idx_{table_name}_embedding"
                        cur.execute(f"""
                            CREATE INDEX IF NOT EXISTS {idx_name}
                            ON {table_name}
                            USING hnsw (embedding vector_cosine_ops)
                            WITH (m = 16, ef_construction = 64);
                        """)
                    elif "embedding" in columns_sql:
                        logger.info("pgvector_skip_index", table=table_name,
                                    reason=f"dim={EMBEDDING_DIM} exceeds pgvector index limit (2000)")
                # Non-vector indexes for query_log
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_query_log_query_id
                    ON query_log (query_id);
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_query_log_conversation
                    ON query_log (conversation_id)
                    WHERE conversation_id IS NOT NULL;
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_query_log_feedback
                    ON query_log (feedback)
                    WHERE feedback IS NOT NULL;
                """)
            conn.commit()
            logger.info("pgvector_tables_initialized", tables=list(_TABLES.keys()))
        finally:
            self._pool.putconn(conn)

    # ------------------------------------------------------------------ #
    # Insert
    # ------------------------------------------------------------------ #

    def insert(
        self,
        table: str,
        text: str,
        embedding: list[float],
        metadata: dict[str, Any] | None = None,
        **extra_columns: Any,
    ) -> int:
        """Insert a single embedding row and return its id."""
        self._validate_table(table)
        cols = list(extra_columns.keys()) + ["embedding"]
        vals = list(extra_columns.values()) + [self._to_pgvector(embedding)]
        if metadata:
            cols.append("metadata")
            vals.append(json.dumps(metadata))

        placeholders = ", ".join(["%s"] * len(vals))
        col_names = ", ".join(cols)
        sql = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders}) RETURNING id;"

        conn = self._pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, vals)
                row_id = cur.fetchone()[0]
            conn.commit()
            return row_id
        finally:
            self._pool.putconn(conn)

    def batch_insert(
        self,
        table: str,
        items: list[dict[str, Any]],
    ) -> int:
        """Batch insert multiple rows.  Each item dict must include an ``embedding`` key.

        Returns the number of rows inserted.
        """
        self._validate_table(table)
        if not items:
            return 0

        conn = self._pool.getconn()
        try:
            with conn.cursor() as cur:
                for item in items:
                    emb = item.pop("embedding")
                    metadata = item.pop("metadata", None)
                    cols = list(item.keys()) + ["embedding"]
                    vals = list(item.values()) + [self._to_pgvector(emb)]
                    if metadata:
                        cols.append("metadata")
                        vals.append(json.dumps(metadata))
                    placeholders = ", ".join(["%s"] * len(vals))
                    col_names = ", ".join(cols)
                    cur.execute(
                        f"INSERT INTO {table} ({col_names}) VALUES ({placeholders});",
                        vals,
                    )
            conn.commit()
            logger.info("pgvector_batch_insert", table=table, count=len(items))
            return len(items)
        finally:
            self._pool.putconn(conn)

    # ------------------------------------------------------------------ #
    # Search
    # ------------------------------------------------------------------ #

    def search(
        self,
        table_name: str | None = None,
        query_embedding: list[float] | None = None,
        top_k: int = 5,
        filter_sql: str | None = None,
        metadata_filter: dict[str, Any] | None = None,
        *,
        table: str | None = None,  # backward-compat alias
    ) -> list[dict[str, Any]]:
        """Cosine similarity search.  Returns top_k results ordered by similarity (descending).

        Parameters
        ----------
        table_name:
            One of the four embedding tables.
        query_embedding:
            The query vector (3072-dim for text-embedding-3-large).
        top_k:
            Number of results to return.
        filter_sql:
            Optional SQL WHERE clause fragment, e.g. ``"source = 'metabase'"``.
        metadata_filter:
            Optional dict of metadata key-value pairs to filter on (uses JSONB @> operator).
        table:
            Alias for table_name (backward compatibility).
        """
        tbl = table_name or table
        if not tbl:
            raise ValueError("Either table_name or table must be provided")
        self._validate_table(tbl)
        emb_str = self._to_pgvector(query_embedding)

        # Build WHERE clause
        where_parts: list[str] = []
        if filter_sql:
            where_parts.append(filter_sql)
        if metadata_filter:
            where_parts.append(f"metadata @> '{json.dumps(metadata_filter)}'::jsonb")
        where = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

        sql = f"""
            SELECT *, 1 - (embedding <=> %s) AS similarity
            FROM {tbl}
            {where}
            ORDER BY embedding <=> %s
            LIMIT %s;
        """

        conn = self._pool.getconn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, [emb_str, emb_str, top_k])
                rows = cur.fetchall()
            return [dict(r) for r in rows]
        finally:
            self._pool.putconn(conn)

    # ------------------------------------------------------------------ #
    # Delete
    # ------------------------------------------------------------------ #

    def delete_by_source(self, table: str | None = None, source: str = "", *, table_name: str | None = None) -> int:
        """Delete all rows where source/source_file matches — useful for re-indexing."""
        tbl = table or table_name
        if not tbl:
            raise ValueError("Either table or table_name must be provided")
        self._validate_table(tbl)
        col = "source_file" if tbl == "doc_embeddings" else "source"
        sql = f"DELETE FROM {tbl} WHERE {col} = %s;"
        conn = self._pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, [source])
                count = cur.rowcount
            conn.commit()
            logger.info("pgvector_delete_by_source", table=table, source=source, deleted=count)
            return count
        finally:
            self._pool.putconn(conn)

    def truncate(self, table: str | None = None, *, table_name: str | None = None) -> None:
        """Truncate an entire table (for full re-index)."""
        tbl = table or table_name
        if not tbl:
            raise ValueError("Either table or table_name must be provided")
        self._validate_table(tbl)
        conn = self._pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(f"TRUNCATE TABLE {tbl};")
            conn.commit()
            logger.info("pgvector_truncated", table=tbl)
        finally:
            self._pool.putconn(conn)

    # ------------------------------------------------------------------ #
    # Async Search (for true parallelism with asyncio.gather)
    # ------------------------------------------------------------------ #

    async def async_search(
        self,
        table_name: str,
        query_embedding: list[float],
        top_k: int = 5,
        filter_sql: str | None = None,
        metadata_filter: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Thread-pool wrapper around synchronous search for true async parallelism.

        Use this instead of search() when calling multiple searches via asyncio.gather.
        """
        return await asyncio.to_thread(
            self.search,
            table_name=table_name,
            query_embedding=query_embedding,
            top_k=top_k,
            filter_sql=filter_sql,
            metadata_filter=metadata_filter,
        )

    # ------------------------------------------------------------------ #
    # Query Log (feedback persistence)
    # ------------------------------------------------------------------ #

    def log_query(
        self,
        query_id: str,
        question: str,
        sql_text: str | None = None,
        difficulty: str = "unknown",
        confidence: str = "LOW",
        row_count: int = 0,
        conversation_id: str | None = None,
    ) -> None:
        """Insert a pipeline query into the query_log table."""
        sql = """
            INSERT INTO query_log (query_id, question, sql_text, difficulty,
                                   confidence, row_count, conversation_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (query_id) DO NOTHING;
        """
        conn = self._pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, [
                    query_id, question, sql_text, difficulty,
                    confidence, row_count, conversation_id,
                ])
            conn.commit()
        finally:
            self._pool.putconn(conn)

    def update_feedback(
        self,
        query_id: str,
        feedback: str,
        correction_sql: str | None = None,
        correction_notes: str | None = None,
    ) -> bool:
        """Set feedback status on a logged query. Returns True if row was updated."""
        sql = """
            UPDATE query_log
            SET feedback = %s, correction_sql = %s, correction_notes = %s,
                feedback_at = NOW()
            WHERE query_id = %s;
        """
        conn = self._pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, [feedback, correction_sql, correction_notes, query_id])
                updated = cur.rowcount > 0
            conn.commit()
            return updated
        finally:
            self._pool.putconn(conn)

    def get_query(self, query_id: str) -> dict | None:
        """Retrieve a single query log entry by its query_id."""
        sql = "SELECT * FROM query_log WHERE query_id = %s;"
        conn = self._pool.getconn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, [query_id])
                row = cur.fetchone()
            return dict(row) if row else None
        finally:
            self._pool.putconn(conn)

    def get_conversation_history(
        self,
        conversation_id: str,
        limit: int = 10,
    ) -> list[dict]:
        """Retrieve recent queries for a conversation, ordered chronologically."""
        sql = """
            SELECT query_id, question, sql_text, difficulty, confidence, row_count,
                   created_at
            FROM query_log
            WHERE conversation_id = %s
            ORDER BY created_at DESC
            LIMIT %s;
        """
        conn = self._pool.getconn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, [conversation_id, limit])
                rows = cur.fetchall()
            return [dict(r) for r in reversed(rows)]  # chronological order
        finally:
            self._pool.putconn(conn)

    def get_pending_corrections(self, limit: int = 50) -> list[dict]:
        """Get queries flagged with thumbs_down that need review."""
        sql = """
            SELECT * FROM query_log
            WHERE feedback = 'thumbs_down'
            ORDER BY feedback_at DESC
            LIMIT %s;
        """
        conn = self._pool.getconn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, [limit])
                rows = cur.fetchall()
            return [dict(r) for r in rows]
        finally:
            self._pool.putconn(conn)

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #

    @staticmethod
    def _validate_table(table: str) -> None:
        if table not in _TABLES:
            raise ValueError(f"Unknown table '{table}'.  Must be one of {list(_TABLES.keys())}")

    @staticmethod
    def _to_pgvector(embedding: list[float]) -> str:
        """Convert a Python list to pgvector literal ``[0.1,0.2,…]``."""
        return "[" + ",".join(str(round(v, 8)) for v in embedding) + "]"

    def close(self) -> None:
        """Close the connection pool."""
        self._pool.closeall()
        logger.info("pgvector_pool_closed")
