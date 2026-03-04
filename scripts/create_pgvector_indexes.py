#!/usr/bin/env python3
"""
Create IVFFlat / HNSW indexes on pgvector embedding tables.
Run this AFTER embeddings are loaded and VPN is connected.

Usage:
    python scripts/create_pgvector_indexes.py
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv
load_dotenv()

import psycopg2


def main():
    conn = psycopg2.connect(
        host=os.getenv("PGVECTOR_HOST"),
        port=int(os.getenv("PGVECTOR_PORT", "5432")),
        dbname=os.getenv("PGVECTOR_DB"),
        user=os.getenv("PGVECTOR_USER"),
        password=os.getenv("PGVECTOR_PASSWORD"),
        connect_timeout=15,
    )
    conn.autocommit = True
    cur = conn.cursor()

    # Check row counts
    for t in ["schema_embeddings", "question_embeddings", "doc_embeddings", "glossary_embeddings"]:
        cur.execute(f"SELECT count(*) FROM {t}")
        print(f"  {t}: {cur.fetchone()[0]} rows")

    # Check existing indexes
    cur.execute("""
        SELECT indexname, tablename FROM pg_indexes
        WHERE tablename IN ('schema_embeddings','question_embeddings','doc_embeddings','glossary_embeddings')
        ORDER BY tablename, indexname
    """)
    existing = cur.fetchall()
    if existing:
        print("\nExisting vector indexes:")
        for idx_name, tbl in existing:
            print(f"  {tbl}: {idx_name}")
        print("\nSkipping — indexes already exist. Drop them first if you need to recreate.")
        conn.close()
        return

    print("\nCreating vector indexes...")

    # Check actual vector dimensions
    cur.execute("SELECT vector_dims(embedding) FROM schema_embeddings LIMIT 1")
    dims = cur.fetchone()
    dims = dims[0] if dims else 0
    print(f"\n  Vector dimensions: {dims}")

    if dims > 2000:
        print(f"\n  ⚠️  pgvector {get_pgvector_version(cur)} has a 2000-dim index limit.")
        print(f"  With {dims}-dim embeddings, sequential scan will be used.")
        print(f"  This is fine for <10K rows. For larger datasets, consider:")
        print(f"    - Upgrading pgvector to ≥0.9.0")
        print(f"    - Using dimensions=2000 in the OpenAI embedding API")
    else:
        print("  Creating HNSW indexes...")
        for tbl in ["schema_embeddings", "question_embeddings", "doc_embeddings"]:
            idx = f"idx_{tbl}_hnsw"
            print(f"    {tbl}...", end=" ", flush=True)
            cur.execute(f"""
                CREATE INDEX {idx}
                ON {tbl} USING hnsw (embedding vector_cosine_ops)
                WITH (m = 16, ef_construction = 64)
            """)
            print("done")
        print("\n  ✅ All indexes created.")

    conn.close()


def get_pgvector_version(cur):
    cur.execute("SELECT extversion FROM pg_extension WHERE extname = 'vector'")
    row = cur.fetchone()
    return row[0] if row else 'unknown'


if __name__ == "__main__":
    main()
