"""
Preprocessing: Documentation Ingester
========================================
Processes various documentation sources and embeds them into pgvector
for knowledge-base retrieval:

  1. Markdown files (*.md)
  2. Word documents (*.docx)
  3. PDF files (*.pdf)
  4. OpenMetadata JSON exports
  5. Table annotations YAML (warnings, notes, owner info)

Chunking strategy: section-based for Markdown, paragraph-based otherwise.
Max chunk size: ~1000 tokens.

Output: pgvector doc_embeddings table + data/doc_chunks.json (file fallback)

Usage:
    python -m preprocessing.ingest_documentation \
        --docs-dir docs/ \
        --annotations config/table_annotations.yaml \
        --pgvector-dsn postgresql://raven:raven@localhost:5432/raven \
        --output data/doc_chunks.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import sys
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# ── Chunking Config ────────────────────────────────────────────────────

MAX_CHUNK_TOKENS = 1000
OVERLAP_TOKENS = 100
APPROX_CHARS_PER_TOKEN = 4


# ── Chunk Data Structure ──────────────────────────────────────────────


def make_chunk(
    text: str,
    source: str,
    section: str = "",
    metadata: dict | None = None,
) -> dict:
    """Create a standardized chunk dict."""
    return {
        "text": text.strip(),
        "source": source,
        "section": section,
        "metadata": metadata or {},
        "hash": hashlib.sha256(text.strip().encode()).hexdigest()[:16],
        "char_count": len(text.strip()),
    }


# ── Markdown Processor ────────────────────────────────────────────────


def chunk_markdown(file_path: Path) -> list[dict]:
    """Split Markdown file by headers into chunks."""
    content = file_path.read_text(encoding="utf-8", errors="replace")
    chunks = []

    # Split by ## or # headers
    sections = re.split(r"(?=^#{1,3}\s)", content, flags=re.MULTILINE)

    for section in sections:
        section = section.strip()
        if not section:
            continue

        # Extract header
        header_match = re.match(r"^(#{1,3})\s+(.+?)$", section, re.MULTILINE)
        header = header_match.group(2).strip() if header_match else ""

        # If section is too long, split into sub-chunks
        sub_chunks = _split_by_token_limit(section)
        for i, sub in enumerate(sub_chunks):
            suffix = f" (part {i + 1})" if len(sub_chunks) > 1 else ""
            chunks.append(make_chunk(
                text=sub,
                source=str(file_path),
                section=f"{header}{suffix}",
                metadata={"file_type": "markdown", "header_level": len(header_match.group(1)) if header_match else 0},
            ))

    return chunks


# ── Plain Text / .doc Processor ───────────────────────────────────────


def chunk_text(file_path: Path) -> list[dict]:
    """Split plain text file by numbered sections or paragraph blocks."""
    content = file_path.read_text(encoding="utf-8", errors="replace")
    chunks = []

    # Try to split by numbered sections (e.g. "1. Objective", "2. Data Sources")
    sections = re.split(r"(?=^\d+\.\s)", content, flags=re.MULTILINE)

    for section in sections:
        section = section.strip()
        if not section:
            continue

        # Extract section header
        header_match = re.match(r"^(\d+)\.\s+(.+?)$", section, re.MULTILINE)
        header = header_match.group(2).strip() if header_match else ""

        sub_chunks = _split_by_token_limit(section)
        for i, sub in enumerate(sub_chunks):
            suffix = f" (part {i + 1})" if len(sub_chunks) > 1 else ""
            chunks.append(make_chunk(
                text=sub,
                source=str(file_path),
                section=f"{header}{suffix}",
                metadata={"file_type": "text"},
            ))

    return chunks


# ── Word Document Processor ───────────────────────────────────────────


def chunk_docx(file_path: Path) -> list[dict]:
    """Extract and chunk Word document paragraphs."""
    try:
        from docx import Document
    except ImportError:
        logger.warning("python-docx not installed — skipping %s", file_path)
        return []

    doc = Document(str(file_path))
    chunks = []
    current_text = ""
    current_heading = ""

    for para in doc.paragraphs:
        text = para.text.strip()
        if not text:
            continue

        # Check if it's a heading
        if para.style.name.startswith("Heading"):
            # Flush current chunk
            if current_text:
                for sub in _split_by_token_limit(current_text):
                    chunks.append(make_chunk(
                        text=sub,
                        source=str(file_path),
                        section=current_heading,
                        metadata={"file_type": "docx"},
                    ))
            current_text = ""
            current_heading = text
        else:
            current_text += "\n" + text

    # Flush final chunk
    if current_text:
        for sub in _split_by_token_limit(current_text):
            chunks.append(make_chunk(
                text=sub,
                source=str(file_path),
                section=current_heading,
                metadata={"file_type": "docx"},
            ))

    return chunks


# ── PDF Processor ─────────────────────────────────────────────────────


def chunk_pdf(file_path: Path) -> list[dict]:
    """Extract and chunk PDF text by pages."""
    try:
        import fitz  # PyMuPDF
    except ImportError:
        logger.warning("PyMuPDF not installed — skipping %s", file_path)
        return []

    doc = fitz.open(str(file_path))
    chunks = []

    for page_num, page in enumerate(doc, 1):
        text = page.get_text().strip()
        if not text:
            continue

        for sub in _split_by_token_limit(text):
            chunks.append(make_chunk(
                text=sub,
                source=str(file_path),
                section=f"Page {page_num}",
                metadata={"file_type": "pdf", "page": page_num},
            ))

    return chunks


# ── OpenMetadata Export Processor ─────────────────────────────────────


def chunk_openmetadata(file_path: Path) -> list[dict]:
    """Parse OpenMetadata JSON export into chunks."""
    with open(file_path) as f:
        data = json.load(f)

    chunks = []

    # Handle various OM export formats
    entities = data if isinstance(data, list) else data.get("data", [data])

    for entity in entities:
        name = entity.get("name", entity.get("displayName", ""))
        desc = entity.get("description", "")
        entity_type = entity.get("serviceType", entity.get("entityType", ""))

        if not desc:
            continue

        text = f"{entity_type} '{name}': {desc}"

        # Include column descriptions
        columns = entity.get("columns", [])
        for col in columns:
            col_name = col.get("name", "")
            col_desc = col.get("description", "")
            if col_desc:
                text += f"\n  - {col_name}: {col_desc}"

        # Include tags
        tags = entity.get("tags", [])
        if tags:
            tag_names = [t.get("tagFQN", t.get("name", "")) for t in tags]
            text += f"\n  Tags: {', '.join(tag_names)}"

        for sub in _split_by_token_limit(text):
            chunks.append(make_chunk(
                text=sub,
                source=str(file_path),
                section=name,
                metadata={
                    "file_type": "openmetadata",
                    "entity_type": entity_type,
                    "entity_name": name,
                },
            ))

    return chunks


# ── Table Annotations Processor ───────────────────────────────────────


def chunk_annotations(file_path: Path) -> list[dict]:
    """Parse table_annotations.yaml into chunks."""
    import yaml

    with open(file_path) as f:
        data = yaml.safe_load(f) or {}

    chunks = []
    tables = data.get("tables", data) if isinstance(data, dict) else []
    if isinstance(tables, dict):
        tables = [{"name": k, **v} for k, v in tables.items()]

    for table in tables:
        name = table.get("name", "")
        if not name:
            continue

        parts = [f"Table annotation for '{name}':"]

        if table.get("warning"):
            parts.append(f"  WARNING: {table['warning']}")
        if table.get("notes"):
            parts.append(f"  Notes: {table['notes']}")
        if table.get("owner"):
            parts.append(f"  Owner: {table['owner']}")
        if table.get("refresh_schedule"):
            parts.append(f"  Refresh: {table['refresh_schedule']}")
        if table.get("known_issues"):
            for issue in table["known_issues"]:
                parts.append(f"  Known issue: {issue}")
        if table.get("tips"):
            for tip in table["tips"]:
                parts.append(f"  Tip: {tip}")

        text = "\n".join(parts)
        chunks.append(make_chunk(
            text=text,
            source=str(file_path),
            section=name,
            metadata={
                "file_type": "annotation",
                "table_name": name,
                "has_warning": bool(table.get("warning")),
            },
        ))

    return chunks


# ── Chunking Helpers ──────────────────────────────────────────────────


def _split_by_token_limit(text: str) -> list[str]:
    """Split text into chunks respecting token limit."""
    max_chars = MAX_CHUNK_TOKENS * APPROX_CHARS_PER_TOKEN
    overlap_chars = OVERLAP_TOKENS * APPROX_CHARS_PER_TOKEN

    if len(text) <= max_chars:
        return [text]

    chunks = []
    start = 0
    while start < len(text):
        end = start + max_chars

        # Try to break at paragraph or sentence boundary
        if end < len(text):
            # Look for paragraph break
            para_break = text.rfind("\n\n", start, end)
            if para_break > start + max_chars // 2:
                end = para_break + 2
            else:
                # Look for sentence break
                sent_break = text.rfind(". ", start, end)
                if sent_break > start + max_chars // 2:
                    end = sent_break + 2

        chunks.append(text[start:end].strip())
        start = end - overlap_chars  # Overlap

    return [c for c in chunks if c]


# ── Master Ingestion ──────────────────────────────────────────────────


def ingest_all(
    docs_dir: Path | None = None,
    annotations_path: Path | None = None,
    extra_files: list[Path] | None = None,
) -> list[dict]:
    """
    Ingest all documentation sources and return unified chunk list.
    """
    all_chunks: list[dict] = []
    seen_hashes: set[str] = set()

    processors = {
        ".md": chunk_markdown,
        ".docx": chunk_docx,
        ".doc": chunk_text,
        ".txt": chunk_text,
        ".pdf": chunk_pdf,
        ".json": chunk_openmetadata,
    }

    # Process docs directory
    if docs_dir and docs_dir.exists():
        for ext, processor in processors.items():
            for file_path in sorted(docs_dir.rglob(f"*{ext}")):
                try:
                    chunks = processor(file_path)
                    for c in chunks:
                        if c["hash"] not in seen_hashes:
                            all_chunks.append(c)
                            seen_hashes.add(c["hash"])
                    logger.info("Processed %s: %d chunks", file_path.name, len(chunks))
                except Exception as e:
                    logger.error("Failed to process %s: %s", file_path, e)

    # Process annotations
    if annotations_path and annotations_path.exists():
        try:
            chunks = chunk_annotations(annotations_path)
            for c in chunks:
                if c["hash"] not in seen_hashes:
                    all_chunks.append(c)
                    seen_hashes.add(c["hash"])
            logger.info("Processed annotations: %d chunks", len(chunks))
        except Exception as e:
            logger.error("Failed to process annotations: %s", e)

    # Process extra files
    for fp in extra_files or []:
        if not fp.exists():
            continue
        ext = fp.suffix.lower()
        processor = processors.get(ext)
        if processor:
            try:
                chunks = processor(fp)
                for c in chunks:
                    if c["hash"] not in seen_hashes:
                        all_chunks.append(c)
                        seen_hashes.add(c["hash"])
            except Exception as e:
                logger.error("Failed to process %s: %s", fp, e)

    logger.info("Total documentation chunks: %d", len(all_chunks))
    return all_chunks


# ── Embedding & Storage ───────────────────────────────────────────────


async def embed_and_store(
    chunks: list[dict],
    pgvector_dsn: str,
    openai_api_key: str | None = None,
    batch_size: int = 100,
) -> int:
    """Embed doc chunks and upsert into pgvector."""
    import openai
    import asyncpg

    client = openai.AsyncOpenAI(api_key=openai_api_key)

    conn = await asyncpg.connect(pgvector_dsn)
    try:
        await conn.execute("""
            CREATE EXTENSION IF NOT EXISTS vector;
            CREATE TABLE IF NOT EXISTS doc_embeddings (
                id SERIAL PRIMARY KEY,
                source VARCHAR(500) NOT NULL,
                section VARCHAR(500),
                text TEXT NOT NULL,
                embedding vector(1536),
                metadata JSONB DEFAULT '{}',
                text_hash VARCHAR(16) UNIQUE,
                created_at TIMESTAMP DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_doc_embedding
                ON doc_embeddings USING ivfflat (embedding vector_cosine_ops)
                WITH (lists = 50);
        """)

        stored = 0
        for i in range(0, len(chunks), batch_size):
            batch = chunks[i : i + batch_size]
            texts = [c["text"] for c in batch]

            resp = await client.embeddings.create(
                model="text-embedding-3-small",
                input=texts,
            )
            embeddings = [item.embedding for item in resp.data]

            for chunk, emb in zip(batch, embeddings):
                await conn.execute(
                    """
                    INSERT INTO doc_embeddings (source, section, text, embedding, metadata, text_hash)
                    VALUES ($1, $2, $3, $4::vector, $5, $6)
                    ON CONFLICT (text_hash) DO UPDATE SET
                        embedding = EXCLUDED.embedding,
                        metadata = EXCLUDED.metadata,
                        created_at = NOW()
                    """,
                    chunk["source"],
                    chunk.get("section", ""),
                    chunk["text"],
                    str(emb),
                    json.dumps(chunk.get("metadata", {})),
                    chunk["hash"],
                )
                stored += 1

            logger.info("Embedded doc batch %d-%d / %d", i, i + len(batch), len(chunks))

        return stored
    finally:
        await conn.close()


# ── File Fallback ─────────────────────────────────────────────────────


def save_chunks(chunks: list[dict], output_path: Path) -> None:
    """Save chunks to JSON for file-based retrieval."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(chunks, f, indent=2)
    logger.info("Saved %d doc chunks to %s", len(chunks), output_path)


# ── CLI ────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="Ingest documentation for RAVEN")
    parser.add_argument("--docs-dir", default="docs/")
    parser.add_argument("--annotations", default="config/table_annotations.yaml")
    parser.add_argument("--pgvector-dsn", help="PostgreSQL DSN (optional)")
    parser.add_argument("--output", default="data/doc_chunks.json")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    docs_dir = Path(args.docs_dir)
    annotations_path = Path(args.annotations)

    chunks = ingest_all(
        docs_dir=docs_dir if docs_dir.exists() else None,
        annotations_path=annotations_path if annotations_path.exists() else None,
    )

    # Always save to file
    save_chunks(chunks, Path(args.output))

    # Optionally embed and store
    if args.pgvector_dsn:
        import asyncio
        import os

        stored = asyncio.run(
            embed_and_store(
                chunks,
                pgvector_dsn=args.pgvector_dsn,
                openai_api_key=os.getenv("OPENAI_API_KEY"),
            )
        )
        logger.info("Stored %d embeddings in pgvector", stored)
    else:
        logger.info("No pgvector DSN — skipped embedding.")

    logger.info("Documentation ingestion complete!")


if __name__ == "__main__":
    main()
