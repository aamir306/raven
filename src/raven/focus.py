"""
RAVEN Focus Mode — Context scoping for improved accuracy.
=========================================================
FocusContext dataclass and JSON-file persistence for Focus Documents.
Supports manual documents, auto-generated dashboard focuses, and living-document suggestions.
"""

from __future__ import annotations

import fcntl
import json
import logging
import os
import re
import tempfile
import threading
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# ── Persistence paths ──────────────────────────────────────────────
FOCUS_DIR = Path(__file__).resolve().parents[2] / "data" / "focus_documents"
SUGGESTIONS_FILE = FOCUS_DIR / "_suggestions.json"


# ── Core dataclass ─────────────────────────────────────────────────

@dataclass
class FocusContext:
    """Scoped context attached to a pipeline run."""

    type: str = "document"  # 'document' | 'dashboard' | 'question' | 'collection'
    name: str = ""
    source_id: str = ""     # focus-document UUID or Metabase dashboard/question ID

    # Scoped artefacts
    tables: list[str] = field(default_factory=list)
    glossary_terms: list[dict] = field(default_factory=list)
    verified_queries: list[dict] = field(default_factory=list)
    business_rules: list[dict] = field(default_factory=list)
    column_notes: dict[str, str] = field(default_factory=dict)

    # Metabase-specific (dashboard / question focus)
    dashboard_cards: list[dict] = field(default_factory=list)
    dashboard_filters: list[dict] = field(default_factory=list)

    # Document metadata
    document_kind: str = ""
    domain: str = ""
    owner: str = ""
    trust_level: str = "reference"
    related_metrics: list[str] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)
    deprecated: bool = False

    # Computed stats
    table_count: int = 0
    rule_count: int = 0
    query_count: int = 0

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "FocusContext":
        known = {f.name for f in cls.__dataclass_fields__.values()}
        return cls(**{k: v for k, v in d.items() if k in known})


# ── Focus-Document persistence (JSON files under data/focus_documents/) ─

@dataclass
class FocusDocument:
    """A persisted focus document."""

    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    description: str = ""
    type: str = "manual"  # 'manual' | 'auto_dashboard' | 'uploaded'
    doc_kind: str = "reference"
    domain: str = ""
    owner: str = ""
    trust_level: str = "reference"
    related_metrics: list[str] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)
    source_filename: str = ""
    version: str = ""
    effective_date: str = ""
    deprecated: bool = False

    tables: list[str] = field(default_factory=list)
    glossary_terms: list[dict] = field(default_factory=list)
    verified_queries: list[dict] = field(default_factory=list)
    business_rules: list[dict] = field(default_factory=list)
    column_notes: dict[str, str] = field(default_factory=dict)

    metabase_dashboard_id: int | None = None

    created_by: str = "system"
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    # Computed
    @property
    def table_count(self) -> int:
        return len(self.tables)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["table_count"] = self.table_count
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "FocusDocument":
        known = {f.name for f in cls.__dataclass_fields__.values()}
        return cls(**{k: v for k, v in d.items() if k in known})

    def to_focus_context(self) -> FocusContext:
        """Convert to a FocusContext for pipeline injection."""
        return FocusContext(
            type=self.type if self.type != "manual" else "document",
            name=self.name,
            source_id=self.id,
            tables=self.tables,
            glossary_terms=self.glossary_terms,
            verified_queries=self.verified_queries,
            business_rules=self.business_rules,
            column_notes=self.column_notes,
            document_kind=self.doc_kind,
            domain=self.domain,
            owner=self.owner,
            trust_level=self.trust_level,
            related_metrics=self.related_metrics,
            tags=self.tags,
            deprecated=self.deprecated,
            table_count=self.table_count,
            rule_count=len(self.business_rules),
            query_count=len(self.verified_queries),
        )


class FocusStore:
    """Thread-safe JSON-file backed store for focus documents and suggestions.

    Safety guarantees:
    - In-process mutex via threading.Lock (guards concurrent coroutines / threads)
    - Atomic writes via tempfile + os.replace (no partial-write corruption)
    - File-level advisory lock via fcntl.flock (guards multi-process access)
    """

    def __init__(self, base_dir: Path | None = None):
        self.base_dir = base_dir or FOCUS_DIR
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self._suggestions_file = self.base_dir / "_suggestions.json"
        self._lock = threading.Lock()

    # ── Atomic file helpers ────────────────────────────────────

    @staticmethod
    def _atomic_write(path: Path, data: str) -> None:
        """Write data to a temp file then atomically replace the target."""
        dir_ = path.parent
        fd, tmp = tempfile.mkstemp(dir=str(dir_), suffix=".tmp")
        try:
            os.write(fd, data.encode())
            os.fsync(fd)
            os.close(fd)
            os.replace(tmp, str(path))
        except BaseException:
            os.close(fd) if not os.get_inheritable(fd) else None
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise

    @staticmethod
    def _locked_read(path: Path) -> str:
        """Read file content under a shared (read) advisory lock."""
        with open(path, "r") as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_SH)
            try:
                return f.read()
            finally:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)

    # ── Document CRUD ──────────────────────────────────────────

    def _doc_path(self, doc_id: str) -> Path:
        return self.base_dir / f"{doc_id}.json"

    def list_documents(self) -> list[dict]:
        """Return all focus documents (summary form)."""
        with self._lock:
            docs = []
            for p in sorted(self.base_dir.glob("*.json")):
                if p.name.startswith("_"):
                    continue
                try:
                    data = json.loads(self._locked_read(p))
                    docs.append(data)
                except Exception:
                    logger.warning("Corrupt focus doc: %s", p)
            return docs

    def get_document(self, doc_id: str) -> FocusDocument | None:
        p = self._doc_path(doc_id)
        if not p.exists():
            return None
        with self._lock:
            try:
                return FocusDocument.from_dict(json.loads(self._locked_read(p)))
            except Exception:
                return None

    def create_document(self, doc: FocusDocument) -> FocusDocument:
        with self._lock:
            doc.updated_at = datetime.now(timezone.utc).isoformat()
            self._atomic_write(
                self._doc_path(doc.id),
                json.dumps(doc.to_dict(), indent=2),
            )
            logger.info("Created focus document: %s (%s)", doc.name, doc.id)
            return doc

    def update_document(self, doc_id: str, updates: dict) -> FocusDocument | None:
        with self._lock:
            p = self._doc_path(doc_id)
            if not p.exists():
                return None
            try:
                existing = FocusDocument.from_dict(json.loads(self._locked_read(p)))
            except Exception:
                return None
            for k, v in updates.items():
                if hasattr(existing, k) and k not in ("id", "created_at", "created_by"):
                    setattr(existing, k, v)
            existing.updated_at = datetime.now(timezone.utc).isoformat()
            self._atomic_write(p, json.dumps(existing.to_dict(), indent=2))
            logger.info("Updated focus document: %s", existing.name)
            return existing

    def delete_document(self, doc_id: str) -> bool:
        with self._lock:
            p = self._doc_path(doc_id)
            if p.exists():
                p.unlink()
                logger.info("Deleted focus document: %s", doc_id)
                return True
            return False

    # ── Enhancement suggestions ────────────────────────────────

    def _load_suggestions(self) -> list[dict]:
        if self._suggestions_file.exists():
            try:
                return json.loads(self._locked_read(self._suggestions_file))
            except Exception:
                return []
        return []

    def _save_suggestions(self, suggestions: list[dict]):
        self._atomic_write(self._suggestions_file, json.dumps(suggestions, indent=2))

    def add_suggestion(self, document_id: str, suggestion_type: str,
                       suggestion_data: dict, source_query_id: str | None = None) -> dict:
        with self._lock:
            suggestions = self._load_suggestions()
            entry = {
                "id": len(suggestions) + 1,
                "document_id": document_id,
                "suggestion_type": suggestion_type,
                "suggestion_data": suggestion_data,
                "source_query_id": source_query_id,
                "status": "pending",
                "reviewed_by": None,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            suggestions.append(entry)
            self._save_suggestions(suggestions)
            return entry

    def list_suggestions(self, document_id: str | None = None,
                         status: str | None = None) -> list[dict]:
        with self._lock:
            suggestions = self._load_suggestions()
            if document_id:
                suggestions = [s for s in suggestions if s["document_id"] == document_id]
            if status:
                suggestions = [s for s in suggestions if s["status"] == status]
            return suggestions

    def review_suggestion(self, suggestion_id: int, action: str,
                          reviewer: str = "admin") -> dict | None:
        """Accept or reject a suggestion. If accepted, apply to the document."""
        with self._lock:
            suggestions = self._load_suggestions()
            target = None
            for s in suggestions:
                if s["id"] == suggestion_id:
                    target = s
                    break
            if not target:
                return None

            target["status"] = action  # 'accepted' | 'rejected'
            target["reviewed_by"] = reviewer

            if action == "accepted":
                self._apply_suggestion_locked(target)

            self._save_suggestions(suggestions)
            return target

    def _apply_suggestion_locked(self, suggestion: dict):
        """Apply an accepted suggestion to its parent document.

        MUST be called while self._lock is held.
        """
        p = self._doc_path(suggestion["document_id"])
        if not p.exists():
            return
        try:
            doc = FocusDocument.from_dict(json.loads(self._locked_read(p)))
        except Exception:
            return

        stype = suggestion["suggestion_type"]
        sdata = suggestion["suggestion_data"]

        if stype == "add_table":
            table = sdata.get("table", "")
            if table and table not in doc.tables:
                doc.tables.append(table)
        elif stype == "add_rule":
            doc.business_rules.append(sdata)
        elif stype == "add_query":
            doc.verified_queries.append(sdata)
        elif stype == "add_note":
            col = sdata.get("column", "")
            note = sdata.get("note", "")
            if col:
                doc.column_notes[col] = note

        doc.updated_at = datetime.now(timezone.utc).isoformat()
        self._atomic_write(
            self._doc_path(doc.id),
            json.dumps(doc.to_dict(), indent=2),
        )


# ── Metabase URL parsing ──────────────────────────────────────────

METABASE_URL_PATTERNS = [
    r'(?P<base>https?://[^/\s]+)/dashboard/(?P<id>\d+)',
    r'(?P<base>https?://[^/\s]+)/question/(?P<id>\d+)',
    r'(?P<base>https?://[^/\s]+)/question#(?P<id>\d+)',
    r'(?P<base>https?://[^/\s]+)/collection/(?P<id>\d+)',
]


def parse_metabase_url(text: str) -> dict | None:
    """Detect Metabase URLs in text and return parsed info."""
    for pattern in METABASE_URL_PATTERNS:
        match = re.search(pattern, text)
        if match:
            url_type = (
                "dashboard" if "/dashboard/" in match.group(0)
                else "question" if "/question" in match.group(0)
                else "collection"
            )
            return {
                "base_url": match.group("base"),
                "type": url_type,
                "id": int(match.group("id")),
                "remaining_text": re.sub(pattern, '', text).strip(),
            }
    return None


# ── Enhancement suggestion generator ─────────────────────────────

async def suggest_enhancements(focus: FocusContext | None,
                                tables_used: list[str],
                                probe_evidence: list[dict] | None = None) -> list[dict]:
    """Generate enhancement suggestions after a pipeline run."""
    if not focus or not focus.source_id:
        return []

    suggestions = []

    # 1. Tables outside focus
    outside_tables = set(tables_used) - set(focus.tables)
    for table in outside_tables:
        suggestions.append({
            "type": "add_table",
            "data": {"table": table},
            "reason": f"Table used in response but not in focus '{focus.name}'",
        })

    # 2. Column format info from probes
    if probe_evidence:
        for probe in probe_evidence:
            col = probe.get("column", "")
            fmt = probe.get("discovered_format", probe.get("format", ""))
            if col and fmt and col not in focus.column_notes:
                suggestions.append({
                    "type": "add_note",
                    "data": {"column": col, "note": fmt},
                    "reason": "Discovered during probe execution",
                })

    return suggestions


# ── Domain-Based Focus Mode (OpenMetadata) ─────────────────────────

async def focus_from_domain(domain_name: str, om_client: Any) -> FocusContext | None:
    """
    Build a FocusContext from an OpenMetadata domain.
    Zero-setup Focus Mode — select a domain and get instant scoping.

    Args:
        domain_name: Name of the OM domain (e.g. "Revenue", "Marketing")
        om_client: OpenMetadataMCPClient instance

    Returns:
        FocusContext with tables and glossary terms from the domain,
        or None if OM is unavailable or domain not found.
    """
    if not om_client:
        return None

    try:
        result = await om_client.focus_from_domain(domain_name)
        if not result or not result.get("tables"):
            logger.info("Domain '%s' not found or has no tables in OpenMetadata", domain_name)
            return None

        glossary_terms = [
            {
                "term": g.get("term", ""),
                "definition": g.get("definition", ""),
                "sql_fragment": g.get("sql_fragment", ""),
            }
            for g in result.get("glossary_terms", [])
        ]

        return FocusContext(
            type="domain",
            name=domain_name,
            source_id=f"om_domain_{domain_name}",
            tables=result.get("tables", []),
            glossary_terms=glossary_terms,
            table_count=result.get("table_count", len(result.get("tables", []))),
        )

    except Exception as exc:
        logger.warning("Failed to build focus from OM domain '%s': %s", domain_name, exc)
        return None


async def list_om_domains(om_client: Any) -> list[dict]:
    """
    List available OpenMetadata domains for Focus Mode selection.

    Returns:
        [{"name": "Revenue", "description": "...", "table_count": 42}, ...]
    """
    if not om_client:
        return []
    try:
        domains = await om_client.get_domains()
        return [
            {
                "name": d.get("name", ""),
                "description": d.get("description", ""),
                "fullyQualifiedName": d.get("fullyQualifiedName", ""),
            }
            for d in domains
        ]
    except Exception as exc:
        logger.debug("Failed to list OM domains: %s", exc)
        return []
