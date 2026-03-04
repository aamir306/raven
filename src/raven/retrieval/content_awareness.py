"""
Content Awareness — Stage 2.6
==============================
Looks up column-level metadata (data-type, null-%, distinct count,
format patterns, sample values) from the content_awareness.json
artifact built during preprocessing.

This metadata is injected into the column-pruning and validation
prompts so the LLM knows about data quirks (e.g., "status stores
ENUM strings, not integers" or "85 % NULL in email column").
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_ARTIFACT = (
    Path(__file__).resolve().parents[3] / "data" / "content_awareness.json"
)


class ContentAwareness:
    """Provide column-level data-awareness metadata."""

    def __init__(self, artifact_path: str | Path | None = None):
        """
        Args:
            artifact_path: Path to content_awareness.json.
                           Falls back to data/content_awareness.json.
        """
        self._data: dict[str, dict] = {}
        path = Path(artifact_path) if artifact_path else DEFAULT_ARTIFACT
        self._load(path)

    # ── Public API ──────────────────────────────────────────────────────

    async def lookup(self, entity_matches: list[dict]) -> list[dict]:
        """
        Retrieve Content Awareness metadata for a list of entity matches.

        Args:
            entity_matches: Output of LSHMatcher.match() or similar —
                each dict must have 'table' and 'column' keys.

        Returns:
            [
                {
                    "table": "gold.finance.orders",
                    "column": "status",
                    "data_type": "varchar",
                    "format_pattern": "ENUM(active, cancelled, pending)",
                    "distinct_count": 3,
                    "null_pct": 0.2,
                    "sample_values": ["active", "cancelled", "pending"],
                    "notes": "Use VARCHAR comparison, not integer codes.",
                },
                ...
            ]
        """
        awareness: list[dict] = []
        for match in entity_matches:
            table = match.get("table", "")
            column = match.get("column", "")
            key = f"{table}.{column}"

            entry = self._data.get(key, {})
            awareness.append({
                "table": table,
                "column": column,
                "data_type": entry.get("data_type", match.get("data_type", "")),
                "format_pattern": entry.get("format_pattern", match.get("format_pattern", "")),
                "distinct_count": entry.get("distinct_count", match.get("distinct_count")),
                "null_pct": entry.get("null_pct", match.get("null_pct")),
                "sample_values": entry.get("sample_values", []),
                "notes": entry.get("notes", match.get("notes", "")),
            })

        logger.debug(
            "Content awareness: %d columns enriched out of %d matches",
            sum(1 for a in awareness if a.get("data_type")),
            len(entity_matches),
        )
        return awareness

    def get(self, table: str, column: str) -> dict:
        """Direct lookup for a single table.column."""
        return self._data.get(f"{table}.{column}", {})

    # ── Internal ────────────────────────────────────────────────────────

    def _load(self, path: Path) -> None:
        """Load content_awareness.json into memory."""
        if not path.exists():
            logger.info("Content awareness artifact not found at %s — starting empty", path)
            return

        try:
            raw = json.loads(path.read_text())
            # Expected format: { "catalog.schema.table.column": { ... }, ... }
            # or { "tables": { "catalog.schema.table": { "columns": { "col": {...} } } } }
            if isinstance(raw, dict):
                if "tables" in raw:
                    for tbl, tbl_info in raw["tables"].items():
                        for col, col_info in tbl_info.get("columns", {}).items():
                            self._data[f"{tbl}.{col}"] = col_info
                else:
                    self._data = raw

            logger.info(
                "Loaded content awareness: %d column entries", len(self._data),
            )
        except Exception as exc:
            logger.warning("Failed to load content awareness from %s: %s", path, exc)

    def reload(self, path: str | Path | None = None) -> None:
        """Reload the artifact (e.g., after preprocessing refresh)."""
        self._data.clear()
        self._load(Path(path) if path else DEFAULT_ARTIFACT)
