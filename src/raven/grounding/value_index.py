"""
Value Index — Entity Disambiguation
=====================================
Maintains a pre-built reverse index from normalised string values to
their (table, column) locations in the data warehouse.  Used by the
``ValueResolver`` to ground entity mentions in the user question to
exact filter predicates.

Index format (JSON):
    {
        "active": [
            {"table": "cdp.ops.batches", "column": "status", "count": 4200},
            {"table": "cdp.crm.users", "column": "status", "count": 180000}
        ],
        "physics": [
            {"table": "cdp.lms.courses", "column": "subject", "count": 32},
            {"table": "cdp.lms.enrollments", "column": "subject_name", "count": 9800}
        ]
    }

The index is built during preprocessing (``preprocessing/build_value_index.py``)
by scanning Trino column metadata, sample values, and semantic enum definitions.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_INDEX_PATH = Path(__file__).resolve().parents[3] / "data" / "value_index.json"


class ValueLocation:
    """A single (table, column) where a value was found."""

    __slots__ = ("table", "column", "count", "source")

    def __init__(
        self,
        table: str,
        column: str,
        count: int = 0,
        source: str = "index",
    ):
        self.table = table
        self.column = column
        self.count = count
        self.source = source

    def to_dict(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "column": self.column,
            "count": self.count,
            "source": self.source,
        }


class ValueIndex:
    """
    Reverse lookup from normalised values to (table, column) locations.

    Supports:
      - Exact match lookup
      - Prefix / substring search (for partial entity mentions)
      - Scope narrowing by preferred tables
    """

    def __init__(self, index_path: str | Path | None = None):
        self._index: dict[str, list[ValueLocation]] = {}
        self._loaded = False
        self._path = Path(index_path) if index_path else DEFAULT_INDEX_PATH
        self._load()

    @property
    def loaded(self) -> bool:
        return self._loaded

    @property
    def size(self) -> int:
        return len(self._index)

    # ── Lookup API ─────────────────────────────────────────────────────

    def lookup(
        self,
        value: str,
        preferred_tables: list[str] | None = None,
        max_results: int = 10,
    ) -> list[ValueLocation]:
        """
        Find (table, column) locations for an exact normalised value.

        Preferred tables appear first and are given higher priority.
        """
        key = self._normalise(value)
        locations = self._index.get(key, [])
        if not locations:
            return []

        if preferred_tables:
            preferred_set = {t.lower() for t in preferred_tables}
            locations = sorted(
                locations,
                key=lambda loc: (loc.table.lower() not in preferred_set, -loc.count),
            )
        else:
            locations = sorted(locations, key=lambda loc: -loc.count)

        return locations[:max_results]

    def search(
        self,
        query: str,
        preferred_tables: list[str] | None = None,
        max_results: int = 10,
    ) -> list[tuple[str, list[ValueLocation]]]:
        """
        Substring search across the index.

        Returns list of (matched_value, locations) tuples.
        """
        query_norm = self._normalise(query)
        if not query_norm or len(query_norm) < 2:
            return []

        matches: list[tuple[str, list[ValueLocation]]] = []
        for key, locations in self._index.items():
            if query_norm in key:
                matches.append((key, locations))

        # Sort by relevance: exact matches first, then by total count
        matches.sort(key=lambda item: (item[0] != query_norm, -sum(loc.count for loc in item[1])))

        if preferred_tables:
            preferred_set = {t.lower() for t in preferred_tables}
            for i, (val, locs) in enumerate(matches):
                matches[i] = (
                    val,
                    sorted(locs, key=lambda loc: (loc.table.lower() not in preferred_set, -loc.count)),
                )

        return matches[:max_results]

    def is_ambiguous(
        self,
        value: str,
        preferred_tables: list[str] | None = None,
    ) -> bool:
        """
        Return True if a value maps to multiple distinct (table, column) pairs
        that span more than one table (after preferred-table narrowing).
        """
        locations = self.lookup(value, preferred_tables)
        if len(locations) <= 1:
            return False

        tables = {loc.table.lower() for loc in locations}
        return len(tables) > 1

    def disambiguation_candidates(
        self,
        value: str,
        preferred_tables: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Return structured disambiguation options for an ambiguous value.

        Each option includes the table, column, and a human-readable description.
        """
        locations = self.lookup(value, preferred_tables)
        if len(locations) <= 1:
            return []

        candidates: list[dict[str, Any]] = []
        for loc in locations:
            candidates.append({
                "value": value,
                "table": loc.table,
                "column": loc.column,
                "count": loc.count,
                "description": f"'{value}' in {loc.table}.{loc.column} ({loc.count:,} rows)",
            })
        return candidates

    # ── Mutation (for preprocessing) ───────────────────────────────────

    def add(self, value: str, table: str, column: str, count: int = 0, source: str = "index") -> None:
        """Add a value → location mapping."""
        key = self._normalise(value)
        if not key:
            return
        loc = ValueLocation(table=table, column=column, count=count, source=source)
        self._index.setdefault(key, []).append(loc)

    def save(self, path: str | Path | None = None) -> None:
        """Persist index to JSON."""
        target = Path(path) if path else self._path
        target.parent.mkdir(parents=True, exist_ok=True)
        payload: dict[str, list[dict]] = {}
        for key, locations in sorted(self._index.items()):
            payload[key] = [loc.to_dict() for loc in locations]
        target.write_text(json.dumps(payload, indent=2))
        logger.info("Value index saved: %d values → %s", len(payload), target)

    # ── Internal ───────────────────────────────────────────────────────

    def _load(self) -> None:
        if not self._path.exists():
            logger.debug("No value index at %s — starting empty", self._path)
            return
        try:
            raw = json.loads(self._path.read_text())
            for key, entries in raw.items():
                locations = [
                    ValueLocation(
                        table=e.get("table", ""),
                        column=e.get("column", ""),
                        count=e.get("count", 0),
                        source=e.get("source", "index"),
                    )
                    for e in entries
                ]
                self._index[key] = locations
            self._loaded = True
            logger.info("Value index loaded: %d values from %s", len(self._index), self._path)
        except Exception as e:
            logger.warning("Failed to load value index: %s", e)

    @staticmethod
    def _normalise(value: str) -> str:
        return " ".join(str(value).lower().strip().split())
