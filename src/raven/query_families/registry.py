"""
Query family registry — centralized catalog of known query families.

A family is a normalized question pattern that can be answered by compiling
a trusted SQL template with slot substitutions. The registry:

1. Indexes families by their normalized key for O(1) lookup
2. Tracks statistics: hit count, last used, success rate
3. Supports runtime registration from Metabase sync or admin API
4. Provides ranked retrieval of candidate families for a question
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Sequence

logger = logging.getLogger(__name__)


@dataclass
class FamilyEntry:
    """A single registered query family."""

    family_key: str  # Normalized question pattern
    template_question: str  # Original question text
    template_sql: str  # Verified SQL template
    tables_used: list[str] = field(default_factory=list)
    source: str = ""  # "semantic_model" | "metabase" | "admin" | "feedback"
    category: str = ""  # Business category for benchmark labeling
    tags: list[str] = field(default_factory=list)

    # ── Statistics ──
    hit_count: int = 0
    success_count: int = 0
    failure_count: int = 0
    last_used: float = 0.0
    created_at: float = field(default_factory=time.time)

    # ── Slot metadata ──
    expected_slots: list[str] = field(default_factory=list)  # e.g. ["limit", "interval"]
    expected_filters: list[str] = field(default_factory=list)  # Column refs for filter subs

    # ── Additional matching context ──
    dimension_question_phrases: list[str] = field(default_factory=list)
    metric_question_phrases: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def success_rate(self) -> float:
        total = self.success_count + self.failure_count
        return self.success_count / total if total > 0 else 0.0

    def record_hit(self, *, success: bool) -> None:
        self.hit_count += 1
        self.last_used = time.time()
        if success:
            self.success_count += 1
        else:
            self.failure_count += 1

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class QueryFamilyRegistry:
    """Centralized catalog of known query families.

    Usage::

        registry = QueryFamilyRegistry()
        registry.load_from_verified_queries(verified_queries)
        registry.load_from_metabase(metabase_cards)

        entry = registry.lookup("total revenue <grain>")
        if entry:
            entry.record_hit(success=True)

        registry.save("data/family_registry.json")
    """

    def __init__(self):
        self._families: dict[str, FamilyEntry] = {}  # key → FamilyEntry
        self._by_table: dict[str, list[str]] = {}  # table → [family_keys]
        self._by_category: dict[str, list[str]] = {}  # category → [family_keys]

    @property
    def size(self) -> int:
        return len(self._families)

    # ── Lookup ─────────────────────────────────────────────────────────

    def lookup(self, family_key: str) -> FamilyEntry | None:
        """Exact lookup by normalized family key."""
        return self._families.get(family_key)

    def lookup_by_table(self, table: str) -> list[FamilyEntry]:
        """Return all families that use a given table."""
        keys = self._by_table.get(table.lower(), [])
        return [self._families[k] for k in keys if k in self._families]

    def lookup_by_category(self, category: str) -> list[FamilyEntry]:
        """Return all families in a given category."""
        keys = self._by_category.get(category.lower(), [])
        return [self._families[k] for k in keys if k in self._families]

    def top_families(self, *, limit: int = 50) -> list[FamilyEntry]:
        """Return top families ranked by hit count."""
        entries = sorted(
            self._families.values(),
            key=lambda e: (e.hit_count, e.success_rate),
            reverse=True,
        )
        return entries[:limit]

    def low_confidence_families(self, *, min_failures: int = 2) -> list[FamilyEntry]:
        """Return families with high failure rates (candidates for review)."""
        return [
            e
            for e in self._families.values()
            if e.failure_count >= min_failures and e.success_rate < 0.50
        ]

    # ── Registration ───────────────────────────────────────────────────

    def register(self, entry: FamilyEntry) -> None:
        """Register or update a family entry."""
        if entry.family_key in self._families:
            existing = self._families[entry.family_key]
            # Merge stats
            entry.hit_count += existing.hit_count
            entry.success_count += existing.success_count
            entry.failure_count += existing.failure_count
            if existing.created_at < entry.created_at:
                entry.created_at = existing.created_at

        self._families[entry.family_key] = entry
        self._rebuild_indexes_for(entry)

    def register_from_dict(self, data: dict[str, Any]) -> FamilyEntry:
        """Register a family from a raw dict (e.g. from YAML/JSON)."""
        entry = FamilyEntry(
            family_key=data.get("family_key", ""),
            template_question=data.get("question", data.get("template_question", "")),
            template_sql=data.get("sql", data.get("template_sql", "")),
            tables_used=list(data.get("tables_used", [])),
            source=data.get("source", "unknown"),
            category=data.get("category", ""),
            tags=list(data.get("tags", [])),
            expected_slots=list(data.get("expected_slots", [])),
            expected_filters=list(data.get("expected_filters", [])),
            dimension_question_phrases=list(data.get("dimension_question_phrases", [])),
            metric_question_phrases=list(data.get("metric_question_phrases", [])),
            metadata=dict(data.get("metadata", {})),
        )
        self.register(entry)
        return entry

    def load_from_verified_queries(
        self,
        verified_queries: Sequence[dict[str, Any]],
        *,
        normalize_fn: Any = None,
    ) -> int:
        """Bulk-load from verified query assets. Returns count loaded."""
        count = 0
        for vq in verified_queries:
            question = vq.get("question", "")
            sql = vq.get("sql", "")
            if not question or not sql:
                continue
            key = normalize_fn(question) if normalize_fn else question.lower().strip()
            entry = FamilyEntry(
                family_key=key,
                template_question=question,
                template_sql=sql,
                tables_used=list(vq.get("tables_used", [])),
                source=vq.get("source", "semantic_model"),
                category=vq.get("category", ""),
                metadata=dict(vq.get("metadata", {})),
            )
            self.register(entry)
            count += 1
        logger.info("Loaded %d query families from verified queries", count)
        return count

    def load_from_metabase(
        self,
        cards: Sequence[dict[str, Any]],
        *,
        normalize_fn: Any = None,
    ) -> int:
        """Bulk-load from Metabase card assets. Returns count loaded."""
        count = 0
        for card in cards:
            if card.get("kind") != "metabase_card":
                continue
            question = str(card.get("name", ""))
            sql = str(card.get("sql", ""))
            if not question or not sql:
                continue
            key = normalize_fn(question) if normalize_fn else question.lower().strip()
            entry = FamilyEntry(
                family_key=key,
                template_question=question,
                template_sql=sql,
                tables_used=list(card.get("tables", [])),
                source="metabase",
                metadata={"focus_name": card.get("focus_name", "")},
            )
            self.register(entry)
            count += 1
        logger.info("Loaded %d query families from Metabase cards", count)
        return count

    # ── Persistence ────────────────────────────────────────────────────

    def save(self, path: str | Path) -> None:
        """Save registry to JSON."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "version": "1.0",
            "count": len(self._families),
            "families": [entry.to_dict() for entry in self._families.values()],
        }
        path.write_text(json.dumps(data, indent=2, default=str))
        logger.info("Saved %d families to %s", len(self._families), path)

    def load(self, path: str | Path) -> int:
        """Load registry from JSON. Returns count loaded."""
        path = Path(path)
        if not path.exists():
            return 0
        data = json.loads(path.read_text())
        families = data.get("families", [])
        for fam_dict in families:
            entry = FamilyEntry(**{
                k: v
                for k, v in fam_dict.items()
                if k in FamilyEntry.__dataclass_fields__
            })
            self._families[entry.family_key] = entry
            self._rebuild_indexes_for(entry)
        logger.info("Loaded %d families from %s", len(families), path)
        return len(families)

    # ── Statistics ─────────────────────────────────────────────────────

    def stats(self) -> dict[str, Any]:
        """Return summary statistics."""
        entries = list(self._families.values())
        total_hits = sum(e.hit_count for e in entries)
        total_success = sum(e.success_count for e in entries)
        total_failure = sum(e.failure_count for e in entries)
        by_source: dict[str, int] = {}
        for e in entries:
            by_source[e.source] = by_source.get(e.source, 0) + 1

        return {
            "total_families": len(entries),
            "total_hits": total_hits,
            "total_success": total_success,
            "total_failure": total_failure,
            "overall_success_rate": (
                total_success / (total_success + total_failure)
                if (total_success + total_failure) > 0
                else 0.0
            ),
            "by_source": by_source,
            "low_confidence_count": len(self.low_confidence_families()),
        }

    # ── Internals ──────────────────────────────────────────────────────

    def _rebuild_indexes_for(self, entry: FamilyEntry) -> None:
        for table in entry.tables_used:
            table_lc = table.lower()
            if table_lc not in self._by_table:
                self._by_table[table_lc] = []
            if entry.family_key not in self._by_table[table_lc]:
                self._by_table[table_lc].append(entry.family_key)

        if entry.category:
            cat_lc = entry.category.lower()
            if cat_lc not in self._by_category:
                self._by_category[cat_lc] = []
            if entry.family_key not in self._by_category[cat_lc]:
                self._by_category[cat_lc].append(entry.family_key)
