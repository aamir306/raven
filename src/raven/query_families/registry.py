"""
Query family registry — centralized catalog of known query families.

A family is a normalized question pattern that can be answered by compiling
a trusted SQL template with slot substitutions. The registry:

1. Persists trusted families from semantic assets, Metabase sync, or feedback
2. Keeps a stable registry id per source asset instead of collapsing on family key
3. Supports replacement of an external scope during refresh/sync
4. Exports persisted families back into the live semantic retrieval path
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable, Sequence

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
    expected_slots: list[str] = field(default_factory=list)
    expected_filters: list[str] = field(default_factory=list)

    # ── Additional matching context ──
    dimension_question_phrases: list[str] = field(default_factory=list)
    metric_question_phrases: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    # ── Stable registry identity ──
    registry_id: str = ""

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
    """Centralized catalog of known query families."""

    def __init__(self):
        self._families: dict[str, FamilyEntry] = {}  # registry_id -> FamilyEntry
        self._by_family_key: dict[str, list[str]] = {}
        self._by_table: dict[str, list[str]] = {}
        self._by_category: dict[str, list[str]] = {}

    @property
    def size(self) -> int:
        return len(self._families)

    # ── Lookup ─────────────────────────────────────────────────────────

    def lookup(self, family_key: str) -> FamilyEntry | None:
        """Return the best entry for a normalized family key."""
        entries = self.lookup_all(family_key)
        return entries[0] if entries else None

    def lookup_all(self, family_key: str) -> list[FamilyEntry]:
        keys = self._by_family_key.get(str(family_key or ""), [])
        entries = [self._families[k] for k in keys if k in self._families]
        return sorted(entries, key=self._rank_entry, reverse=True)

    def lookup_by_table(self, table: str) -> list[FamilyEntry]:
        keys = self._by_table.get(str(table or "").lower(), [])
        entries = [self._families[k] for k in keys if k in self._families]
        return sorted(entries, key=self._rank_entry, reverse=True)

    def lookup_by_category(self, category: str) -> list[FamilyEntry]:
        keys = self._by_category.get(str(category or "").lower(), [])
        entries = [self._families[k] for k in keys if k in self._families]
        return sorted(entries, key=self._rank_entry, reverse=True)

    def top_families(self, *, limit: int = 50) -> list[FamilyEntry]:
        entries = sorted(self._families.values(), key=self._rank_entry, reverse=True)
        return entries[:limit]

    def low_confidence_families(self, *, min_failures: int = 2) -> list[FamilyEntry]:
        return [
            e
            for e in self._families.values()
            if e.failure_count >= min_failures and e.success_rate < 0.50
        ]

    # ── Registration ───────────────────────────────────────────────────

    def register(self, entry: FamilyEntry) -> FamilyEntry:
        """Register or update a family entry."""
        entry.registry_id = entry.registry_id or self._derive_registry_id(entry)
        if entry.registry_id in self._families:
            existing = self._families[entry.registry_id]
            entry.hit_count += existing.hit_count
            entry.success_count += existing.success_count
            entry.failure_count += existing.failure_count
            if existing.created_at < entry.created_at:
                entry.created_at = existing.created_at

        self._families[entry.registry_id] = entry
        self._rebuild_indexes()
        return entry

    def register_from_dict(self, data: dict[str, Any]) -> FamilyEntry:
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
            registry_id=str(data.get("registry_id", "")),
        )
        self.register(entry)
        return entry

    def load_from_verified_queries(
        self,
        verified_queries: Sequence[dict[str, Any]],
        *,
        normalize_fn: Any = None,
    ) -> int:
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
        scope_key: str = "",
        scope_name: str = "",
    ) -> int:
        count = 0
        entries: list[FamilyEntry] = []
        for card in cards:
            if card.get("kind") not in {"metabase_card", "metabase_question"}:
                continue
            question = str(card.get("name", ""))
            sql = str(card.get("sql", ""))
            if not question or not sql:
                continue
            key = normalize_fn(question) if normalize_fn else question.lower().strip()
            metadata = dict(card.get("metadata", {}))
            metadata.setdefault("scope_key", scope_key)
            metadata.setdefault("scope_name", scope_name)
            metadata.setdefault("asset_id", str(card.get("card_id") or card.get("id") or ""))
            metadata.setdefault("display", card.get("display", ""))
            metadata.setdefault("asset_type", card.get("kind", "metabase_card"))
            entries.append(
                FamilyEntry(
                    family_key=key,
                    template_question=question,
                    template_sql=sql,
                    tables_used=list(card.get("tables", [])),
                    source=str(card.get("source", "metabase_sync")),
                    metadata=metadata,
                )
            )

        if scope_key:
            self.replace_scope(scope_key=scope_key, entries=entries)
            count = len(entries)
        else:
            for entry in entries:
                self.register(entry)
                count += 1

        logger.info("Loaded %d query families from Metabase cards", count)
        return count

    def remove_where(self, predicate: Callable[[FamilyEntry], bool]) -> int:
        removed_keys = [key for key, entry in self._families.items() if predicate(entry)]
        for key in removed_keys:
            self._families.pop(key, None)
        if removed_keys:
            self._rebuild_indexes()
        return len(removed_keys)

    def replace_scope(self, *, scope_key: str, entries: Sequence[FamilyEntry]) -> int:
        scope_key = str(scope_key or "").strip()
        if not scope_key:
            for entry in entries:
                self.register(entry)
            return len(entries)

        self.remove_where(lambda entry: entry.metadata.get("scope_key") == scope_key)
        for entry in entries:
            entry.metadata.setdefault("scope_key", scope_key)
            self.register(entry)
        return len(entries)

    def export_assets(
        self,
        *,
        source_prefix: str | None = None,
        category: str | None = None,
    ) -> list[dict[str, Any]]:
        entries = list(self._families.values())
        if source_prefix:
            entries = [e for e in entries if str(e.source).startswith(source_prefix)]
        if category:
            entries = [e for e in entries if e.category == category]
        entries = sorted(entries, key=self._rank_entry, reverse=True)
        return [
            {
                "registry_id": entry.registry_id,
                "family_key": entry.family_key,
                "question": entry.template_question,
                "sql": entry.template_sql,
                "tables_used": list(entry.tables_used),
                "source": entry.source,
                "category": entry.category,
                "tags": list(entry.tags),
                "notes": str(entry.metadata.get("notes", "")),
                "metadata": dict(entry.metadata),
                "dimension_question_phrases": list(entry.dimension_question_phrases),
                "metric_question_phrases": list(entry.metric_question_phrases),
                "success_rate": entry.success_rate,
                "hit_count": entry.hit_count,
            }
            for entry in entries
        ]

    # ── Persistence ────────────────────────────────────────────────────

    def save(self, path: str | Path) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "version": "2.0",
            "count": len(self._families),
            "families": [entry.to_dict() for entry in self._families.values()],
        }
        path.write_text(json.dumps(data, indent=2, default=str))
        logger.info("Saved %d families to %s", len(self._families), path)

    def load(self, path: str | Path) -> int:
        path = Path(path)
        if not path.exists():
            return 0
        data = json.loads(path.read_text())
        self._families = {}
        for fam_dict in data.get("families", []):
            entry = FamilyEntry(**{
                k: v
                for k, v in fam_dict.items()
                if k in FamilyEntry.__dataclass_fields__
            })
            self.register(entry)
        logger.info("Loaded %d families from %s", len(self._families), path)
        return len(self._families)

    # ── Statistics ─────────────────────────────────────────────────────

    def stats(self) -> dict[str, Any]:
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

    @staticmethod
    def _rank_entry(entry: FamilyEntry) -> tuple[float, int, float]:
        return (
            entry.success_rate,
            entry.hit_count,
            entry.created_at,
        )

    @staticmethod
    def _derive_registry_id(entry: FamilyEntry) -> str:
        asset_id = str(entry.metadata.get("asset_id", ""))
        scope_key = str(entry.metadata.get("scope_key", ""))
        seed = "|".join(
            [
                str(entry.source or ""),
                asset_id,
                scope_key,
                str(entry.family_key or ""),
                str(entry.template_question or ""),
                hashlib.sha1(str(entry.template_sql or "").encode("utf-8")).hexdigest()[:16],
            ]
        )
        return hashlib.sha1(seed.encode("utf-8")).hexdigest()

    def _rebuild_indexes(self) -> None:
        self._by_family_key = {}
        self._by_table = {}
        self._by_category = {}
        for registry_id, entry in self._families.items():
            self._by_family_key.setdefault(entry.family_key, []).append(registry_id)
            for table in entry.tables_used:
                table_lc = table.lower()
                self._by_table.setdefault(table_lc, []).append(registry_id)
            if entry.category:
                cat_lc = entry.category.lower()
                self._by_category.setdefault(cat_lc, []).append(registry_id)
