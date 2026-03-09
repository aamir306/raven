"""Normalized Metabase assets for query-family sync."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any

from ..query_families.registry import FamilyEntry

_TABLE_RE = re.compile(r"(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_.]*)", re.IGNORECASE)


def _parse_tables_from_sql(sql: str) -> list[str]:
    matches = _TABLE_RE.findall(str(sql or ""))
    return list(dict.fromkeys(matches))


@dataclass(frozen=True)
class MetabaseQueryAsset:
    asset_type: str
    asset_id: str
    name: str
    sql: str
    tables: tuple[str, ...] = field(default_factory=tuple)
    display: str = ""
    description: str = ""
    scope_type: str = ""
    scope_id: str = ""
    scope_name: str = ""
    tags: tuple[str, ...] = field(default_factory=tuple)
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def scope_key(self) -> str:
        if self.scope_type and self.scope_id:
            return f"{self.scope_type}:{self.scope_id}"
        return self.scope_type or self.scope_id

    @property
    def source(self) -> str:
        return "metabase_sync"

    @property
    def source_key(self) -> str:
        suffix = self.scope_key or self.asset_id or "unknown"
        return f"{self.source}:{suffix}"

    def to_family_entry(self, normalize_fn: Any = None) -> FamilyEntry:
        family_key = normalize_fn(self.name) if normalize_fn else self.name.lower().strip()
        metadata = dict(self.metadata)
        metadata.update(
            {
                "asset_id": self.asset_id,
                "asset_type": self.asset_type,
                "scope_key": self.scope_key,
                "scope_name": self.scope_name,
                "display": self.display,
                "description": self.description,
                "source_key": self.source_key,
            }
        )
        return FamilyEntry(
            family_key=family_key,
            template_question=self.name,
            template_sql=self.sql,
            tables_used=list(self.tables),
            source=self.source,
            tags=list(self.tags),
            metadata=metadata,
        )

    def to_embedding_record(self) -> dict[str, Any]:
        metadata = {
            "asset_id": self.asset_id,
            "asset_type": self.asset_type,
            "scope_key": self.scope_key,
            "scope_name": self.scope_name,
            "tables_used": list(self.tables),
            "display": self.display,
            "description": self.description,
            "source": self.source,
        }
        return {
            "question_text": self.name,
            "sql_query": self.sql,
            "source": self.source_key,
            "metadata": metadata,
        }


def build_metabase_query_assets(
    cards: list[dict[str, Any]],
    *,
    scope_type: str,
    scope_id: str | int,
    scope_name: str,
    asset_type: str = "metabase_card",
) -> list[MetabaseQueryAsset]:
    assets: list[MetabaseQueryAsset] = []
    for card in cards:
        name = str(card.get("name", "") or "").strip()
        sql = str(card.get("sql", "") or "").strip()
        if not name or not sql:
            continue
        tables = tuple(card.get("tables", []) or _parse_tables_from_sql(sql))
        tags = tuple(str(tag).strip() for tag in (card.get("tags", []) or []) if str(tag).strip())
        assets.append(
            MetabaseQueryAsset(
                asset_type=str(card.get("kind", asset_type) or asset_type),
                asset_id=str(card.get("card_id") or card.get("id") or ""),
                name=name,
                sql=sql,
                tables=tables,
                display=str(card.get("display", "") or ""),
                description=str(card.get("description", "") or ""),
                scope_type=str(scope_type or ""),
                scope_id=str(scope_id or ""),
                scope_name=str(scope_name or ""),
                tags=tags,
                metadata=dict(card.get("metadata", {})),
            )
        )
    return assets
