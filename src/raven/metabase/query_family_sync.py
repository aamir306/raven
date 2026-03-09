"""Persistent Metabase -> query-family sync."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .assets import build_metabase_query_assets
from ..query_families.registry import QueryFamilyRegistry


class MetabaseQueryFamilySync:
    """Sync Metabase cards/questions into the persistent query-family registry."""

    def __init__(
        self,
        registry: QueryFamilyRegistry,
        *,
        registry_path: str | Path | None = None,
        pgvector: Any | None = None,
        openai: Any | None = None,
        normalize_fn: Any = None,
    ) -> None:
        self.registry = registry
        self.registry_path = Path(registry_path) if registry_path else None
        self.pgvector = pgvector
        self.openai = openai
        self.normalize_fn = normalize_fn

    async def sync_cards(
        self,
        cards: list[dict[str, Any]],
        *,
        scope_type: str,
        scope_id: str | int,
        scope_name: str,
        persist_embeddings: bool = False,
    ) -> dict[str, Any]:
        assets = build_metabase_query_assets(
            cards,
            scope_type=scope_type,
            scope_id=scope_id,
            scope_name=scope_name,
        )
        scope_key = f"{scope_type}:{scope_id}"
        entries = [asset.to_family_entry(self.normalize_fn) for asset in assets]
        synced_count = self.registry.replace_scope(scope_key=scope_key, entries=entries)

        if self.registry_path:
            self.registry.save(self.registry_path)

        embedded_count = 0
        if persist_embeddings and assets and self.pgvector is not None and self.openai is not None:
            embedded_count = await self._sync_embeddings(assets)

        return {
            "scope_key": scope_key,
            "scope_name": scope_name,
            "synced_count": synced_count,
            "embedded_count": embedded_count,
            "registry_size": self.registry.size,
            "tables": sorted({table for asset in assets for table in asset.tables}),
        }

    async def _sync_embeddings(self, assets: list[Any]) -> int:
        source_keys = sorted({asset.source_key for asset in assets})
        for source_key in source_keys:
            self.pgvector.delete_by_source(table="question_embeddings", source=source_key)

        embeddings = await self.openai.batch_embed([asset.name for asset in assets])
        items = []
        for asset, embedding in zip(assets, embeddings, strict=False):
            record = asset.to_embedding_record()
            record["embedding"] = embedding
            items.append(record)
        return self.pgvector.batch_insert(table="question_embeddings", items=items)
