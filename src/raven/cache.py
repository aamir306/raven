"""
Query Result Cache
==================
In-memory LRU cache with TTL for pipeline results.
Avoids recomputing identical questions within a configurable window.

Also supports semantic deduplication via embedding similarity
(future: pgvector-backed persistent cache).
"""

from __future__ import annotations

import hashlib
import logging
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

_DEFAULT_TTL = 3600  # 1 hour
_DEFAULT_MAX_SIZE = 500


@dataclass
class CacheEntry:
    """A cached pipeline result with expiration."""
    result: dict[str, Any]
    created_at: float
    ttl: float
    hit_count: int = 0

    @property
    def is_expired(self) -> bool:
        return time.monotonic() - self.created_at > self.ttl


class QueryCache:
    """Thread-safe LRU cache for pipeline results, keyed by normalized question text."""

    def __init__(
        self,
        max_size: int = _DEFAULT_MAX_SIZE,
        ttl_seconds: float = _DEFAULT_TTL,
        enabled: bool = True,
    ) -> None:
        self._store: OrderedDict[str, CacheEntry] = OrderedDict()
        self._max_size = max_size
        self._ttl = ttl_seconds
        self._enabled = enabled
        self._hits = 0
        self._misses = 0

    @property
    def enabled(self) -> bool:
        return self._enabled

    @staticmethod
    def _normalize(question: str) -> str:
        """Normalize question text for cache key generation."""
        # Lowercase, strip whitespace, collapse multiple spaces
        q = " ".join(question.lower().split())
        return q

    @staticmethod
    def _hash(text: str) -> str:
        """Generate a short hash for the normalized question."""
        return hashlib.sha256(text.encode()).hexdigest()[:16]

    def get(self, question: str) -> dict[str, Any] | None:
        """Look up a cached result. Returns None on miss or expiry."""
        if not self._enabled:
            return None

        key = self._hash(self._normalize(question))
        entry = self._store.get(key)

        if entry is None:
            self._misses += 1
            return None

        if entry.is_expired:
            del self._store[key]
            self._misses += 1
            logger.debug("Cache expired for key %s", key)
            return None

        # Move to end (most recently used)
        self._store.move_to_end(key)
        entry.hit_count += 1
        self._hits += 1

        logger.info("Cache hit for key %s (hits=%d)", key, entry.hit_count)
        return entry.result

    def put(self, question: str, result: dict[str, Any]) -> None:
        """Store a pipeline result in the cache."""
        if not self._enabled:
            return

        # Don't cache errors or ambiguous responses
        if result.get("status") not in ("success",):
            return

        key = self._hash(self._normalize(question))

        # Evict oldest if at capacity
        while len(self._store) >= self._max_size:
            evicted_key, _ = self._store.popitem(last=False)
            logger.debug("Cache eviction: %s", evicted_key)

        self._store[key] = CacheEntry(
            result=result,
            created_at=time.monotonic(),
            ttl=self._ttl,
        )

    def invalidate(self, question: str | None = None) -> None:
        """Invalidate a specific question or the entire cache."""
        if question:
            key = self._hash(self._normalize(question))
            self._store.pop(key, None)
        else:
            self._store.clear()
            logger.info("Cache cleared")

    def stats(self) -> dict[str, Any]:
        """Return cache statistics."""
        # Prune expired entries
        expired_keys = [k for k, v in self._store.items() if v.is_expired]
        for k in expired_keys:
            del self._store[k]

        return {
            "size": len(self._store),
            "max_size": self._max_size,
            "ttl_seconds": self._ttl,
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": round(self._hits / max(self._hits + self._misses, 1) * 100, 1),
            "enabled": self._enabled,
        }
