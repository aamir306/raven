"""
Redis-backed cache and rate-limiter for RAVEN.

Falls back to the existing in-memory ``QueryCache`` when Redis is unavailable.

Configuration via environment variables:
  RAVEN_REDIS_URL      — Redis connection URL (default: redis://localhost:6379/0)
  RAVEN_CACHE_BACKEND  — "redis" | "memory" (default: "memory")
  RAVEN_CACHE_TTL      — TTL in seconds (default: 3600)
  RAVEN_CACHE_PREFIX   — Key prefix (default: "raven:")
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from typing import Any

logger = logging.getLogger(__name__)

_DEFAULT_TTL = 3600
_DEFAULT_PREFIX = "raven:"

try:
    import redis

    HAS_REDIS = True
except ImportError:
    HAS_REDIS = False


def _cache_key(prefix: str, question: str) -> str:
    """Deterministic cache key from question text."""
    normalized = " ".join(question.lower().split())
    digest = hashlib.sha256(normalized.encode()).hexdigest()[:24]
    return f"{prefix}cache:{digest}"


def _rate_key(prefix: str, client_id: str) -> str:
    """Rate limiter key for a client (IP or user ID)."""
    return f"{prefix}rate:{client_id}"


class RedisCache:
    """Redis-backed query cache with automatic fallback.

    Usage::

        cache = RedisCache.from_env()
        cache.put("What is revenue?", {"status": "success", "sql": "..."})
        result = cache.get("What is revenue?")  # dict or None
    """

    def __init__(
        self,
        url: str = "redis://localhost:6379/0",
        ttl: int = _DEFAULT_TTL,
        prefix: str = _DEFAULT_PREFIX,
        enabled: bool = True,
    ):
        self._url = url
        self._ttl = ttl
        self._prefix = prefix
        self._enabled = enabled
        self._client: Any = None
        self._connected = False
        self._hits = 0
        self._misses = 0

        if enabled and HAS_REDIS:
            try:
                self._client = redis.Redis.from_url(
                    url,
                    decode_responses=True,
                    socket_connect_timeout=2,
                    socket_timeout=2,
                    retry_on_timeout=True,
                )
                self._client.ping()
                self._connected = True
                logger.info("Redis cache connected: %s", url)
            except Exception as exc:
                logger.warning(
                    "Redis unavailable (%s), falling back to in-memory cache: %s",
                    url,
                    exc,
                )
                self._connected = False

    @classmethod
    def from_env(cls) -> RedisCache:
        """Create from environment variables."""
        url = os.getenv("RAVEN_REDIS_URL", "redis://localhost:6379/0")
        backend = os.getenv("RAVEN_CACHE_BACKEND", "memory").lower()
        ttl = int(os.getenv("RAVEN_CACHE_TTL", str(_DEFAULT_TTL)))
        prefix = os.getenv("RAVEN_CACHE_PREFIX", _DEFAULT_PREFIX)
        enabled = backend == "redis"
        return cls(url=url, ttl=ttl, prefix=prefix, enabled=enabled)

    @property
    def is_connected(self) -> bool:
        return self._connected

    # ── Cache operations ───────────────────────────────────────────────

    def get(self, question: str) -> dict[str, Any] | None:
        """Look up cached result. Returns None on miss."""
        if not self._connected:
            return None

        key = _cache_key(self._prefix, question)
        try:
            raw = self._client.get(key)
            if raw is None:
                self._misses += 1
                return None
            self._hits += 1
            return json.loads(raw)
        except Exception as exc:
            logger.debug("Redis GET failed: %s", exc)
            self._misses += 1
            return None

    def put(self, question: str, result: dict[str, Any]) -> None:
        """Store a result with TTL."""
        if not self._connected:
            return

        if result.get("status") not in ("success",):
            return

        key = _cache_key(self._prefix, question)
        try:
            self._client.setex(key, self._ttl, json.dumps(result, default=str))
        except Exception as exc:
            logger.debug("Redis SET failed: %s", exc)

    def invalidate(self, question: str | None = None) -> None:
        """Invalidate a specific question or all cached results."""
        if not self._connected:
            return

        try:
            if question:
                key = _cache_key(self._prefix, question)
                self._client.delete(key)
            else:
                # Scan and delete all cache keys
                pattern = f"{self._prefix}cache:*"
                cursor = 0
                while True:
                    cursor, keys = self._client.scan(cursor, match=pattern, count=100)
                    if keys:
                        self._client.delete(*keys)
                    if cursor == 0:
                        break
        except Exception as exc:
            logger.debug("Redis INVALIDATE failed: %s", exc)

    def stats(self) -> dict[str, Any]:
        """Return cache statistics."""
        total = self._hits + self._misses
        result: dict[str, Any] = {
            "backend": "redis" if self._connected else "disconnected",
            "url": self._url,
            "ttl_seconds": self._ttl,
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": round(self._hits / max(total, 1) * 100, 1),
        }
        if self._connected:
            try:
                info = self._client.info("keyspace")
                result["redis_keys"] = info
            except Exception:
                pass
        return result

    # ── Rate limiting ──────────────────────────────────────────────────

    def check_rate(
        self,
        client_id: str,
        rpm: int = 60,
        window: int = 60,
    ) -> tuple[bool, int]:
        """Check rate limit for a client.

        Returns:
            (allowed, remaining) — whether the request is allowed and remaining quota
        """
        if not self._connected:
            return True, rpm  # Allow all if Redis unavailable

        key = _rate_key(self._prefix, client_id)
        now = time.time()

        try:
            pipe = self._client.pipeline()
            # Remove expired entries
            pipe.zremrangebyscore(key, 0, now - window)
            # Add current request
            pipe.zadd(key, {str(now): now})
            # Count requests in window
            pipe.zcard(key)
            # Set expiry on the key
            pipe.expire(key, window)
            results = pipe.execute()

            count = results[2]
            remaining = max(0, rpm - count)
            allowed = count <= rpm

            return allowed, remaining
        except Exception as exc:
            logger.debug("Redis rate check failed: %s", exc)
            return True, rpm

    def rate_stats(self, client_id: str, window: int = 60) -> dict[str, Any]:
        """Get rate limit stats for a client."""
        if not self._connected:
            return {"backend": "disconnected"}

        key = _rate_key(self._prefix, client_id)
        try:
            now = time.time()
            count = self._client.zcount(key, now - window, "+inf")
            return {
                "backend": "redis",
                "client_id": client_id,
                "requests_in_window": count,
                "window_seconds": window,
            }
        except Exception:
            return {"backend": "error"}


class HybridCache:
    """Two-tier cache: in-memory L1 + Redis L2.

    Queries hit L1 first (fast, bounded), then L2 (shared, persistent).
    Writes go to both. This gives the best of both worlds:
    single-process speed + multi-process consistency.
    """

    def __init__(
        self,
        *,
        memory_cache: Any = None,  # QueryCache instance
        redis_cache: RedisCache | None = None,
    ):
        from .cache import QueryCache

        self.l1 = memory_cache or QueryCache()
        self.l2 = redis_cache or RedisCache.from_env()
        self._l2_hits = 0

    def get(self, question: str) -> dict[str, Any] | None:
        """Try L1 first, then L2. Promote L2 hits to L1."""
        result = self.l1.get(question)
        if result is not None:
            return result

        result = self.l2.get(question)
        if result is not None:
            self._l2_hits += 1
            self.l1.put(question, result)
            return result

        return None

    def put(self, question: str, result: dict[str, Any]) -> None:
        """Write to both tiers."""
        self.l1.put(question, result)
        self.l2.put(question, result)

    def invalidate(self, question: str | None = None) -> None:
        """Invalidate both tiers."""
        self.l1.invalidate(question)
        self.l2.invalidate(question)

    def stats(self) -> dict[str, Any]:
        return {
            "l1": self.l1.stats(),
            "l2": self.l2.stats(),
            "l2_promotions": self._l2_hits,
        }
