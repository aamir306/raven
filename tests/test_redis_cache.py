"""Tests for redis_cache.py (mocked Redis)."""

from unittest.mock import MagicMock, patch

import pytest

from src.raven.redis_cache import HybridCache, RedisCache


# ── RedisCache tests (mocked) ─────────────────────────────────────────

class TestRedisCache:
    def _make_connected_cache(self, mock_client=None):
        """Create a RedisCache that thinks it's connected."""
        cache = RedisCache.__new__(RedisCache)
        cache._url = "redis://localhost:6379/0"
        cache._ttl = 3600
        cache._prefix = "raven:"
        cache._enabled = True
        cache._client = mock_client or MagicMock()
        cache._connected = True
        cache._hits = 0
        cache._misses = 0
        return cache

    def test_get_miss(self):
        mock_client = MagicMock()
        mock_client.get.return_value = None
        cache = self._make_connected_cache(mock_client)

        result = cache.get("What is revenue?")
        assert result is None
        assert cache._misses == 1

    def test_get_hit(self):
        import json
        mock_client = MagicMock()
        mock_client.get.return_value = json.dumps({"status": "success", "sql": "SELECT 1"})
        cache = self._make_connected_cache(mock_client)

        result = cache.get("What is revenue?")
        assert result == {"status": "success", "sql": "SELECT 1"}
        assert cache._hits == 1

    def test_put(self):
        mock_client = MagicMock()
        cache = self._make_connected_cache(mock_client)

        cache.put("What is revenue?", {"status": "success", "sql": "SELECT 1"})
        mock_client.setex.assert_called_once()

    def test_put_skips_non_success(self):
        mock_client = MagicMock()
        cache = self._make_connected_cache(mock_client)

        cache.put("What is revenue?", {"status": "error", "error": "bad"})
        mock_client.setex.assert_not_called()

    def test_invalidate_specific(self):
        mock_client = MagicMock()
        mock_client.delete.return_value = 1
        cache = self._make_connected_cache(mock_client)

        cache.invalidate("What is revenue?")
        mock_client.delete.assert_called_once()

    def test_check_rate_allowed(self):
        mock_client = MagicMock()
        mock_pipeline = MagicMock()
        mock_pipeline.execute.return_value = [None, None, 3, None]
        mock_client.pipeline.return_value = mock_pipeline
        cache = self._make_connected_cache(mock_client)

        allowed, remaining = cache.check_rate("user1", rpm=10, window=60)
        assert allowed is True
        assert remaining == 7  # 10 - 3

    def test_check_rate_exceeded(self):
        mock_client = MagicMock()
        mock_pipeline = MagicMock()
        mock_pipeline.execute.return_value = [None, None, 15, None]
        mock_client.pipeline.return_value = mock_pipeline
        cache = self._make_connected_cache(mock_client)

        allowed, remaining = cache.check_rate("user1", rpm=10, window=60)
        assert allowed is False
        assert remaining == 0

    def test_disconnected_get_returns_none(self):
        cache = RedisCache.__new__(RedisCache)
        cache._connected = False
        assert cache.get("anything") is None

    def test_disconnected_rate_allows_all(self):
        cache = RedisCache.__new__(RedisCache)
        cache._connected = False
        allowed, remaining = cache.check_rate("user1")
        assert allowed is True

    def test_stats(self):
        mock_client = MagicMock()
        cache = self._make_connected_cache(mock_client)
        cache._hits = 5
        cache._misses = 3
        stats = cache.stats()
        assert stats["hits"] == 5
        assert stats["misses"] == 3
        assert stats["backend"] == "redis"

    def test_from_env_defaults_to_memory(self):
        """Default RAVEN_CACHE_BACKEND=memory means Redis is not enabled."""
        with patch.dict("os.environ", {}, clear=False):
            cache = RedisCache.from_env()
            assert cache._enabled is False


# ── HybridCache tests ─────────────────────────────────────────────────

class TestHybridCache:
    def test_l1_only_mode(self):
        """When Redis is unavailable, should still work with L1 memory."""
        # Create a disconnected redis cache
        mock_redis = RedisCache.__new__(RedisCache)
        mock_redis._connected = False
        mock_redis._url = ""
        mock_redis._ttl = 3600
        mock_redis._prefix = "raven:"
        mock_redis._enabled = False
        mock_redis._client = None
        mock_redis._hits = 0
        mock_redis._misses = 0

        hybrid = HybridCache(redis_cache=mock_redis)
        hybrid.put("What is revenue?", {"status": "success", "sql": "SELECT 1"})
        result = hybrid.get("What is revenue?")
        assert result == {"status": "success", "sql": "SELECT 1"}

    def test_l1_miss_l2_hit(self):
        import json
        mock_client = MagicMock()
        mock_client.get.return_value = json.dumps({"status": "success", "sql": "SELECT 1"})

        mock_redis = RedisCache.__new__(RedisCache)
        mock_redis._connected = True
        mock_redis._url = ""
        mock_redis._ttl = 3600
        mock_redis._prefix = "raven:"
        mock_redis._enabled = True
        mock_redis._client = mock_client
        mock_redis._hits = 0
        mock_redis._misses = 0

        hybrid = HybridCache(redis_cache=mock_redis)
        result = hybrid.get("What is revenue?")
        assert result == {"status": "success", "sql": "SELECT 1"}
        # L2 hit should be promoted to L1
        assert hybrid._l2_hits == 1

    def test_put_writes_both_tiers(self):
        mock_client = MagicMock()

        mock_redis = RedisCache.__new__(RedisCache)
        mock_redis._connected = True
        mock_redis._url = ""
        mock_redis._ttl = 3600
        mock_redis._prefix = "raven:"
        mock_redis._enabled = True
        mock_redis._client = mock_client
        mock_redis._hits = 0
        mock_redis._misses = 0

        hybrid = HybridCache(redis_cache=mock_redis)
        hybrid.put("test question", {"status": "success", "sql": "SELECT 1"})
        mock_client.setex.assert_called_once()

    def test_invalidate_both(self):
        mock_client = MagicMock()

        mock_redis = RedisCache.__new__(RedisCache)
        mock_redis._connected = True
        mock_redis._url = ""
        mock_redis._ttl = 3600
        mock_redis._prefix = "raven:"
        mock_redis._enabled = True
        mock_redis._client = mock_client
        mock_redis._hits = 0
        mock_redis._misses = 0

        hybrid = HybridCache(redis_cache=mock_redis)
        hybrid.put("q1", {"status": "success", "sql": "SELECT 1"})
        hybrid.invalidate("q1")
        mock_client.delete.assert_called_once()
        assert hybrid.get("q1") is None
