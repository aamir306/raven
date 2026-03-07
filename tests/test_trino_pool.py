"""Tests for connectors/trino_pool.py (mocked Trino connector)."""

import asyncio
from unittest.mock import MagicMock, patch

import pytest

from src.raven.connectors.trino_pool import PooledConnection, TrinoSessionPool


# ── PooledConnection tests ─────────────────────────────────────────────

class TestPooledConnection:
    def test_basic_properties(self):
        mock_conn = MagicMock()
        pc = PooledConnection(connection=mock_conn)
        assert pc.query_count == 0
        assert pc.is_healthy is True
        assert pc.age_seconds >= 0
        assert pc.idle_seconds >= 0

    def test_query_count_increments(self):
        mock_conn = MagicMock()
        pc = PooledConnection(connection=mock_conn)
        pc.query_count += 1
        assert pc.query_count == 1

    def test_connection_stored(self):
        mock_conn = MagicMock()
        pc = PooledConnection(connection=mock_conn)
        assert pc.connection is mock_conn


# ── TrinoSessionPool tests ────────────────────────────────────────────

class TestTrinoSessionPool:
    def _make_pool(self, **kwargs) -> TrinoSessionPool:
        mock_connector = MagicMock()
        defaults = dict(
            connector=mock_connector,
            pool_size=3,
            max_concurrent=2,
        )
        defaults.update(kwargs)
        return TrinoSessionPool(**defaults)

    def test_creation(self):
        pool = self._make_pool()
        assert pool._pool_size == 3
        assert pool._max_concurrent == 2

    def test_stats(self):
        pool = self._make_pool()
        stats = pool.stats()
        assert "pool_size" in stats
        assert stats["pool_size"] == 3
        assert stats["max_concurrent"] == 2
        assert stats["idle_connections"] == 0
        assert stats["queries_executed"] == 0

    def test_pool_size_from_env(self):
        with patch.dict("os.environ", {"RAVEN_TRINO_POOL_SIZE": "7"}):
            mock_connector = MagicMock()
            pool = TrinoSessionPool(connector=mock_connector)
            assert pool._pool_size == 7

    def test_max_concurrent_from_env(self):
        with patch.dict("os.environ", {"RAVEN_TRINO_MAX_CONCURRENT": "5"}):
            mock_connector = MagicMock()
            pool = TrinoSessionPool(connector=mock_connector)
            assert pool._max_concurrent == 5

    @pytest.mark.asyncio
    async def test_execute(self):
        import pandas as pd
        mock_connector = MagicMock()
        mock_connector.execute.return_value = pd.DataFrame({"col1": [1, 2]})

        pool = TrinoSessionPool(connector=mock_connector, pool_size=2)
        df = await pool.execute("SELECT 1")
        assert len(df) == 2
        assert pool._queries_executed == 1

    @pytest.mark.asyncio
    async def test_evict_idle(self):
        pool = self._make_pool(max_idle_time=0)
        evicted = await pool.evict_idle()
        assert evicted >= 0

    @pytest.mark.asyncio
    async def test_close(self):
        pool = self._make_pool()
        await pool.close()
        # Should not raise
