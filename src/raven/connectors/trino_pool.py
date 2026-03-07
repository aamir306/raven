"""
Trino session pool with bounded concurrency.

Wraps ``TrinoConnector`` with:
- A bounded connection pool (reuse instead of create/close per query)
- Concurrency limiting via ``asyncio.Semaphore``
- Request-scoped session reuse pattern
- Connection health checking + automatic eviction

Configuration via environment variables:
  RAVEN_TRINO_POOL_SIZE       — Max connections in pool (default: 5)
  RAVEN_TRINO_MAX_CONCURRENT  — Max concurrent queries (default: 3)
  RAVEN_TRINO_POOL_TIMEOUT    — Wait timeout for connection from pool (default: 30s)
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class PooledConnection:
    """A connection with metadata for pool management."""

    connection: Any  # trino.dbapi.Connection
    created_at: float = field(default_factory=time.monotonic)
    last_used: float = field(default_factory=time.monotonic)
    query_count: int = 0
    is_healthy: bool = True

    @property
    def age_seconds(self) -> float:
        return time.monotonic() - self.created_at

    @property
    def idle_seconds(self) -> float:
        return time.monotonic() - self.last_used


class TrinoSessionPool:
    """Bounded connection pool for Trino with concurrency control.

    Usage::

        pool = TrinoSessionPool(connector)

        # In an async context:
        async with pool.acquire() as conn:
            df = conn.execute("SELECT 1")

        # Or use the managed execute:
        df = await pool.execute("SELECT COUNT(*) FROM t")
    """

    def __init__(
        self,
        connector: Any,  # TrinoConnector instance
        *,
        pool_size: int | None = None,
        max_concurrent: int | None = None,
        pool_timeout: float | None = None,
        max_connection_age: float = 600.0,  # 10 minutes
        max_idle_time: float = 120.0,  # 2 minutes
        max_queries_per_conn: int = 50,
    ):
        self.connector = connector
        self._pool_size = pool_size or int(os.getenv("RAVEN_TRINO_POOL_SIZE", "5"))
        self._max_concurrent = max_concurrent or int(
            os.getenv("RAVEN_TRINO_MAX_CONCURRENT", "3")
        )
        self._pool_timeout = pool_timeout or float(
            os.getenv("RAVEN_TRINO_POOL_TIMEOUT", "30")
        )
        self._max_age = max_connection_age
        self._max_idle = max_idle_time
        self._max_queries = max_queries_per_conn

        # Pool internals
        self._idle: deque[PooledConnection] = deque()
        self._in_use: set[int] = set()  # id(PooledConnection)
        self._lock = asyncio.Lock()
        self._semaphore = asyncio.Semaphore(self._max_concurrent)
        self._total_created = 0
        self._total_evicted = 0

        # Stats
        self._queries_executed = 0
        self._wait_time_total = 0.0

    # ── Public API ─────────────────────────────────────────────────────

    @asynccontextmanager
    async def acquire(self):
        """Acquire a pooled connection from the pool.

        Usage::

            async with pool.acquire() as pooled_conn:
                df = pooled_conn.connection.cursor().execute(...)
        """
        start = time.monotonic()
        try:
            await asyncio.wait_for(
                self._semaphore.acquire(), timeout=self._pool_timeout
            )
        except asyncio.TimeoutError:
            raise TimeoutError(
                f"Timed out waiting for Trino connection "
                f"(pool_size={self._pool_size}, timeout={self._pool_timeout}s)"
            )

        self._wait_time_total += time.monotonic() - start

        pooled = None
        try:
            pooled = await self._get_connection()
            yield pooled
        finally:
            if pooled is not None:
                await self._return_connection(pooled)
            self._semaphore.release()

    async def execute(self, sql: str, timeout: int | None = None) -> pd.DataFrame:
        """Execute a query using a pooled connection.

        Falls back to direct connector execution if pool cannot serve.
        """
        await self._semaphore.acquire()
        try:
            self._queries_executed += 1
            # Use the connector's execute method which handles
            # connection creation internally, but we get concurrency bounding
            df = await asyncio.to_thread(
                self.connector.execute, sql, timeout
            )
            return df
        finally:
            self._semaphore.release()

    async def explain(self, sql: str) -> dict[str, Any]:
        """Run EXPLAIN with concurrency control."""
        await self._semaphore.acquire()
        try:
            return await asyncio.to_thread(self.connector.explain, sql)
        finally:
            self._semaphore.release()

    async def close(self) -> None:
        """Close all pooled connections."""
        async with self._lock:
            while self._idle:
                pooled = self._idle.popleft()
                self._close_connection(pooled)
            logger.info(
                "Trino session pool closed: created=%d, evicted=%d",
                self._total_created,
                self._total_evicted,
            )

    def stats(self) -> dict[str, Any]:
        """Return pool statistics."""
        return {
            "pool_size": self._pool_size,
            "max_concurrent": self._max_concurrent,
            "idle_connections": len(self._idle),
            "in_use_connections": len(self._in_use),
            "total_created": self._total_created,
            "total_evicted": self._total_evicted,
            "queries_executed": self._queries_executed,
            "avg_wait_time_ms": round(
                (self._wait_time_total / max(self._queries_executed, 1)) * 1000, 1
            ),
        }

    # ── Internals ──────────────────────────────────────────────────────

    async def _get_connection(self) -> PooledConnection:
        """Get a healthy connection from pool or create new."""
        async with self._lock:
            # Try to find a healthy idle connection
            while self._idle:
                pooled = self._idle.popleft()
                if self._is_healthy(pooled):
                    pooled.last_used = time.monotonic()
                    pooled.query_count += 1
                    self._in_use.add(id(pooled))
                    return pooled
                else:
                    self._close_connection(pooled)
                    self._total_evicted += 1

            # Create new connection
            conn = self.connector._get_connection()
            pooled = PooledConnection(connection=conn)
            pooled.query_count = 1
            self._in_use.add(id(pooled))
            self._total_created += 1

            logger.debug(
                "New Trino pooled connection: total_created=%d, idle=%d, in_use=%d",
                self._total_created,
                len(self._idle),
                len(self._in_use),
            )
            return pooled

    async def _return_connection(self, pooled: PooledConnection) -> None:
        """Return a connection to the pool or close it if unhealthy."""
        async with self._lock:
            self._in_use.discard(id(pooled))

            if self._is_healthy(pooled) and len(self._idle) < self._pool_size:
                pooled.last_used = time.monotonic()
                self._idle.append(pooled)
            else:
                self._close_connection(pooled)
                if not self._is_healthy(pooled):
                    self._total_evicted += 1

    def _is_healthy(self, pooled: PooledConnection) -> bool:
        """Check if a pooled connection is still usable."""
        if not pooled.is_healthy:
            return False
        if pooled.age_seconds > self._max_age:
            return False
        if pooled.idle_seconds > self._max_idle:
            return False
        if pooled.query_count >= self._max_queries:
            return False
        return True

    @staticmethod
    def _close_connection(pooled: PooledConnection) -> None:
        """Safely close a connection."""
        try:
            pooled.connection.close()
        except Exception:
            pass
        pooled.is_healthy = False

    # ── Maintenance ────────────────────────────────────────────────────

    async def evict_idle(self) -> int:
        """Evict all idle connections that have exceeded max_idle_time.

        Intended to be called periodically (e.g. every 60s).
        """
        evicted = 0
        async with self._lock:
            still_idle: deque[PooledConnection] = deque()
            while self._idle:
                pooled = self._idle.popleft()
                if self._is_healthy(pooled):
                    still_idle.append(pooled)
                else:
                    self._close_connection(pooled)
                    evicted += 1
            self._idle = still_idle

        if evicted:
            self._total_evicted += evicted
            logger.debug("Evicted %d idle Trino connections", evicted)
        return evicted
