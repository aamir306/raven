"""
Query Executor — Stage 7.1
============================
Executes the final validated SQL on Trino and returns a DataFrame.
Handles execution errors gracefully and applies safety limits.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from ..connectors.trino_connector import TrinoConnector

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 30.0


class QueryExecutor:
    """Execute final SQL on Trino with safety limits and error handling."""

    def __init__(self, trino: TrinoConnector, timeout: float = DEFAULT_TIMEOUT):
        self.trino = trino
        self.timeout = timeout

    async def execute(self, sql: str) -> dict:
        """
        Execute SQL on Trino and return results.

        Returns:
            {
                "success": True/False,
                "df": DataFrame | None,
                "row_count": int,
                "error": "" | "error message",
            }
        """
        if not sql or not sql.strip():
            return {
                "success": False,
                "df": None,
                "row_count": 0,
                "error": "Empty SQL",
            }

        try:
            df = await asyncio.wait_for(
                asyncio.to_thread(self.trino.execute, sql),
                timeout=self.timeout,
            )
            row_count = len(df) if df is not None else 0
            logger.info("Query executed: %d rows returned", row_count)
            return {
                "success": True,
                "df": df,
                "row_count": row_count,
                "error": "",
            }
        except asyncio.TimeoutError:
            msg = f"Query timed out (>{self.timeout}s)"
            logger.warning(msg)
            return {"success": False, "df": None, "row_count": 0, "error": msg}
        except Exception as e:
            msg = f"SQL execution failed: {e}"
            logger.warning(msg)
            return {"success": False, "df": None, "row_count": 0, "error": msg}
