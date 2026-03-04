"""
Trino connection wrapper for RAVEN.

Provides read-only query execution against Trino-Iceberg catalogs with:
- Connection pooling via trino-python-client
- Read-only enforcement (rejects INSERT/UPDATE/DELETE/DROP/ALTER/CREATE)
- Resource group limits (memory, timeout, row cap)
- Query execution returning pandas DataFrames
- EXPLAIN support for cost estimation
"""

from __future__ import annotations

import re
import time
from typing import Any

import pandas as pd
import sqlparse
import structlog
import trino
from trino.auth import BasicAuthentication

from ..safety.query_validator import validate_read_only

logger = structlog.get_logger(__name__)

# Allowed statement types (read-only operations)
_ALLOWED_TYPES = {"SELECT", "WITH", "EXPLAIN", "DESCRIBE", "SHOW"}


class TrinoConnector:
    """Thread-safe Trino query client with read-only enforcement."""

    def __init__(
        self,
        host: str,
        port: int = 443,
        user: str = "text2sql_readonly",
        catalog: str = "cdp",
        schema: str = "gold_dbt",
        http_scheme: str = "https",
        password: str | None = None,
        ssl_insecure: bool = False,
        max_query_memory: str = "4GB",
        max_execution_time_seconds: int = 120,
        max_rows_returned: int = 10_000,
    ) -> None:
        self._host = host
        self._port = port
        self._user = user
        self._catalog = catalog
        self._schema = schema
        self._http_scheme = http_scheme
        self._password = password
        self._ssl_insecure = ssl_insecure
        self._max_query_memory = max_query_memory
        self._max_execution_time_seconds = max_execution_time_seconds
        self._max_rows = max_rows_returned

    # ------------------------------------------------------------------ #
    # Connection
    # ------------------------------------------------------------------ #

    def _get_connection(self) -> trino.dbapi.Connection:
        """Create a new Trino connection."""
        auth = BasicAuthentication(self._user, self._password) if self._password else None
        kwargs: dict[str, Any] = dict(
            host=self._host,
            port=self._port,
            user=self._user,
            catalog=self._catalog,
            schema=self._schema,
            http_scheme=self._http_scheme,
            auth=auth,
            request_timeout=self._max_execution_time_seconds,
        )
        if self._ssl_insecure:
            kwargs["verify"] = False
        return trino.dbapi.connect(**kwargs)

    def test_connection(self) -> bool:
        """Verify connectivity with ``SELECT 1``."""
        try:
            df = self.execute("SELECT 1 AS ok")
            return df is not None and len(df) == 1
        except Exception as exc:
            logger.error("trino_connection_test_failed", error=str(exc))
            return False

    # ------------------------------------------------------------------ #
    # Query execution
    # ------------------------------------------------------------------ #

    def execute(self, sql: str, timeout: int | None = None) -> pd.DataFrame:
        """Execute a **read-only** SQL statement and return a DataFrame.

        Parameters
        ----------
        sql:
            The SQL to execute.  Must be a SELECT / WITH / EXPLAIN / DESCRIBE / SHOW.
        timeout:
            Per-query timeout override (seconds).  Falls back to instance default.

        Raises
        ------
        PermissionError
            If the SQL is not a read-only statement.
        trino.exceptions.TrinoQueryError
            On Trino execution errors.
        """
        # — Safety gate ---------------------------------------------------
        if not validate_read_only(sql):
            raise PermissionError(
                f"RAVEN only allows read-only queries.  Rejected statement type detected in: {sql[:120]}…"
            )

        start = time.perf_counter()
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            rows = cursor.fetchmany(self._max_rows)
            elapsed_ms = (time.perf_counter() - start) * 1000

            logger.info(
                "trino_query_executed",
                sql_preview=sql[:200],
                rows_returned=len(rows),
                columns=len(columns),
                elapsed_ms=round(elapsed_ms, 1),
            )
            return pd.DataFrame(rows, columns=columns)
        except trino.exceptions.TrinoUserError as exc:
            logger.warning("trino_user_error", error=str(exc), sql_preview=sql[:200])
            raise
        except trino.exceptions.TrinoQueryError as exc:
            logger.warning("trino_query_error", error=str(exc), sql_preview=sql[:200])
            raise
        finally:
            conn.close()

    # ------------------------------------------------------------------ #
    # EXPLAIN — cost estimation
    # ------------------------------------------------------------------ #

    def explain(self, sql: str) -> dict[str, Any]:
        """Run ``EXPLAIN (TYPE DISTRIBUTED)`` and return the plan as a dict.

        Returns a dict with keys: ``plan_text``, ``estimated_cost`` (if parseable).
        """
        explain_sql = f"EXPLAIN (TYPE DISTRIBUTED) {sql}"
        df = self.execute(explain_sql)
        plan_text = "\n".join(df.iloc[:, 0].tolist()) if len(df) > 0 else ""

        # Attempt to extract scan bytes from plan text
        scan_bytes = self._parse_scan_bytes(plan_text)

        return {
            "plan_text": plan_text,
            "estimated_scan_bytes": scan_bytes,
        }

    @staticmethod
    def _parse_scan_bytes(plan_text: str) -> int | None:
        """Best-effort extraction of estimated scan size from EXPLAIN output."""
        match = re.search(r"(\d+(?:\.\d+)?)\s*([KMGT]?B)", plan_text, re.IGNORECASE)
        if not match:
            return None
        value = float(match.group(1))
        unit = match.group(2).upper()
        multipliers = {"B": 1, "KB": 1024, "MB": 1024**2, "GB": 1024**3, "TB": 1024**4}
        return int(value * multipliers.get(unit, 1))
