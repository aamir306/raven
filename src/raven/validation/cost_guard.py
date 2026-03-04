"""
Cost Guard — Stage 6.3
========================
EXPLAIN-based cost estimation to prevent expensive queries
from reaching production Trino cluster.

Thresholds are loaded from config/cost_guards.yaml.
"""

from __future__ import annotations

import asyncio
import logging
import re
from pathlib import Path
from typing import Any

import yaml

from ..connectors.trino_connector import TrinoConnector

logger = logging.getLogger(__name__)

CONFIG_PATH = Path(__file__).resolve().parents[3] / "config" / "cost_guards.yaml"


class CostGuard:
    """EXPLAIN-based cost guard — block expensive queries."""

    def __init__(self, trino: TrinoConnector, config_path: str | Path | None = None):
        self.trino = trino
        self._config = self._load_config(config_path)

    # ── Public API ──────────────────────────────────────────────────────

    async def check(self, sql: str) -> dict:
        """
        Check if a SQL query passes cost thresholds.

        Returns:
            {
                "passed": True/False,
                "estimated_scan_gb": 12.3,
                "max_scan_gb": 500,
                "explain_ok": True,
                "reason": "" | "Estimated scan 600 GB exceeds 500 GB limit",
            }
        """
        thresholds = self._config.get("thresholds", {})
        max_scan_gb = thresholds.get("max_scan_gb", 500)
        max_rows = thresholds.get("max_output_rows", 100_000)

        result = {
            "passed": True,
            "estimated_scan_gb": 0.0,
            "max_scan_gb": max_scan_gb,
            "explain_ok": False,
            "reason": "",
        }

        try:
            plan_text = await asyncio.to_thread(self.trino.explain, sql)
            result["explain_ok"] = True

            # Parse estimated data scan from EXPLAIN output
            scan_gb = self._parse_scan_size(plan_text)
            result["estimated_scan_gb"] = scan_gb

            if scan_gb > max_scan_gb:
                result["passed"] = False
                result["reason"] = (
                    f"Estimated scan {scan_gb:.1f} GB exceeds "
                    f"{max_scan_gb} GB limit"
                )
                logger.warning(
                    "Cost guard BLOCKED: %s (%.1f GB > %d GB)",
                    sql[:80], scan_gb, max_scan_gb,
                )

            # Check output rows if available
            output_rows = self._parse_output_rows(plan_text)
            if output_rows and output_rows > max_rows:
                result["passed"] = False
                result["reason"] += (
                    f"; Estimated {output_rows:,} output rows exceeds "
                    f"{max_rows:,} limit"
                )

        except Exception as e:
            logger.warning("EXPLAIN failed for cost guard: %s", e)
            result["explain_ok"] = False
            # If EXPLAIN fails, we still allow the query (it might still work)
            result["passed"] = True
            result["reason"] = f"EXPLAIN failed: {e}"

        return result

    # ── Parsers ─────────────────────────────────────────────────────────

    @staticmethod
    def _parse_scan_size(plan: str) -> float:
        """Extract estimated data scan size (GB) from EXPLAIN output."""
        if not plan:
            return 0.0

        # Look for patterns like: "ScanFilter... 12.3GB" or "Input: 12.3GB"
        # Trino EXPLAIN ANALYZE shows actual sizes
        patterns = [
            r"(\d+(?:\.\d+)?)\s*GB",
            r"(\d+(?:\.\d+)?)\s*MB",
            r"(\d+(?:\.\d+)?)\s*TB",
        ]

        max_gb = 0.0
        for pattern in patterns:
            matches = re.findall(pattern, plan, re.IGNORECASE)
            for m in matches:
                val = float(m)
                if "TB" in pattern:
                    val *= 1024
                elif "MB" in pattern:
                    val /= 1024
                max_gb = max(max_gb, val)

        return max_gb

    @staticmethod
    def _parse_output_rows(plan: str) -> int | None:
        """Extract estimated output rows from EXPLAIN."""
        if not plan:
            return None

        match = re.search(r"Output.*?(\d+)\s*rows", plan, re.IGNORECASE)
        if match:
            return int(match.group(1))
        return None

    @staticmethod
    def _load_config(config_path: str | Path | None = None) -> dict:
        """Load cost guard configuration."""
        path = Path(config_path) if config_path else CONFIG_PATH
        if path.exists():
            try:
                return yaml.safe_load(path.read_text()) or {}
            except Exception as exc:
                logger.warning("Failed to load cost guards config: %s", exc)
        return {"thresholds": {"max_scan_gb": 500, "max_output_rows": 100_000}}
