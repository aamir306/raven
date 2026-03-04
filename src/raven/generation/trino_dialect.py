"""
Trino Dialect — Stage 5 Utility
=================================
Trino-specific SQL dialect rules, validation helpers,
and common transformations.

Loaded from prompts/trino_dialect_rules.txt (20 rules)
and config/error_taxonomy.json (13 categories, 36 sub-types).
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

RULES_PATH = Path(__file__).resolve().parents[3] / "prompts" / "trino_dialect_rules.txt"
TAXONOMY_PATH = Path(__file__).resolve().parents[3] / "config" / "error_taxonomy.json"


class TrinoDialect:
    """Trino-specific dialect rules, error classification, and SQL utilities."""

    def __init__(self):
        self.rules_text = self._load_rules()
        self.error_taxonomy = self._load_taxonomy()

    # ── Error Classification ───────────────────────────────────────────

    def classify_error(self, error_message: str) -> tuple[str, str, str]:
        """
        Classify a Trino error using the 36-subtype error taxonomy.

        Returns:
            (category, subtype, description)
        """
        msg_lower = error_message.lower()

        # Walk taxonomy for keyword match
        for category, subtypes in self.error_taxonomy.items():
            if isinstance(subtypes, dict):
                for subtype, info in subtypes.items():
                    desc = info if isinstance(info, str) else info.get("description", "")
                    keywords = [w for w in desc.lower().split() if len(w) > 3]
                    if any(kw in msg_lower for kw in keywords):
                        return category, subtype, desc

        # Fallback pattern matching
        patterns = [
            ("syntax", "sql_syntax_error", "SQL syntax error",
             ["syntax", "unexpected", "mismatched input"]),
            ("schema_link", "col_missing", "Referenced column or table does not exist",
             ["does not exist", "cannot be resolved", "column not found"]),
            ("filter", "type_mismatch", "Type mismatch in comparison",
             ["type mismatch", "cannot be cast", "cannot be applied"]),
            ("aggregation", "missing_group_by", "Missing GROUP BY clause",
             ["must be an aggregate", "group by", "not in group by"]),
            ("join", "ambiguous_column", "Ambiguous column reference in JOIN",
             ["ambiguous", "column reference"]),
            ("function", "wrong_arg_type", "Wrong argument type for function",
             ["unexpected parameters", "function not found"]),
            ("date_time", "wrong_date_format", "Wrong date/time format or function",
             ["cannot cast", "date", "timestamp", "interval"]),
        ]

        for category, subtype, desc, keywords in patterns:
            if any(kw in msg_lower for kw in keywords):
                return category, subtype, desc

        return "syntax", "sql_syntax_error", "Unknown error — review SQL syntax"

    # ── SQL Validation Helpers ─────────────────────────────────────────

    @staticmethod
    def is_read_only(sql: str) -> bool:
        """Check if SQL is read-only (no DML/DDL)."""
        first_word = sql.strip().split()[0].upper() if sql.strip() else ""
        return first_word in ("SELECT", "WITH", "EXPLAIN", "SHOW", "DESCRIBE")

    @staticmethod
    def strip_semicolons(sql: str) -> str:
        """Remove trailing semicolons (Trino JDBC doesn't accept them)."""
        return sql.strip().rstrip(";")

    @staticmethod
    def ensure_limit(sql: str, max_rows: int = 1000) -> str:
        """Add LIMIT if not present (safety for large scans)."""
        if "LIMIT" not in sql.upper():
            return f"{sql}\nLIMIT {max_rows}"
        return sql

    # ── Loaders ────────────────────────────────────────────────────────

    @staticmethod
    def _load_rules() -> str:
        """Load Trino dialect rules text."""
        if RULES_PATH.exists():
            return RULES_PATH.read_text()
        logger.warning("Trino dialect rules not found at %s", RULES_PATH)
        return ""

    @staticmethod
    def _load_taxonomy() -> dict:
        """Load the error taxonomy."""
        if TAXONOMY_PATH.exists():
            try:
                return json.loads(TAXONOMY_PATH.read_text())
            except Exception as exc:
                logger.warning("Failed to load error taxonomy: %s", exc)
        return {}
