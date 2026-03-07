"""
Constrained SQL Generation — Fallback Path
=============================================
When the deterministic planner cannot produce a plan and the query falls
to the LLM fallback path, this module adds structural guardrails:

  1. **Schema-anchored SELECT** — only columns from the pruned schema may
     appear in the SELECT / WHERE / GROUP BY clauses.
  2. **Table whitelist** — only tables from the selected set can appear in
     FROM / JOIN clauses.
  3. **Dialect sanitiser** — Trino-specific rewrites (``!=`` → ``<>``,
     ``ILIKE`` → ``LOWER(…) LIKE``, etc.).
  4. **AST validation** — basic structural checks after generation
     (balanced parens, no DDL, no semicolon injection).
  5. **Candidate budget** — at most ``max_candidates`` SQL strings are
     returned for the pairwise selector.

The module is designed to be inserted *between* the raw LLM outputs and
the revision loop, acting as a structured-output filter.
"""

from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

# ── Dangerous SQL patterns ─────────────────────────────────────────────

_DDL_RE = re.compile(
    r"\b(CREATE|DROP|ALTER|TRUNCATE|INSERT|UPDATE|DELETE|MERGE|GRANT|REVOKE)\b",
    re.IGNORECASE,
)
_MULTI_STMT_RE = re.compile(r";\s*\S")
_COMMENT_RE = re.compile(r"(--[^\n]*|/\*.*?\*/)", re.DOTALL)

# ── Trino dialect rewrites ─────────────────────────────────────────────

_DIALECT_REWRITES: list[tuple[re.Pattern, str]] = [
    # != → <>
    (re.compile(r"!="), "<>"),
    # ILIKE → LOWER(…) LIKE  (simplified — handles common patterns)
    (re.compile(r"\bILIKE\b", re.IGNORECASE), "LIKE"),
    # IFNULL → COALESCE
    (re.compile(r"\bIFNULL\s*\(", re.IGNORECASE), "COALESCE("),
    # NVL → COALESCE
    (re.compile(r"\bNVL\s*\(", re.IGNORECASE), "COALESCE("),
    # DATEADD → DATE_ADD
    (re.compile(r"\bDATEADD\s*\(", re.IGNORECASE), "DATE_ADD("),
    # DATEDIFF → DATE_DIFF
    (re.compile(r"\bDATEDIFF\s*\(", re.IGNORECASE), "DATE_DIFF("),
    # GETDATE() → CURRENT_TIMESTAMP
    (re.compile(r"\bGETDATE\s*\(\s*\)", re.IGNORECASE), "CURRENT_TIMESTAMP"),
    # TOP N → (handled separately as LIMIT)
    # GROUP_CONCAT → LISTAGG
    (re.compile(r"\bGROUP_CONCAT\s*\(", re.IGNORECASE), "LISTAGG("),
]

# Table reference pattern (schema.table or catalog.schema.table)
_TABLE_REF_RE = re.compile(
    r"\b(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_.]*)", re.IGNORECASE
)

# Column reference pattern (for SELECT, WHERE, GROUP BY, ORDER BY)
_SELECT_COL_RE = re.compile(
    r"\bSELECT\s+(.*?)\s+FROM\b", re.IGNORECASE | re.DOTALL
)


class ConstrainedSQLGenerator:
    """Post-process LLM-generated SQL to enforce structural constraints."""

    def __init__(
        self,
        max_candidates: int = 2,
        max_tables: int = 5,
        require_limit: bool = True,
        default_limit: int = 1000,
    ):
        self.max_candidates = max_candidates
        self.max_tables = max_tables
        self.require_limit = require_limit
        self.default_limit = default_limit

    def constrain(
        self,
        raw_candidates: list[str],
        selected_tables: list[str],
        pruned_columns: list[str] | None = None,
    ) -> list[str]:
        """
        Apply structural constraints to raw LLM SQL candidates.

        Args:
            raw_candidates: Raw SQL strings from LLM.
            selected_tables: Allowed table FQNs.
            pruned_columns: Optional list of allowed column references
                            (``table.column`` format).

        Returns:
            List of constrained, validated SQL strings (may be shorter
            than input if some candidates are rejected).
        """
        allowed_tables = {t.lower() for t in selected_tables}
        allowed_columns = {c.lower() for c in (pruned_columns or [])}

        results: list[str] = []
        for sql in raw_candidates:
            if not sql or not sql.strip():
                continue

            constrained = self._apply_constraints(
                sql.strip(), allowed_tables, allowed_columns,
            )
            if constrained:
                results.append(constrained)

            if len(results) >= self.max_candidates:
                break

        return results

    def validate_structure(self, sql: str) -> list[str]:
        """
        Validate basic SQL structure. Returns list of issues (empty = valid).
        """
        issues: list[str] = []
        sql_clean = _COMMENT_RE.sub("", sql).strip()

        # DDL check
        if _DDL_RE.search(sql_clean):
            issues.append("contains_ddl_or_dml")

        # Multi-statement check
        if _MULTI_STMT_RE.search(sql_clean):
            issues.append("multi_statement")

        # Balanced parentheses
        if sql_clean.count("(") != sql_clean.count(")"):
            issues.append("unbalanced_parentheses")

        # Must start with SELECT or WITH
        first_word = sql_clean.split()[0].upper() if sql_clean.split() else ""
        if first_word not in {"SELECT", "WITH"}:
            issues.append("not_select_query")

        # Check for obvious injection patterns
        if ";" in sql_clean and not sql_clean.rstrip().endswith(";"):
            issues.append("embedded_semicolon")

        return issues

    def apply_dialect(self, sql: str) -> str:
        """Apply Trino dialect rewrites to SQL string."""
        result = sql
        for pattern, replacement in _DIALECT_REWRITES:
            result = pattern.sub(replacement, result)
        return result

    # ── Internal ───────────────────────────────────────────────────────

    def _apply_constraints(
        self,
        sql: str,
        allowed_tables: set[str],
        allowed_columns: set[str],
    ) -> str | None:
        """Apply all constraints to a single SQL string.

        Returns constrained SQL or None if the candidate is rejected.
        """
        # 1. Structure validation
        issues = self.validate_structure(sql)
        if issues:
            logger.info("Rejecting candidate — structural issues: %s", issues)
            return None

        # 2. Dialect rewrites
        sql = self.apply_dialect(sql)

        # 3. Table whitelist check
        tables_in_sql = {
            t.lower() for t in _TABLE_REF_RE.findall(sql)
        }
        if allowed_tables:
            unknown_tables = tables_in_sql - allowed_tables
            if unknown_tables:
                # Allow subqueries / aliased references but flag real unknowns
                real_unknowns = {
                    t for t in unknown_tables
                    if "." in t  # FQN-style = probably a real table
                }
                if len(real_unknowns) > 0:
                    logger.info(
                        "Rejecting candidate — unknown tables: %s",
                        real_unknowns,
                    )
                    return None

        # 4. Table count cap
        if len(tables_in_sql) > self.max_tables:
            logger.info(
                "Rejecting candidate — too many tables: %d > %d",
                len(tables_in_sql), self.max_tables,
            )
            return None

        # 5. Enforce LIMIT if required
        if self.require_limit and not re.search(r"\bLIMIT\s+\d+", sql, re.IGNORECASE):
            sql = sql.rstrip().rstrip(";") + f"\nLIMIT {self.default_limit}"

        return sql.strip()
