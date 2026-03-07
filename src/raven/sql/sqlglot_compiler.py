"""
sqlglot-backed Trino SQL compiler.

Provides three capabilities the narrow hand-rolled compiler lacks:
  1. **Parse & validate** — round-trip any SQL through sqlglot to catch syntax errors
  2. **Dialect enforcement** — transpile from any dialect to Trino
  3. **AST-level transforms** — add/remove clauses, rewrite functions, enforce limits

The original ``compile_trino_sql()`` in ``trino_compiler.py`` is kept for deterministic
plan compilation.  This module adds a higher-level ``TrinoSQLCompiler`` that wraps
both planned and LLM-generated SQL through sqlglot.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any, Sequence

try:
    import sqlglot
    from sqlglot import exp, transpile
    from sqlglot.errors import ErrorLevel

    HAS_SQLGLOT = True
except ImportError:  # pragma: no cover — optional dependency
    HAS_SQLGLOT = False

from .ast_builder import QueryAst, build_query_ast
from .trino_compiler import compile_trino_sql

logger = logging.getLogger(__name__)

# Trino reserved words that must be quoted when used as identifiers
_TRINO_RESERVED = frozenset({
    "all", "alter", "and", "any", "as", "between", "by", "case", "cast",
    "column", "create", "cross", "current", "delete", "describe", "distinct",
    "drop", "else", "end", "escape", "except", "execute", "exists", "extract",
    "false", "for", "from", "full", "group", "having", "if", "in", "inner",
    "insert", "intersect", "into", "is", "join", "left", "like", "limit",
    "natural", "normalize", "not", "null", "on", "or", "order", "outer",
    "right", "select", "table", "then", "true", "union", "unnest", "update",
    "using", "values", "when", "where", "with",
})


@dataclass
class CompilationResult:
    """Result of sqlglot compilation."""

    sql: str
    original: str
    dialect_from: str = ""
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    transforms_applied: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return len(self.errors) == 0 and bool(self.sql)


class TrinoSQLCompiler:
    """sqlglot-backed Trino SQL compiler with validation and dialect transforms.

    Usage::

        compiler = TrinoSQLCompiler()
        result = compiler.compile("SELECT IFNULL(x, 0) FROM t")
        assert result.sql == 'SELECT COALESCE(x, 0) FROM t'

        # From a QueryPlan
        result = compiler.compile_plan(plan)
    """

    def __init__(
        self,
        *,
        max_limit: int = 10_000,
        default_limit: int = 1000,
        enforce_limit: bool = True,
        allowed_tables: Sequence[str] | None = None,
    ):
        self.max_limit = max_limit
        self.default_limit = default_limit
        self.enforce_limit = enforce_limit
        self.allowed_tables = (
            frozenset(t.lower() for t in allowed_tables) if allowed_tables else None
        )

    # ── Public API ─────────────────────────────────────────────────────

    def compile(
        self,
        sql: str,
        *,
        source_dialect: str = "trino",
    ) -> CompilationResult:
        """Parse, validate, transform, and emit Trino SQL.

        Args:
            sql: Input SQL (may come from any dialect if source_dialect differs)
            source_dialect: sqlglot dialect name of the input SQL

        Returns:
            CompilationResult with the Trino SQL or error details
        """
        result = CompilationResult(sql="", original=sql, dialect_from=source_dialect)

        if not HAS_SQLGLOT:
            # Graceful fallback → return input with a warning
            result.sql = sql.strip()
            result.warnings.append("sqlglot not installed — returning SQL as-is")
            return result

        # 1. Parse
        try:
            parsed = sqlglot.parse(sql, read=source_dialect, error_level=ErrorLevel.RAISE)
        except Exception as exc:
            result.errors.append(f"parse_error: {exc}")
            return result

        if not parsed:
            result.errors.append("empty_parse: no statements found")
            return result

        if len(parsed) > 1:
            result.errors.append(
                f"multi_statement: found {len(parsed)} statements, expected 1"
            )
            return result

        tree = parsed[0]

        # 2. Reject non-SELECT
        if not isinstance(tree, (exp.Select, exp.Union, exp.Intersect, exp.Except)):
            # Also allow CTEs (WITH ... SELECT)
            if isinstance(tree, exp.Subquery) or (
                hasattr(tree, "find") and tree.find(exp.Select)
            ):
                pass  # CTE or subquery — OK
            else:
                result.errors.append(
                    f"non_select: statement type {type(tree).__name__} not allowed"
                )
                return result

        # 3. Apply transforms
        tree = self._apply_transforms(tree, result)

        # 4. Emit Trino SQL
        try:
            output = tree.sql(dialect="trino", pretty=True)
            result.sql = output.strip()
        except Exception as exc:
            result.errors.append(f"emit_error: {exc}")

        return result

    def compile_plan(self, plan: Any) -> CompilationResult:
        """Compile a QueryPlan through sqlglot for validation.

        Falls back to the narrow compiler if sqlglot validation fails.
        """
        narrow_sql = compile_trino_sql(build_query_ast(plan))
        result = self.compile(narrow_sql)
        if not result.ok:
            # Narrow compiler output is trusted — use as fallback
            logger.debug(
                "sqlglot validation failed for plan SQL, using narrow compiler: %s",
                result.errors,
            )
            return CompilationResult(
                sql=narrow_sql,
                original=narrow_sql,
                warnings=[f"sqlglot_fallback: {', '.join(result.errors)}"],
            )
        return result

    def validate(self, sql: str) -> list[str]:
        """Validate SQL without transforming. Returns list of error strings."""
        result = self.compile(sql)
        return result.errors

    def transpile_to_trino(self, sql: str, source_dialect: str) -> str:
        """Transpile SQL from another dialect to Trino."""
        if not HAS_SQLGLOT:
            return sql
        try:
            results = transpile(sql, read=source_dialect, write="trino")
            return results[0] if results else sql
        except Exception:
            return sql

    # ── Transforms ─────────────────────────────────────────────────────

    def _apply_transforms(
        self,
        tree: exp.Expression,
        result: CompilationResult,
    ) -> exp.Expression:
        """Apply Trino-specific transforms to the AST."""
        tree = self._rewrite_functions(tree, result)
        tree = self._enforce_limit(tree, result)
        tree = self._check_tables(tree, result)
        return tree

    def _rewrite_functions(
        self,
        tree: exp.Expression,
        result: CompilationResult,
    ) -> exp.Expression:
        """Rewrite non-Trino functions to Trino equivalents."""
        rewrites: dict[str, str] = {
            "IFNULL": "COALESCE",
            "NVL": "COALESCE",
            "ISNULL": "COALESCE",
            "GETDATE": "CURRENT_TIMESTAMP",
            "NOW": "CURRENT_TIMESTAMP",
            "CURDATE": "CURRENT_DATE",
            "DATEADD": "DATE_ADD",
            "DATEDIFF": "DATE_DIFF",
            "LEN": "LENGTH",
            "CHARINDEX": "STRPOS",
            "SUBSTRING_INDEX": "SPLIT_PART",
            "GROUP_CONCAT": "LISTAGG",
        }

        for node in tree.walk():
            if isinstance(node, exp.Anonymous):
                func_name = node.name.upper()
                if func_name in rewrites:
                    new_name = rewrites[func_name]
                    node.set("this", new_name)
                    result.transforms_applied.append(
                        f"function_rewrite:{func_name}->{new_name}"
                    )

        return tree

    def _enforce_limit(
        self,
        tree: exp.Expression,
        result: CompilationResult,
    ) -> exp.Expression:
        """Add or cap LIMIT clause."""
        if not self.enforce_limit:
            return tree

        select = tree.find(exp.Select)
        if not select:
            return tree

        limit_node = tree.find(exp.Limit)
        if limit_node:
            # Cap existing limit
            try:
                existing = int(limit_node.expression.this)
                if existing > self.max_limit:
                    limit_node.set(
                        "expression",
                        exp.Literal.number(self.max_limit),
                    )
                    result.transforms_applied.append(
                        f"limit_capped:{existing}->{self.max_limit}"
                    )
            except (ValueError, AttributeError):
                pass
        else:
            # Add default limit
            tree = tree.limit(self.default_limit)
            result.transforms_applied.append(f"limit_added:{self.default_limit}")

        return tree

    def _check_tables(
        self,
        tree: exp.Expression,
        result: CompilationResult,
    ) -> exp.Expression:
        """Check that referenced tables are in the allowed set."""
        if not self.allowed_tables:
            return tree

        for table_node in tree.find_all(exp.Table):
            table_name = table_node.name.lower()
            full_name = ".".join(
                part
                for part in [
                    (table_node.catalog or "").lower(),
                    (table_node.db or "").lower(),
                    table_name,
                ]
                if part
            )
            if (
                table_name not in self.allowed_tables
                and full_name not in self.allowed_tables
            ):
                result.warnings.append(f"unknown_table:{full_name or table_name}")

        return tree

    # ── Static helpers ─────────────────────────────────────────────────

    @staticmethod
    def is_available() -> bool:
        """Check if sqlglot is installed."""
        return HAS_SQLGLOT

    @staticmethod
    def quote_identifier(name: str) -> str:
        """Quote a Trino identifier if it's a reserved word."""
        if name.lower() in _TRINO_RESERVED:
            return f'"{name}"'
        return name
