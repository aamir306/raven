"""
Query validator — enforces read-only SQL execution for RAVEN.

Rejects any statement that is not a SELECT / WITH / EXPLAIN / DESCRIBE / SHOW.
Uses sqlparse for robust statement-type detection.
"""

from __future__ import annotations

import re

import sqlparse

# Whitelist of allowed first keywords (read-only operations)
_ALLOWED_FIRST_KEYWORDS = {"SELECT", "WITH", "EXPLAIN", "DESCRIBE", "SHOW", "VALUES"}

# Blacklist patterns — catch edge cases sqlparse might miss
_BLACKLIST_RE = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|MERGE|GRANT|REVOKE|REPLACE)\b",
    re.IGNORECASE,
)


def validate_read_only(sql: str) -> bool:
    """Return True if the SQL is a safe read-only statement.

    Checks:
    1. sqlparse statement type detection
    2. Regex blacklist for dangerous keywords
    3. Rejects multi-statement inputs (semicolons producing >1 statement)
    """
    if not sql or not sql.strip():
        return False

    stripped = sql.strip().rstrip(";")

    # Reject multi-statement
    statements = sqlparse.parse(stripped)
    if len(statements) != 1:
        return False

    stmt = statements[0]
    first_token = stmt.token_first(skip_cm=True, skip_ws=True)
    if first_token is None:
        return False

    first_keyword = first_token.ttype is sqlparse.tokens.Keyword or first_token.ttype in (
        sqlparse.tokens.Keyword.DML,
        sqlparse.tokens.Keyword.DDL,
        sqlparse.tokens.CTE,
    )

    keyword_value = first_token.normalized.upper() if first_token else ""

    # WITH … SELECT is fine
    if keyword_value == "WITH":
        # Extra check: ensure no DML hides inside
        if _BLACKLIST_RE.search(stripped):
            # Check if the blacklisted word is actually part of a column/table name
            # by looking at context — but to be safe, reject common DML combos
            for match in _BLACKLIST_RE.finditer(stripped):
                word = match.group(0).upper()
                if word in {"INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE", "TRUNCATE", "MERGE"}:
                    return False
        return True

    if keyword_value in _ALLOWED_FIRST_KEYWORDS:
        return True

    return False


def validate_no_injection(sql: str) -> bool:
    """Basic SQL injection detection — reject suspicious patterns."""
    suspicious_patterns = [
        r";\s*(DROP|DELETE|INSERT|UPDATE|ALTER|CREATE)",  # statement stacking
        r"--.*$",  # single-line comments (could be legit but flag)
        r"/\*.*?\*/",  # block comments
        r"xp_cmdshell",  # SQL Server-specific but catch-all
        r"UNION\s+ALL\s+SELECT\s+NULL",  # classic injection probe
    ]
    for pattern in suspicious_patterns:
        if re.search(pattern, sql, re.IGNORECASE | re.DOTALL):
            return False
    return True
