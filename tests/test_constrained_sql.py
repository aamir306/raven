"""Tests for ConstrainedSQLGenerator."""

from __future__ import annotations

from src.raven.generation.constrained_sql import ConstrainedSQLGenerator


def test_constrain_passes_valid_sql():
    gen = ConstrainedSQLGenerator(require_limit=False)
    candidates = [
        "SELECT SUM(amount) AS total FROM cdp.sales.orders",
    ]
    result = gen.constrain(candidates, selected_tables=["cdp.sales.orders"])
    assert len(result) == 1
    assert "SUM(amount)" in result[0]


def test_constrain_rejects_ddl():
    gen = ConstrainedSQLGenerator()
    candidates = [
        "DROP TABLE cdp.sales.orders",
    ]
    result = gen.constrain(candidates, selected_tables=["cdp.sales.orders"])
    assert len(result) == 0


def test_constrain_rejects_multi_statement():
    gen = ConstrainedSQLGenerator()
    candidates = [
        "SELECT 1; DELETE FROM cdp.sales.orders",
    ]
    result = gen.constrain(candidates, selected_tables=["cdp.sales.orders"])
    assert len(result) == 0


def test_constrain_enforces_limit():
    gen = ConstrainedSQLGenerator(require_limit=True, default_limit=500)
    candidates = [
        "SELECT * FROM cdp.sales.orders",
    ]
    result = gen.constrain(candidates, selected_tables=["cdp.sales.orders"])
    assert len(result) == 1
    assert "LIMIT 500" in result[0]


def test_constrain_rejects_unknown_tables():
    gen = ConstrainedSQLGenerator(require_limit=False)
    candidates = [
        "SELECT * FROM cdp.sales.orders JOIN cdp.unknown.table ON 1=1",
    ]
    result = gen.constrain(
        candidates,
        selected_tables=["cdp.sales.orders"],
    )
    assert len(result) == 0


def test_constrain_max_candidates():
    gen = ConstrainedSQLGenerator(max_candidates=1, require_limit=False)
    candidates = [
        "SELECT 1 FROM cdp.sales.orders",
        "SELECT 2 FROM cdp.sales.orders",
        "SELECT 3 FROM cdp.sales.orders",
    ]
    result = gen.constrain(candidates, selected_tables=["cdp.sales.orders"])
    assert len(result) == 1


def test_constrain_applies_dialect_rewrites():
    gen = ConstrainedSQLGenerator(require_limit=False)
    candidates = [
        "SELECT * FROM cdp.sales.orders WHERE status != 'active' AND IFNULL(amount, 0) > 0",
    ]
    result = gen.constrain(candidates, selected_tables=["cdp.sales.orders"])
    assert len(result) == 1
    assert "<>" in result[0]
    assert "COALESCE(" in result[0]


def test_validate_structure_balanced_parens():
    gen = ConstrainedSQLGenerator()
    issues = gen.validate_structure("SELECT COUNT( FROM orders")
    assert "unbalanced_parentheses" in issues


def test_validate_structure_not_select():
    gen = ConstrainedSQLGenerator()
    issues = gen.validate_structure("UPDATE orders SET x = 1")
    assert "not_select_query" in issues
    assert "contains_ddl_or_dml" in issues


def test_apply_dialect_rewrites():
    gen = ConstrainedSQLGenerator()
    sql = "SELECT GETDATE(), NVL(x, 0) FROM t WHERE a ILIKE '%test%'"
    result = gen.apply_dialect(sql)
    assert "CURRENT_TIMESTAMP" in result
    assert "COALESCE(" in result
    assert "LIKE" in result
