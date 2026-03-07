"""Tests for sql/sqlglot_compiler.py."""

import pytest

from src.raven.sql.sqlglot_compiler import TrinoSQLCompiler, CompilationResult, HAS_SQLGLOT


@pytest.fixture
def compiler():
    return TrinoSQLCompiler()


class TestCompilationResult:
    def test_ok_when_no_errors(self):
        r = CompilationResult(sql="SELECT 1", original="SELECT 1")
        assert r.ok

    def test_not_ok_when_errors(self):
        r = CompilationResult(sql="", original="bad", errors=["bad sql"])
        assert not r.ok

    def test_not_ok_when_empty_sql(self):
        r = CompilationResult(sql="", original="")
        assert not r.ok


class TestTrinoSQLCompiler:
    def test_basic_select(self, compiler):
        result = compiler.compile("SELECT 1")
        assert result.ok
        assert "SELECT" in result.sql.upper()

    def test_simple_query(self, compiler):
        sql = "SELECT id, name FROM users WHERE id = 1"
        result = compiler.compile(sql)
        assert result.ok
        assert result.errors == []

    @pytest.mark.skipif(not HAS_SQLGLOT, reason="sqlglot not installed")
    def test_rejects_drop_table(self, compiler):
        result = compiler.compile("DROP TABLE users")
        assert not result.ok
        assert any("non_select" in e for e in result.errors)

    @pytest.mark.skipif(not HAS_SQLGLOT, reason="sqlglot not installed")
    def test_rejects_insert(self, compiler):
        result = compiler.compile("INSERT INTO users VALUES (1, 'alice')")
        assert not result.ok

    @pytest.mark.skipif(not HAS_SQLGLOT, reason="sqlglot not installed")
    def test_rejects_delete(self, compiler):
        result = compiler.compile("DELETE FROM users WHERE id = 1")
        assert not result.ok

    @pytest.mark.skipif(not HAS_SQLGLOT, reason="sqlglot not installed")
    def test_function_rewrite_ifnull(self, compiler):
        result = compiler.compile("SELECT IFNULL(name, 'unknown') FROM users")
        assert result.ok

    @pytest.mark.skipif(not HAS_SQLGLOT, reason="sqlglot not installed")
    def test_function_rewrite_nvl(self, compiler):
        result = compiler.compile("SELECT NVL(name, 'N/A') FROM users")
        assert result.ok

    @pytest.mark.skipif(not HAS_SQLGLOT, reason="sqlglot not installed")
    def test_limit_enforcement(self, compiler):
        result = compiler.compile("SELECT id FROM users")
        assert result.ok
        assert "LIMIT" in result.sql.upper()

    def test_existing_limit_preserved(self, compiler):
        result = compiler.compile("SELECT id FROM users LIMIT 10")
        assert result.ok
        assert "10" in result.sql

    @pytest.mark.skipif(not HAS_SQLGLOT, reason="sqlglot not installed")
    def test_table_allowlist_warning(self):
        comp = TrinoSQLCompiler(allowed_tables={"users", "orders"})
        result = comp.compile("SELECT id FROM secret_table")
        assert result.ok
        assert any("unknown_table" in w for w in result.warnings)

    def test_table_allowlist_known(self):
        comp = TrinoSQLCompiler(allowed_tables={"users", "orders"})
        result = comp.compile("SELECT id FROM users")
        assert result.ok

    def test_validate_returns_list(self, compiler):
        errors = compiler.validate("SELECT 1")
        assert isinstance(errors, list)

    @pytest.mark.skipif(not HAS_SQLGLOT, reason="sqlglot not installed")
    def test_validate_bad_sql(self, compiler):
        errors = compiler.validate("SELECTT 1 FROM")
        assert len(errors) > 0

    def test_transpile_to_trino(self, compiler):
        result = compiler.transpile_to_trino("SELECT NOW()", source_dialect="mysql")
        assert isinstance(result, str)
        assert len(result) > 0

    def test_is_available(self):
        assert isinstance(TrinoSQLCompiler.is_available(), bool)

    def test_empty_sql(self, compiler):
        result = compiler.compile("")
        assert not result.ok

    def test_fallback_when_no_sqlglot(self, compiler):
        if not HAS_SQLGLOT:
            result = compiler.compile("SELECT 1")
            assert result.ok
            assert any("sqlglot not installed" in w for w in result.warnings)

    def test_quote_identifier(self):
        assert TrinoSQLCompiler.quote_identifier("select") == '"select"'
        assert TrinoSQLCompiler.quote_identifier("my_col") == "my_col"
