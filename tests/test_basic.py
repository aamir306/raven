"""
RAVEN — Basic Integration Tests
=================================
Tests for connectors, safety module, and pipeline initialization.
Run with: pytest tests/ -v
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ── Path constants ─────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONFIG_DIR = PROJECT_ROOT / "config"
PROMPTS_DIR = PROJECT_ROOT / "prompts"


# ── Test: File structure ──────────────────────────────────────────────


class TestProjectStructure:
    """Verify all required files exist."""

    def test_config_files_exist(self):
        assert (CONFIG_DIR / "settings.yaml").exists()
        assert (CONFIG_DIR / "model_routing.yaml").exists()
        assert (CONFIG_DIR / "cost_guards.yaml").exists()
        assert (CONFIG_DIR / "error_taxonomy.json").exists()
        assert (CONFIG_DIR / "trino_dialect_rules.txt").exists()
        assert (CONFIG_DIR / "semantic_model.yaml").exists()

    def test_prompt_templates_exist(self):
        expected = [
            "router_classify.txt",
            "ir_keyword_extract.txt",
            "ss_column_filter.txt",
            "ss_table_select.txt",
            "ss_column_prune.txt",
            "probe_decompose.txt",
            "probe_generate.txt",
            "gen_divide_conquer.txt",
            "gen_execution_plan.txt",
            "gen_fewshot.txt",
            "gen_revision.txt",
            "val_pairwise_compare.txt",
            "val_error_taxonomy.txt",
            "out_chart_detect.txt",
            "out_nl_summary.txt",
            "trino_dialect_rules.txt",
        ]
        for name in expected:
            path = PROMPTS_DIR / name
            assert path.exists(), f"Missing prompt template: {name}"

    def test_prompt_templates_have_placeholders(self):
        # trino_dialect_rules.txt is a static reference file — no placeholders
        skip = {"trino_dialect_rules.txt"}
        for path in PROMPTS_DIR.glob("*.txt"):
            if path.name in skip:
                continue
            content = path.read_text()
            assert "{" in content, f"Prompt {path.name} has no placeholders"

    def test_package_structure(self):
        src = PROJECT_ROOT / "src" / "raven"
        packages = [
            "", "connectors", "safety", "router", "retrieval",
            "schema", "probes", "generation", "validation",
            "output", "feedback",
        ]
        for pkg in packages:
            init = src / pkg / "__init__.py" if pkg else src / "__init__.py"
            assert init.exists(), f"Missing __init__.py for package: {pkg or 'raven'}"

    def test_error_taxonomy_valid_json(self):
        path = CONFIG_DIR / "error_taxonomy.json"
        data = json.loads(path.read_text())
        assert isinstance(data, dict)
        assert len(data) > 0


# ── Test: Safety Module ───────────────────────────────────────────────


class TestQueryValidator:
    """Test read-only SQL validation."""

    def test_select_allowed(self):
        from src.raven.safety.query_validator import validate_read_only
        assert validate_read_only("SELECT * FROM users") is True

    def test_with_cte_allowed(self):
        from src.raven.safety.query_validator import validate_read_only
        sql = "WITH cte AS (SELECT 1) SELECT * FROM cte"
        assert validate_read_only(sql) is True

    def test_insert_blocked(self):
        from src.raven.safety.query_validator import validate_read_only
        assert validate_read_only("INSERT INTO users VALUES (1)") is False

    def test_drop_blocked(self):
        from src.raven.safety.query_validator import validate_read_only
        assert validate_read_only("DROP TABLE users") is False

    def test_delete_blocked(self):
        from src.raven.safety.query_validator import validate_read_only
        assert validate_read_only("DELETE FROM users WHERE id = 1") is False

    def test_update_blocked(self):
        from src.raven.safety.query_validator import validate_read_only
        assert validate_read_only("UPDATE users SET name = 'x'") is False

    def test_explain_allowed(self):
        from src.raven.safety.query_validator import validate_read_only
        assert validate_read_only("EXPLAIN SELECT * FROM users") is True

    def test_injection_detection(self):
        from src.raven.safety.query_validator import validate_no_injection
        assert validate_no_injection("SELECT * FROM users; DROP TABLE users;") is False


class TestDataPolicy:
    """Test data policy checks."""

    def test_clean_prompt_passes(self):
        from src.raven.safety.data_policy import is_safe_for_api
        assert is_safe_for_api("SELECT count(*) FROM gold.users") is True

    def test_email_detected(self):
        from src.raven.safety.data_policy import check_prompt
        result = check_prompt("Find user with email john@example.com")
        assert len(result) > 0

    def test_ip_detected(self):
        from src.raven.safety.data_policy import check_prompt
        result = check_prompt("Check IP 192.168.1.1 in logs")
        assert len(result) > 0


# ── Test: Router ──────────────────────────────────────────────────────


class TestDifficultyClassifier:
    """Test router classification."""

    @pytest.mark.asyncio
    async def test_classify_returns_difficulty(self):
        from src.raven.router.classifier import DifficultyClassifier, Difficulty

        mock_openai = AsyncMock()
        mock_openai.complete = AsyncMock(return_value="SIMPLE")

        classifier = DifficultyClassifier(mock_openai)
        result = await classifier.classify("How many users yesterday?")
        assert result == Difficulty.SIMPLE

    @pytest.mark.asyncio
    async def test_classify_complex(self):
        from src.raven.router.classifier import DifficultyClassifier, Difficulty

        mock_openai = AsyncMock()
        mock_openai.complete = AsyncMock(return_value="COMPLEX")

        classifier = DifficultyClassifier(mock_openai)
        result = await classifier.classify("Weekly retention by acquisition channel")
        assert result == Difficulty.COMPLEX

    @pytest.mark.asyncio
    async def test_classify_ambiguous(self):
        from src.raven.router.classifier import DifficultyClassifier, Difficulty

        mock_openai = AsyncMock()
        mock_openai.complete = AsyncMock(return_value="AMBIGUOUS")

        classifier = DifficultyClassifier(mock_openai)
        result = await classifier.classify("Show me the data")
        assert result == Difficulty.AMBIGUOUS

    @pytest.mark.asyncio
    async def test_classify_fallback_to_complex(self):
        from src.raven.router.classifier import DifficultyClassifier, Difficulty

        mock_openai = AsyncMock()
        mock_openai.complete = AsyncMock(return_value="UNCLEAR OUTPUT")

        classifier = DifficultyClassifier(mock_openai)
        result = await classifier.classify("Something")
        assert result == Difficulty.COMPLEX


# ── Test: SQL Extraction ──────────────────────────────────────────────


class TestSQLExtraction:
    """Test SQL extraction from LLM responses."""

    def test_extract_from_code_block(self):
        from src.raven.generation.sql_generator import SQLGenerator

        response = "Here's the SQL:\n```sql\nSELECT * FROM users\n```"
        result = SQLGenerator._extract_sql(response)
        assert result == "SELECT * FROM users"

    def test_extract_bare_sql(self):
        from src.raven.generation.sql_generator import SQLGenerator

        response = "SELECT count(*) FROM orders WHERE ds = CURRENT_DATE"
        result = SQLGenerator._extract_sql(response)
        assert "SELECT count" in result

    def test_extract_with_cte(self):
        from src.raven.generation.sql_generator import SQLGenerator

        response = "WITH cte AS (\n  SELECT 1\n)\nSELECT * FROM cte"
        result = SQLGenerator._extract_sql(response)
        assert "WITH cte" in result


# ── Test: API Models ──────────────────────────────────────────────────


class TestAPIModels:
    """Test Pydantic model validation."""

    def test_query_request_valid(self):
        from web.routes import QueryRequest
        req = QueryRequest(question="How many users?")
        assert req.question == "How many users?"

    def test_query_request_empty_rejected(self):
        from web.routes import QueryRequest
        with pytest.raises(Exception):
            QueryRequest(question="")

    def test_feedback_request_valid(self):
        from web.routes import FeedbackRequest
        req = FeedbackRequest(query_id="abc-123", feedback="thumbs_up")
        assert req.feedback == "thumbs_up"

    def test_feedback_request_invalid_type(self):
        from web.routes import FeedbackRequest
        with pytest.raises(Exception):
            FeedbackRequest(query_id="abc", feedback="invalid")
