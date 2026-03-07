"""Tests for ValueIndex and AmbiguityPolicy."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

from src.raven.grounding.ambiguity_policy import AmbiguityDecision, AmbiguityPolicy
from src.raven.grounding.value_index import ValueIndex, ValueLocation


# ── ValueIndex tests ───────────────────────────────────────────────────


def test_value_index_add_and_lookup():
    idx = ValueIndex(index_path="/nonexistent/path.json")
    idx.add("active", "cdp.ops.batches", "status", count=4200)
    idx.add("active", "cdp.crm.users", "status", count=180000)

    results = idx.lookup("active")
    assert len(results) == 2
    # Sorted by count descending
    assert results[0].table == "cdp.crm.users"
    assert results[0].count == 180000


def test_value_index_lookup_with_preferred_tables():
    idx = ValueIndex(index_path="/nonexistent/path.json")
    idx.add("active", "cdp.ops.batches", "status", count=100)
    idx.add("active", "cdp.crm.users", "status", count=180000)

    results = idx.lookup("active", preferred_tables=["cdp.ops.batches"])
    assert len(results) == 2
    # Preferred table should come first despite lower count
    assert results[0].table == "cdp.ops.batches"


def test_value_index_search_substring():
    idx = ValueIndex(index_path="/nonexistent/path.json")
    idx.add("active", "cdp.ops.batches", "status", count=100)
    idx.add("inactive", "cdp.ops.batches", "status", count=50)
    idx.add("physics", "cdp.lms.courses", "subject", count=32)

    results = idx.search("active")
    assert len(results) == 2  # "active" and "inactive"
    # Exact match should be first
    assert results[0][0] == "active"


def test_value_index_is_ambiguous():
    idx = ValueIndex(index_path="/nonexistent/path.json")
    idx.add("active", "cdp.ops.batches", "status", count=100)
    idx.add("active", "cdp.crm.users", "status", count=200)

    assert idx.is_ambiguous("active") is True


def test_value_index_not_ambiguous_single_table():
    idx = ValueIndex(index_path="/nonexistent/path.json")
    idx.add("active", "cdp.ops.batches", "status", count=100)
    idx.add("active", "cdp.ops.batches", "state", count=50)

    # Same table, different columns — not ambiguous at table level
    assert idx.is_ambiguous("active") is False


def test_value_index_disambiguation_candidates():
    idx = ValueIndex(index_path="/nonexistent/path.json")
    idx.add("active", "cdp.ops.batches", "status", count=100)
    idx.add("active", "cdp.crm.users", "status", count=200)

    candidates = idx.disambiguation_candidates("active")
    assert len(candidates) == 2
    assert all("description" in c for c in candidates)


def test_value_index_save_and_load():
    idx = ValueIndex(index_path="/nonexistent/path.json")
    idx.add("test_value", "cdp.test.table", "col1", count=42)

    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        idx.save(f.name)
        loaded = ValueIndex(index_path=f.name)
        results = loaded.lookup("test_value")
        assert len(results) == 1
        assert results[0].table == "cdp.test.table"
        assert results[0].count == 42

    Path(f.name).unlink(missing_ok=True)


def test_value_index_normalisation():
    idx = ValueIndex(index_path="/nonexistent/path.json")
    idx.add("  Active  ", "cdp.ops.batches", "status", count=100)

    # Lookup with different casing/whitespace should match
    results = idx.lookup("active")
    assert len(results) == 1


# ── AmbiguityPolicy tests ─────────────────────────────────────────────


def test_ambiguity_policy_no_ambiguities():
    policy = AmbiguityPolicy()
    decision = policy.evaluate([])
    assert decision.action == "pick_best"
    assert decision.reason == "no_ambiguities"


def test_ambiguity_policy_too_many_ambiguities():
    policy = AmbiguityPolicy()
    ambiguities = [
        {"type": "value_match", "table": f"t{i}", "column": "c", "candidates": ["a", "b"]}
        for i in range(5)
    ]
    decision = policy.evaluate(ambiguities)
    assert decision.action == "abstain"
    assert "too_many_ambiguities" in decision.reason


def test_ambiguity_policy_single_value_ambiguity_clarifies():
    policy = AmbiguityPolicy()
    ambiguities = [
        {
            "type": "value_match",
            "table": "cdp.ops.batches",
            "column": "status",
            "candidates": ["active", "inactive"],
        }
    ]
    decision = policy.evaluate(ambiguities)
    assert decision.action == "clarify"
    assert len(decision.suggestions) > 0


def test_ambiguity_policy_weak_ambiguity_proceeds():
    policy = AmbiguityPolicy()
    ambiguities = [
        {
            "type": "metabase_filter_reference",
            "filter": "status",
            "source": "metabase",
        }
    ]
    decision = policy.evaluate(ambiguities)
    assert decision.action == "pick_best"
    assert decision.reason == "weak_ambiguities_only"


def test_ambiguity_policy_focus_resolves():
    policy = AmbiguityPolicy()

    class FakeFocus:
        tables = ["cdp.ops.batches"]

    ambiguities = [
        {
            "type": "value_match",
            "table": "cdp.ops.batches",
            "column": "status",
            "candidates": ["active", "inactive"],
        }
    ]
    decision = policy.evaluate(ambiguities, focus=FakeFocus())
    assert decision.action == "pick_best"
    assert decision.reason == "focus_scoped_resolution"


def test_ambiguity_policy_evaluate_grounding_result():
    policy = AmbiguityPolicy()
    grounding = {
        "ambiguities": [],
        "filters": [{"column": "status", "value": "active"}],
    }
    decision = policy.evaluate_grounding_result(grounding)
    assert decision.action == "pick_best"
