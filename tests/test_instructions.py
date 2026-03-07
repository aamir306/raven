"""Tests for contracts/instructions.py and contracts/instruction_compiler.py."""

import pytest

from src.raven.contracts.instructions import (
    Instruction,
    InstructionAction,
    InstructionCondition,
    InstructionScope,
    InstructionSet,
)
from src.raven.contracts.instruction_compiler import InstructionCompiler


# ── InstructionCondition tests ─────────────────────────────────────────

class TestInstructionCondition:
    def test_empty_condition_matches_everything(self):
        cond = InstructionCondition()
        assert cond.matches(tables=["t1"], metrics=["m1"])
        assert cond.is_unconditional()

    def test_table_condition(self):
        cond = InstructionCondition(tables=frozenset(["orders"]))
        assert cond.matches(tables=["orders", "users"])
        assert not cond.matches(tables=["users"])

    def test_metric_condition(self):
        cond = InstructionCondition(metrics=frozenset(["revenue"]))
        assert cond.matches(metrics=["revenue"])
        assert not cond.matches(metrics=["count"])

    def test_intent_condition(self):
        cond = InstructionCondition(intents=frozenset(["KPI", "TOP_K"]))
        assert cond.matches(intent="KPI")
        assert cond.matches(intent="top_k")
        assert not cond.matches(intent="TIME_SERIES")

    def test_question_pattern(self):
        cond = InstructionCondition(question_pattern=r"\brevenue\b")
        assert cond.matches(question="What is total revenue?")
        assert not cond.matches(question="How many students?")

    def test_combined_conditions(self):
        cond = InstructionCondition(
            tables=frozenset(["orders"]),
            metrics=frozenset(["revenue"]),
        )
        assert cond.matches(tables=["orders"], metrics=["revenue"])
        assert not cond.matches(tables=["orders"], metrics=["count"])
        assert not cond.matches(tables=["users"], metrics=["revenue"])


# ── Instruction tests ──────────────────────────────────────────────────

class TestInstruction:
    def test_to_dict(self):
        inst = Instruction(
            id="test-1",
            name="prefer_orders",
            description="Use orders table for revenue",
            scope=InstructionScope.TABLE,
            action=InstructionAction.PREFER,
            condition=InstructionCondition(),
            prefer_tables=("gold_orders",),
        )
        d = inst.to_dict()
        assert d["id"] == "test-1"
        assert d["scope"] == "table"
        assert d["action"] == "prefer"
        assert d["prefer_tables"] == ["gold_orders"]


# ── InstructionSet tests ──────────────────────────────────────────────

class TestInstructionSet:
    def _make_set(self) -> InstructionSet:
        instructions = [
            Instruction(
                id="g1",
                name="always_limit",
                description="Always add LIMIT",
                scope=InstructionScope.GLOBAL,
                action=InstructionAction.GUARDRAIL,
                condition=InstructionCondition(),
                guardrail_sql="LIMIT 1000",
                priority=10,
            ),
            Instruction(
                id="t1",
                name="prefer_gold_orders",
                description="Prefer gold_orders for revenue",
                scope=InstructionScope.TABLE,
                action=InstructionAction.PREFER,
                condition=InstructionCondition(
                    tables=frozenset(["gold_orders"]),
                    metrics=frozenset(["revenue"]),
                ),
                prefer_tables=("cdp.cdp_revenue.gold_orders",),
                priority=50,
            ),
            Instruction(
                id="m1",
                name="revenue_note",
                description="Revenue excludes refunds",
                scope=InstructionScope.METRIC,
                action=InstructionAction.NOTE,
                condition=InstructionCondition(metrics=frozenset(["revenue"])),
                note_text="Revenue excludes refunds unless specified",
                priority=100,
            ),
        ]
        iset = InstructionSet(instructions=instructions)
        iset.rebuild_indexes()
        return iset

    def test_query_global(self):
        iset = self._make_set()
        matches = iset.query(tables=["users"])
        assert any(m.id == "g1" for m in matches)

    def test_query_table_scoped(self):
        iset = self._make_set()
        matches = iset.query(tables=["gold_orders"], metrics=["revenue"])
        ids = {m.id for m in matches}
        assert "g1" in ids  # global always matches
        assert "t1" in ids  # table condition matches
        assert "m1" in ids  # metric condition matches

    def test_query_no_match(self):
        iset = self._make_set()
        matches = iset.query(tables=["users"], metrics=["count"])
        ids = {m.id for m in matches}
        assert "g1" in ids
        assert "t1" not in ids
        assert "m1" not in ids

    def test_prefer_tables(self):
        iset = self._make_set()
        preferred = iset.prefer_tables_for(
            tables=["gold_orders"], metrics=["revenue"]
        )
        assert "cdp.cdp_revenue.gold_orders" in preferred

    def test_notes_for(self):
        iset = self._make_set()
        notes = iset.notes_for(metrics=["revenue"])
        assert any("refunds" in n for n in notes)

    def test_len(self):
        iset = self._make_set()
        assert len(iset) == 3


# ── InstructionCompiler tests ──────────────────────────────────────────

class TestInstructionCompiler:
    def test_compile_structured(self):
        compiler = InstructionCompiler()
        rules = [
            {
                "name": "prefer_orders",
                "scope": "table",
                "action": "prefer",
                "description": "Use orders for revenue",
                "prefer_tables": ["gold_orders"],
                "condition": {"metrics": ["revenue"]},
                "priority": 50,
            }
        ]
        iset = compiler.compile(rules)
        assert len(iset) == 1
        inst = iset.instructions[0]
        assert inst.action == InstructionAction.PREFER
        assert inst.scope == InstructionScope.TABLE
        assert "gold_orders" in inst.prefer_tables

    def test_compile_legacy_business_rule(self):
        compiler = InstructionCompiler()
        rules = [
            {
                "term": "active_batch",
                "definition": "Always filter by is_active = true when querying batches",
                "sql_fragment": "is_active = true",
                "synonyms": ["active batch", "live batch"],
                "categories": ["batch"],
                "rule_type": "filter",
            }
        ]
        iset = compiler.compile(rules, source_file="semantic_model")
        assert len(iset) == 1
        inst = iset.instructions[0]
        assert inst.name == "active_batch"
        assert "is_active = true" in inst.sql_fragment

    def test_compile_legacy_avoid(self):
        compiler = InstructionCompiler()
        rules = [
            {
                "term": "staging_tables",
                "definition": "Avoid using staging tables for production queries",
                "sql_fragment": "",
                "synonyms": [],
                "categories": [],
                "rule_type": "constraint",
            }
        ]
        iset = compiler.compile(rules)
        assert len(iset) == 1
        assert iset.instructions[0].action == InstructionAction.AVOID

    def test_compile_empty(self):
        compiler = InstructionCompiler()
        iset = compiler.compile([])
        assert len(iset) == 0

    def test_compile_and_merge(self):
        compiler = InstructionCompiler()
        list1 = [{"term": "rule1", "definition": "First rule", "sql_fragment": ""}]
        list2 = [{"term": "rule2", "definition": "Must always filter", "sql_fragment": "x = 1"}]
        iset = compiler.compile_and_merge(list1, list2, source_files=["a.yaml", "b.yaml"])
        assert len(iset) == 2

    def test_compile_skips_empty_rules(self):
        compiler = InstructionCompiler()
        rules = [{"term": "", "definition": ""}]
        iset = compiler.compile(rules)
        assert len(iset) == 0
