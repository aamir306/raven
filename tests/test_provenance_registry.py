"""Tests for query_families/provenance.py and query_families/registry.py."""

import json
import tempfile

import pytest

from src.raven.query_families.provenance import (
    FamilyProvenance,
    SlotSubstitution,
    build_provenance_from_match,
)
from src.raven.query_families.registry import (
    FamilyEntry,
    QueryFamilyRegistry,
)


# ── SlotSubstitution ──────────────────────────────────────────────────

class TestSlotSubstitution:
    def test_frozen(self):
        s = SlotSubstitution(
            slot_type="entity",
            original_value="{entity}",
            new_value="Physics",
            column_ref="subject_name",
        )
        assert s.slot_type == "entity"
        assert s.new_value == "Physics"

    def test_asdict(self):
        from dataclasses import asdict
        s = SlotSubstitution(
            slot_type="metric",
            original_value="{metric}",
            new_value="revenue",
        )
        d = asdict(s)
        assert d["slot_type"] == "metric"
        assert d["new_value"] == "revenue"


# ── FamilyProvenance ──────────────────────────────────────────────────

class TestFamilyProvenance:
    def _make(self, **overrides) -> FamilyProvenance:
        defaults = dict(
            family_key="count_by_subject",
            template_question="How many students in {entity}?",
            template_sql="SELECT COUNT(*) FROM students WHERE subject = '{entity}'",
            compiled_sql="SELECT COUNT(*) FROM students WHERE subject = 'Physics'",
            source="verified_query",
            tables_used=["students"],
            similarity_score=0.92,
            match_type="slot_substitution",
            slot_substitutions=[
                SlotSubstitution("entity", "{entity}", "Physics", "subject")
            ],
        )
        defaults.update(overrides)
        return FamilyProvenance(**defaults)

    def test_compilation_confidence_no_subs(self):
        prov = self._make(slot_substitutions=[], match_type="exact")
        assert prov.compilation_confidence == 1.0

    def test_compilation_confidence_degrades(self):
        prov = self._make()
        # 1 slot substitution -> 1.0 - 0.05 = 0.95
        assert prov.compilation_confidence == pytest.approx(0.95, abs=0.01)

    def test_compilation_confidence_many_subs(self):
        subs = [
            SlotSubstitution("entity", f"{{e{i}}}", f"val{i}")
            for i in range(15)
        ]
        prov = self._make(slot_substitutions=subs)
        assert prov.compilation_confidence >= 0.40

    def test_evidence_strength(self):
        prov = self._make()
        strength = prov.evidence_strength
        assert 0.0 < strength <= 1.0

    def test_to_dict(self):
        prov = self._make()
        d = prov.to_dict()
        assert d["family_key"] == "count_by_subject"
        assert d["match_type"] == "slot_substitution"
        assert "evidence_strength" in d
        assert "compilation_confidence" in d

    def test_source_weight(self):
        vq = self._make(source="verified_query")
        mb = self._make(source="metabase")
        # verified_query source gives 0.4 vs metabase 0.25
        assert vq.evidence_strength >= mb.evidence_strength

    def test_summary(self):
        prov = self._make()
        s = prov.summary()
        assert "count_by_subject" in s
        assert "verified_query" in s


class TestBuildProvenanceFromMatch:
    def test_from_match_dict(self):
        match_dict = {
            "family_key": "revenue_by_date",
            "question": "Total revenue on 2024-01-01",
            "template_sql": "SELECT SUM(amount) FROM orders WHERE dt = '{date}'",
            "sql": "SELECT SUM(amount) FROM orders WHERE dt = '2024-01-01'",
            "source": "verified_query",
            "similarity": 0.88,
            "tables_used": ["orders"],
            "slots": {"date": "2024-01-01"},
        }
        prov = build_provenance_from_match(match_dict)
        assert prov.family_key == "revenue_by_date"
        assert prov.source == "verified_query"
        assert prov.evidence_strength > 0
        assert prov.match_type == "slot_substitution"

    def test_from_minimal_dict(self):
        match_dict = {"family_key": "test", "sql": "SELECT 1"}
        prov = build_provenance_from_match(match_dict)
        assert prov.family_key == "test"
        assert prov.compiled_sql == "SELECT 1"

    def test_exact_match_type(self):
        match_dict = {
            "family_key": "exact_test",
            "sql": "SELECT 1",
            "similarity": 0.99,
        }
        prov = build_provenance_from_match(match_dict)
        assert prov.match_type == "exact"


# ── FamilyEntry ───────────────────────────────────────────────────────

class TestFamilyEntry:
    def test_record_hit(self):
        entry = FamilyEntry(
            family_key="test",
            template_question="q?",
            template_sql="SELECT 1",
            tables_used=["t1"],
            source="verified_queries",
        )
        entry.record_hit(success=True)
        assert entry.hit_count == 1
        assert entry.success_count == 1
        assert entry.success_rate == 1.0

        entry.record_hit(success=False)
        assert entry.hit_count == 2
        assert entry.failure_count == 1
        assert entry.success_rate == pytest.approx(0.5)

    def test_to_dict(self):
        entry = FamilyEntry(
            family_key="test",
            template_question="q?",
            template_sql="SELECT 1",
            tables_used=["t1"],
            source="verified_queries",
            category="aggregation",
            tags=["revenue"],
        )
        d = entry.to_dict()
        assert d["family_key"] == "test"
        assert d["category"] == "aggregation"


# ── QueryFamilyRegistry ──────────────────────────────────────────────

class TestQueryFamilyRegistry:
    def _make_registry(self) -> QueryFamilyRegistry:
        reg = QueryFamilyRegistry()
        reg.register(FamilyEntry(
            family_key="revenue_by_date",
            template_question="Revenue on {date}?",
            template_sql="SELECT SUM(amount) FROM orders WHERE dt = '{date}'",
            tables_used=["orders"],
            source="verified_queries",
            category="aggregation",
        ))
        reg.register(FamilyEntry(
            family_key="top_students",
            template_question="Top {N} students?",
            template_sql="SELECT name FROM students ORDER BY score DESC LIMIT {N}",
            tables_used=["students"],
            source="metabase",
            category="ranking",
        ))
        return reg

    def test_register_and_lookup(self):
        reg = self._make_registry()
        entry = reg.lookup("revenue_by_date")
        assert entry is not None
        assert entry.family_key == "revenue_by_date"

    def test_registry_keeps_multiple_entries_for_same_family_key(self):
        reg = QueryFamilyRegistry()
        reg.register(FamilyEntry(
            family_key="revenue_by_date",
            template_question="Revenue by date",
            template_sql="SELECT ds, SUM(amount) FROM orders GROUP BY ds",
            tables_used=["orders"],
            source="metabase_sync",
            metadata={"scope_key": "dashboard:1", "asset_id": "10"},
        ))
        reg.register(FamilyEntry(
            family_key="revenue_by_date",
            template_question="Revenue by date",
            template_sql="SELECT ds, SUM(net_amount) FROM revenue_daily GROUP BY ds",
            tables_used=["revenue_daily"],
            source="metabase_sync",
            metadata={"scope_key": "dashboard:2", "asset_id": "10"},
        ))

        assert reg.size == 2
        entries = reg.lookup_all("revenue_by_date")
        assert len(entries) == 2
        assert {e.metadata["scope_key"] for e in entries} == {"dashboard:1", "dashboard:2"}

    def test_lookup_missing(self):
        reg = self._make_registry()
        assert reg.lookup("nonexistent") is None

    def test_lookup_by_table(self):
        reg = self._make_registry()
        entries = reg.lookup_by_table("orders")
        assert len(entries) == 1
        assert entries[0].family_key == "revenue_by_date"

    def test_lookup_by_category(self):
        reg = self._make_registry()
        entries = reg.lookup_by_category("ranking")
        assert len(entries) == 1
        assert entries[0].family_key == "top_students"

    def test_top_families(self):
        reg = self._make_registry()
        # Record some hits
        entry = reg.lookup("revenue_by_date")
        entry.record_hit(success=True)
        entry.record_hit(success=True)
        entry2 = reg.lookup("top_students")
        entry2.record_hit(success=True)
        top = reg.top_families(limit=1)
        assert len(top) == 1
        assert top[0].family_key == "revenue_by_date"

    def test_stats(self):
        reg = self._make_registry()
        s = reg.stats()
        assert s["total_families"] == 2
        assert "by_source" in s

    def test_save_and_load(self, tmp_path):
        reg = self._make_registry()
        path = tmp_path / "registry.json"
        reg.save(str(path))
        assert path.exists()

        reg2 = QueryFamilyRegistry()
        reg2.load(str(path))
        assert reg2.lookup("revenue_by_date") is not None
        assert reg2.lookup("top_students") is not None

    def test_register_from_dict(self):
        reg = QueryFamilyRegistry()
        reg.register_from_dict({
            "family_key": "test",
            "template_question": "q?",
            "template_sql": "SELECT 1",
            "tables_used": ["t1"],
            "source": "verified_queries",
        })
        assert reg.lookup("test") is not None

    def test_low_confidence_families(self):
        reg = self._make_registry()
        entry = reg.lookup("top_students")
        # Need at least 1 hit for success_rate to be meaningful
        for _ in range(10):
            entry.record_hit(success=False)
        low = reg.low_confidence_families(min_failures=2)
        assert any(e.family_key == "top_students" for e in low)
