"""
Typed instruction assets for semantic domain packs.

Instructions are compiled policy objects that encode domain-specific rules
for how queries should be built, which tables to prefer/avoid, metric
derivation formulas, terminology mappings, and guardrails.

Unlike raw business_rules (unstructured text), compiled instructions have:
- explicit scope (global, per-table, per-metric)
- typed actions (prefer, avoid, require, rewrite, guardrail)
- condition predicates for when they apply
- priority ordering for conflict resolution
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Sequence


class InstructionScope(str, Enum):
    """Where an instruction applies."""

    GLOBAL = "global"
    TABLE = "table"
    METRIC = "metric"
    DIMENSION = "dimension"
    INTENT = "intent"


class InstructionAction(str, Enum):
    """What an instruction prescribes."""

    PREFER = "prefer"  # Soft preference: use X when possible
    AVOID = "avoid"  # Soft negative: do not use X unless required
    REQUIRE = "require"  # Hard rule: this must hold
    REWRITE = "rewrite"  # Transform: replace X with Y
    GUARDRAIL = "guardrail"  # Constraint: add WHERE/LIMIT/etc.
    NOTE = "note"  # Advisory: show in context but no enforcement


@dataclass(frozen=True)
class InstructionCondition:
    """Predicate that determines when an instruction activates.

    All non-empty fields must match for the instruction to fire.
    Empty fields are treated as "match anything".
    """

    tables: frozenset[str] = frozenset()
    metrics: frozenset[str] = frozenset()
    dimensions: frozenset[str] = frozenset()
    intents: frozenset[str] = frozenset()
    question_pattern: str = ""  # Regex matched against normalized question

    def matches(
        self,
        *,
        tables: Sequence[str] = (),
        metrics: Sequence[str] = (),
        dimensions: Sequence[str] = (),
        intent: str = "",
        question: str = "",
    ) -> bool:
        """Return True if all non-empty conditions are satisfied."""
        if self.tables and not (self.tables & frozenset(tables)):
            return False
        if self.metrics and not (self.metrics & frozenset(metrics)):
            return False
        if self.dimensions and not (self.dimensions & frozenset(dimensions)):
            return False
        if self.intents and intent and intent.upper() not in {i.upper() for i in self.intents}:
            return False
        if self.question_pattern:
            try:
                if not re.search(self.question_pattern, question, re.IGNORECASE):
                    return False
            except re.error:
                return False
        return True

    def is_unconditional(self) -> bool:
        return (
            not self.tables
            and not self.metrics
            and not self.dimensions
            and not self.intents
            and not self.question_pattern
        )


@dataclass(frozen=True)
class Instruction:
    """A single compiled instruction policy object."""

    id: str
    name: str
    description: str
    scope: InstructionScope
    action: InstructionAction
    condition: InstructionCondition
    priority: int = 100  # Lower = higher priority (applied first)

    # Action-specific payloads
    prefer_tables: tuple[str, ...] = ()
    avoid_tables: tuple[str, ...] = ()
    require_filter: str = ""  # SQL fragment for WHERE clause
    rewrite_from: str = ""  # Pattern to replace
    rewrite_to: str = ""  # Replacement text
    guardrail_sql: str = ""  # SQL clause to append/enforce
    sql_fragment: str = ""  # General SQL hint
    note_text: str = ""  # Human-readable advisory

    # Provenance
    source_file: str = ""
    source_rule: str = ""  # Original business_rule term

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "scope": self.scope.value,
            "action": self.action.value,
            "priority": self.priority,
        }
        if self.prefer_tables:
            result["prefer_tables"] = list(self.prefer_tables)
        if self.avoid_tables:
            result["avoid_tables"] = list(self.avoid_tables)
        if self.require_filter:
            result["require_filter"] = self.require_filter
        if self.rewrite_from:
            result["rewrite_from"] = self.rewrite_from
            result["rewrite_to"] = self.rewrite_to
        if self.guardrail_sql:
            result["guardrail_sql"] = self.guardrail_sql
        if self.sql_fragment:
            result["sql_fragment"] = self.sql_fragment
        if self.note_text:
            result["note_text"] = self.note_text
        if self.source_file:
            result["source_file"] = self.source_file
        return result


@dataclass
class InstructionSet:
    """Collection of compiled instructions with query-time lookup."""

    instructions: list[Instruction] = field(default_factory=list)

    # –– Indexes built at load time ––
    _by_scope: dict[InstructionScope, list[Instruction]] = field(
        default_factory=dict, repr=False
    )
    _by_table: dict[str, list[Instruction]] = field(
        default_factory=dict, repr=False
    )
    _global: list[Instruction] = field(default_factory=list, repr=False)

    def rebuild_indexes(self) -> None:
        """Must be called after instructions list is modified."""
        self._by_scope.clear()
        self._by_table.clear()
        self._global.clear()

        for inst in sorted(self.instructions, key=lambda i: i.priority):
            self._by_scope.setdefault(inst.scope, []).append(inst)
            if inst.scope == InstructionScope.GLOBAL:
                self._global.append(inst)
            for table in inst.condition.tables:
                self._by_table.setdefault(table.lower(), []).append(inst)
            for table in inst.prefer_tables:
                self._by_table.setdefault(table.lower(), []).append(inst)
            for table in inst.avoid_tables:
                self._by_table.setdefault(table.lower(), []).append(inst)

    def query(
        self,
        *,
        tables: Sequence[str] = (),
        metrics: Sequence[str] = (),
        dimensions: Sequence[str] = (),
        intent: str = "",
        question: str = "",
    ) -> list[Instruction]:
        """Return all instructions matching the given context, sorted by priority."""
        candidates: set[str] = set()
        matches: list[Instruction] = []

        for inst in self._global:
            if inst.id not in candidates and inst.condition.matches(
                tables=tables,
                metrics=metrics,
                dimensions=dimensions,
                intent=intent,
                question=question,
            ):
                candidates.add(inst.id)
                matches.append(inst)

        for table in tables:
            for inst in self._by_table.get(table.lower(), []):
                if inst.id not in candidates and inst.condition.matches(
                    tables=tables,
                    metrics=metrics,
                    dimensions=dimensions,
                    intent=intent,
                    question=question,
                ):
                    candidates.add(inst.id)
                    matches.append(inst)

        # Catch any indexed-by-scope instructions not yet matched
        for scope in (InstructionScope.METRIC, InstructionScope.DIMENSION, InstructionScope.INTENT):
            for inst in self._by_scope.get(scope, []):
                if inst.id not in candidates and inst.condition.matches(
                    tables=tables,
                    metrics=metrics,
                    dimensions=dimensions,
                    intent=intent,
                    question=question,
                ):
                    candidates.add(inst.id)
                    matches.append(inst)

        matches.sort(key=lambda i: i.priority)
        return matches

    def prefer_tables_for(
        self,
        *,
        tables: Sequence[str] = (),
        metrics: Sequence[str] = (),
        question: str = "",
    ) -> list[str]:
        """Return table names that matching PREFER instructions suggest."""
        result: list[str] = []
        for inst in self.query(tables=tables, metrics=metrics, question=question):
            if inst.action == InstructionAction.PREFER and inst.prefer_tables:
                result.extend(inst.prefer_tables)
        return list(dict.fromkeys(result))

    def avoid_tables_for(
        self,
        *,
        tables: Sequence[str] = (),
        metrics: Sequence[str] = (),
        question: str = "",
    ) -> list[str]:
        """Return table names that matching AVOID instructions suggest skipping."""
        result: list[str] = []
        for inst in self.query(tables=tables, metrics=metrics, question=question):
            if inst.action == InstructionAction.AVOID and inst.avoid_tables:
                result.extend(inst.avoid_tables)
        return list(dict.fromkeys(result))

    def required_filters_for(
        self,
        *,
        tables: Sequence[str] = (),
        metrics: Sequence[str] = (),
        intent: str = "",
        question: str = "",
    ) -> list[str]:
        """Return SQL WHERE fragments that REQUIRE instructions mandate."""
        result: list[str] = []
        for inst in self.query(
            tables=tables, metrics=metrics, intent=intent, question=question,
        ):
            if inst.action == InstructionAction.REQUIRE and inst.require_filter:
                result.append(inst.require_filter)
        return result

    def rewrites_for(
        self,
        *,
        tables: Sequence[str] = (),
        metrics: Sequence[str] = (),
        question: str = "",
    ) -> list[tuple[str, str]]:
        """Return (from_pattern, to_replacement) pairs from REWRITE instructions."""
        result: list[tuple[str, str]] = []
        for inst in self.query(tables=tables, metrics=metrics, question=question):
            if inst.action == InstructionAction.REWRITE and inst.rewrite_from:
                result.append((inst.rewrite_from, inst.rewrite_to))
        return result

    def notes_for(
        self,
        *,
        tables: Sequence[str] = (),
        metrics: Sequence[str] = (),
        question: str = "",
    ) -> list[str]:
        """Return advisory note texts from matching NOTE instructions."""
        result: list[str] = []
        for inst in self.query(tables=tables, metrics=metrics, question=question):
            if inst.action == InstructionAction.NOTE and inst.note_text:
                result.append(inst.note_text)
            elif inst.description:
                result.append(inst.description)
        return result

    def __len__(self) -> int:
        return len(self.instructions)
