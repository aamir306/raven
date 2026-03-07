"""
Compile raw instruction YAML / business_rules dicts into typed Instruction objects.

The compiler handles two input formats:
1. Legacy business_rules from ContractBundle (list[dict] with term/definition/sql_fragment)
2. Structured instruction YAML (list[dict] with name/scope/action/condition/payloads)

Both are normalized into Instruction objects stored in an InstructionSet.
"""

from __future__ import annotations

import hashlib
import logging
import re
from typing import Any, Sequence

from .instructions import (
    Instruction,
    InstructionAction,
    InstructionCondition,
    InstructionScope,
    InstructionSet,
)

logger = logging.getLogger(__name__)

# ── Heuristic patterns for legacy business_rules → typed instructions ──

_TABLE_PATTERN = re.compile(
    r"\b(?:use|prefer|from|table|join)\s+[`'\"]?(\w+\.\w+(?:\.\w+)?)[`'\"]?",
    re.IGNORECASE,
)
_AVOID_PATTERN = re.compile(
    r"\b(?:avoid|don't use|do not use|never use|skip)\b",
    re.IGNORECASE,
)
_REQUIRE_PATTERN = re.compile(
    r"\b(?:must|always|require|mandatory|filter by|where)\b",
    re.IGNORECASE,
)
_REWRITE_PATTERN = re.compile(
    r"\b(?:replace|rewrite|instead of|map to|translate)\b",
    re.IGNORECASE,
)
_METRIC_PATTERN = re.compile(
    r"\b(?:metric|measure|kpi|aggregate|sum|count|avg|average)\b",
    re.IGNORECASE,
)
_DIMENSION_PATTERN = re.compile(
    r"\b(?:dimension|group by|breakdown|segment|category)\b",
    re.IGNORECASE,
)


def _stable_id(text: str) -> str:
    """Generate a stable short ID from text content."""
    return hashlib.sha256(text.encode()).hexdigest()[:12]


def _infer_scope(raw: dict[str, Any]) -> InstructionScope:
    """Infer scope from raw rule content."""
    scope_str = str(raw.get("scope", "")).lower()
    for s in InstructionScope:
        if scope_str == s.value:
            return s

    text = f"{raw.get('term', '')} {raw.get('definition', '')} {raw.get('description', '')}"
    if _METRIC_PATTERN.search(text):
        return InstructionScope.METRIC
    if _DIMENSION_PATTERN.search(text):
        return InstructionScope.DIMENSION
    if _TABLE_PATTERN.search(text):
        return InstructionScope.TABLE
    return InstructionScope.GLOBAL


def _infer_action(raw: dict[str, Any]) -> InstructionAction:
    """Infer action from raw rule content."""
    action_str = str(raw.get("action", "")).lower()
    for a in InstructionAction:
        if action_str == a.value:
            return a

    text = f"{raw.get('term', '')} {raw.get('definition', '')} {raw.get('description', '')}"
    if _AVOID_PATTERN.search(text):
        return InstructionAction.AVOID
    if _REQUIRE_PATTERN.search(text):
        return InstructionAction.REQUIRE
    if _REWRITE_PATTERN.search(text):
        return InstructionAction.REWRITE
    if raw.get("sql_fragment"):
        return InstructionAction.GUARDRAIL
    return InstructionAction.NOTE


def _extract_tables(text: str) -> list[str]:
    """Extract table references from free text."""
    return _TABLE_PATTERN.findall(text)


def _build_condition(raw: dict[str, Any]) -> InstructionCondition:
    """Build an InstructionCondition from raw dict."""
    cond = raw.get("condition", {}) or {}
    if isinstance(cond, str):
        return InstructionCondition(question_pattern=cond)

    tables = cond.get("tables", [])
    if isinstance(tables, str):
        tables = [tables]

    metrics = cond.get("metrics", [])
    if isinstance(metrics, str):
        metrics = [metrics]

    dimensions = cond.get("dimensions", [])
    if isinstance(dimensions, str):
        dimensions = [dimensions]

    intents = cond.get("intents", [])
    if isinstance(intents, str):
        intents = [intents]

    return InstructionCondition(
        tables=frozenset(tables),
        metrics=frozenset(metrics),
        dimensions=frozenset(dimensions),
        intents=frozenset(intents),
        question_pattern=str(cond.get("question_pattern", "")),
    )


class InstructionCompiler:
    """Compiles raw instruction/rule definitions into an InstructionSet.

    Supports both legacy business_rules dicts and structured instruction YAML.
    """

    def __init__(self, *, default_priority: int = 100):
        self.default_priority = default_priority

    def compile(
        self,
        rules: Sequence[dict[str, Any]],
        *,
        source_file: str = "",
    ) -> InstructionSet:
        """Compile a list of raw rules/instructions into an InstructionSet."""
        instructions: list[Instruction] = []

        for idx, raw in enumerate(rules):
            try:
                inst = self._compile_one(raw, idx=idx, source_file=source_file)
                if inst:
                    instructions.append(inst)
            except Exception as exc:
                logger.warning("Failed to compile instruction %d: %s", idx, exc)

        iset = InstructionSet(instructions=instructions)
        iset.rebuild_indexes()
        logger.info(
            "Compiled %d instructions from %d raw rules (source=%s)",
            len(instructions),
            len(rules),
            source_file or "unknown",
        )
        return iset

    def compile_and_merge(
        self,
        *rule_lists: Sequence[dict[str, Any]],
        source_files: Sequence[str] = (),
    ) -> InstructionSet:
        """Compile multiple rule lists and merge into a single InstructionSet."""
        all_instructions: list[Instruction] = []

        for idx, rules in enumerate(rule_lists):
            src = source_files[idx] if idx < len(source_files) else ""
            iset = self.compile(rules, source_file=src)
            all_instructions.extend(iset.instructions)

        merged = InstructionSet(instructions=all_instructions)
        merged.rebuild_indexes()
        return merged

    def _compile_one(
        self,
        raw: dict[str, Any],
        *,
        idx: int = 0,
        source_file: str = "",
    ) -> Instruction | None:
        """Compile a single raw rule dict into an Instruction."""
        # Determine if this is structured (has 'action' key) or legacy
        is_structured = "action" in raw and "scope" in raw

        if is_structured:
            return self._compile_structured(raw, idx=idx, source_file=source_file)
        return self._compile_legacy(raw, idx=idx, source_file=source_file)

    def _compile_structured(
        self,
        raw: dict[str, Any],
        *,
        idx: int = 0,
        source_file: str = "",
    ) -> Instruction:
        """Compile a structured instruction with explicit fields."""
        name = str(raw.get("name", f"instruction_{idx}"))
        description = str(raw.get("description", ""))
        id_ = str(raw.get("id", _stable_id(f"{source_file}:{name}:{idx}")))

        scope = InstructionScope(raw["scope"].lower())
        action = InstructionAction(raw["action"].lower())
        condition = _build_condition(raw)

        return Instruction(
            id=id_,
            name=name,
            description=description,
            scope=scope,
            action=action,
            condition=condition,
            priority=int(raw.get("priority", self.default_priority)),
            prefer_tables=tuple(raw.get("prefer_tables", ())),
            avoid_tables=tuple(raw.get("avoid_tables", ())),
            require_filter=str(raw.get("require_filter", "")),
            rewrite_from=str(raw.get("rewrite_from", "")),
            rewrite_to=str(raw.get("rewrite_to", "")),
            guardrail_sql=str(raw.get("guardrail_sql", "")),
            sql_fragment=str(raw.get("sql_fragment", "")),
            note_text=str(raw.get("note_text", description)),
            source_file=source_file,
            source_rule=name,
        )

    def _compile_legacy(
        self,
        raw: dict[str, Any],
        *,
        idx: int = 0,
        source_file: str = "",
    ) -> Instruction | None:
        """Compile a legacy business_rule dict into an Instruction."""
        term = str(raw.get("term", ""))
        definition = str(raw.get("definition", ""))
        sql_fragment = str(raw.get("sql_fragment", ""))

        if not term and not definition:
            return None

        text = f"{term} {definition}"
        id_ = _stable_id(f"{source_file}:legacy:{term}:{idx}")
        scope = _infer_scope(raw)
        action = _infer_action(raw)

        # Extract referenced tables from text
        referenced_tables = _extract_tables(text)
        synonyms = raw.get("synonyms", [])
        categories = raw.get("categories", [])

        # Build condition from extracted context
        condition_tables: list[str] = list(referenced_tables)
        condition = InstructionCondition(
            tables=frozenset(condition_tables),
        )

        # Map action-specific payloads
        prefer_tables: tuple[str, ...] = ()
        avoid_tables: tuple[str, ...] = ()
        require_filter = ""
        rewrite_from = ""
        rewrite_to = ""

        if action == InstructionAction.PREFER:
            prefer_tables = tuple(referenced_tables)
        elif action == InstructionAction.AVOID:
            avoid_tables = tuple(referenced_tables)
        elif action == InstructionAction.REQUIRE and sql_fragment:
            require_filter = sql_fragment
        elif action == InstructionAction.REWRITE:
            # Try to parse "replace X with Y" from definition
            rewrite_match = re.search(
                r"(?:replace|rewrite|instead of)\s+(.+?)\s+(?:with|to|as)\s+(.+)",
                definition,
                re.IGNORECASE,
            )
            if rewrite_match:
                rewrite_from = rewrite_match.group(1).strip()
                rewrite_to = rewrite_match.group(2).strip()

        return Instruction(
            id=id_,
            name=term or f"rule_{idx}",
            description=definition,
            scope=scope,
            action=action,
            condition=condition,
            priority=self.default_priority,
            prefer_tables=prefer_tables,
            avoid_tables=avoid_tables,
            require_filter=require_filter,
            rewrite_from=rewrite_from,
            rewrite_to=rewrite_to,
            guardrail_sql=sql_fragment if action == InstructionAction.GUARDRAIL else "",
            sql_fragment=sql_fragment,
            note_text=definition,
            source_file=source_file,
            source_rule=term,
        )
