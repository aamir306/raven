"""
Typed query-plan representation for deterministic SQL compilation.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from ..grounding.value_resolver import ResolvedFilter
from ..sql import build_query_ast, compile_trino_sql


@dataclass(frozen=True)
class PlanEvidence:
    kind: str
    source: str
    detail: str
    score: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PlanJoin:
    left_table: str
    right_table: str
    condition_sql: str
    source: str = ""
    notes: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class QueryPlan:
    path_type: str
    intent: str
    table: str
    metric_name: str
    metric_sql: str
    confidence: str = "MEDIUM"
    source_tables: list[str] = field(default_factory=list)
    joins: list[PlanJoin] = field(default_factory=list)
    group_by: str | None = None
    group_by_sql: str | None = None
    time_dimension: str | None = None
    time_dimension_sql: str | None = None
    time_grain: str | None = None
    filters: list[ResolvedFilter] = field(default_factory=list)
    order_direction: str = "DESC"
    limit: int | None = None
    evidence: list[PlanEvidence] = field(default_factory=list)

    def compiled_sql(self) -> str:
        return compile_trino_sql(build_query_ast(self))

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["filters"] = [flt.to_dict() for flt in self.filters]
        payload["joins"] = [join.to_dict() for join in self.joins]
        payload["evidence"] = [ev.to_dict() for ev in self.evidence]
        payload["source_tables"] = list(self.source_tables or [self.table])
        payload["compiled_sql"] = self.compiled_sql()
        return payload
