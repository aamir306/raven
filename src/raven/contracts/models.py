"""
Typed contract bundle models for semantic/domain pack loading.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class ContractSource:
    path: str
    kind: str


@dataclass
class ContractBundle:
    name: str = ""
    description: str = ""
    tables: list[dict[str, Any]] = field(default_factory=list)
    business_rules: list[dict[str, Any]] = field(default_factory=list)
    verified_queries: list[dict[str, Any]] = field(default_factory=list)
    relationships: list[dict[str, Any]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    sources: list[ContractSource] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "tables": list(self.tables),
            "business_rules": list(self.business_rules),
            "verified_queries": list(self.verified_queries),
            "relationships": list(self.relationships),
            "metadata": dict(self.metadata),
        }
