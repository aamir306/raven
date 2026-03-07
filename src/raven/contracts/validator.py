"""
Validation for semantic/domain contract bundles.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, field

from .models import ContractBundle


class SemanticContractValidationError(ValueError):
    """Raised when a semantic contract bundle is structurally invalid."""


@dataclass
class ValidationReport:
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors

    def raise_for_errors(self) -> None:
        if self.errors:
            raise SemanticContractValidationError("; ".join(self.errors))


class SemanticContractValidator:
    """Validate contract bundles before they are used by the query engine."""

    def validate(self, bundle: ContractBundle) -> ValidationReport:
        report = ValidationReport()
        table_names = [str(table.get("name", "")).strip() for table in bundle.tables]
        table_counter = Counter(name for name in table_names if name)

        if not bundle.tables:
            report.warnings.append("No tables defined in semantic contract bundle")

        duplicates = [name for name, count in table_counter.items() if count > 1]
        for name in duplicates:
            report.errors.append(f"Duplicate table contract: {name}")

        known_tables = set(table_counter)
        for idx, table in enumerate(bundle.tables):
            name = str(table.get("name", "")).strip()
            if not name:
                report.errors.append(f"Table at index {idx} is missing a name")
                continue

            metric_names = defaultdict(int)
            for metric in list(table.get("metrics", []) or []):
                metric_name = str(metric.get("name", "")).strip()
                if not metric_name:
                    report.errors.append(f"Table {name} has a metric without a name")
                    continue
                metric_names[metric_name] += 1
                if not str(metric.get("sql", "")).strip():
                    report.errors.append(f"Metric {metric_name} on table {name} is missing sql")
            for metric_name, count in metric_names.items():
                if count > 1:
                    report.errors.append(f"Duplicate metric {metric_name} on table {name}")

        for idx, relationship in enumerate(bundle.relationships):
            left_table = str(relationship.get("left_table", "")).strip()
            right_table = str(relationship.get("right_table", "")).strip()
            join_columns = relationship.get("join_columns", {}) or {}
            left_column = str(join_columns.get("left", "")).strip()
            right_column = str(join_columns.get("right", "")).strip()

            if not left_table or not right_table:
                report.errors.append(f"Relationship at index {idx} is missing left_table/right_table")
                continue
            if left_table not in known_tables:
                report.warnings.append(f"Relationship references unknown left table: {left_table}")
            if right_table not in known_tables:
                report.warnings.append(f"Relationship references unknown right table: {right_table}")
            if not left_column or not right_column:
                report.errors.append(
                    f"Relationship {left_table} -> {right_table} is missing join columns"
                )

        for idx, rule in enumerate(bundle.business_rules):
            term = str(rule.get("term", "")).strip()
            if not term:
                report.errors.append(f"Business rule at index {idx} is missing term")

        for idx, query in enumerate(bundle.verified_queries):
            question = str(query.get("question", "")).strip()
            sql = str(query.get("sql", "")).strip()
            if not question:
                report.errors.append(f"Verified query at index {idx} is missing question")
            if not sql:
                report.errors.append(f"Verified query '{question or idx}' is missing sql")

        return report
