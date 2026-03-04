"""
Error Taxonomy Checker — Stage 6.2
=====================================
Post-generation validation: checks the winning SQL against the
36-subtype error taxonomy using Content Awareness metadata.

Catches semantic errors that EXPLAIN won't detect, e.g.:
  - Using string comparison on an integer column
  - Filtering on a column with 85 % NULLs without COALESCE
  - Using ILIKE on a case-sensitive enum column
"""

from __future__ import annotations

import logging
from pathlib import Path

from ..connectors.openai_client import OpenAIClient

logger = logging.getLogger(__name__)

PROMPT_PATH = Path(__file__).resolve().parents[3] / "prompts" / "val_error_taxonomy.txt"


class ErrorTaxonomyChecker:
    """Check SQL for semantic errors using Content Awareness metadata."""

    def __init__(self, openai: OpenAIClient):
        self.openai = openai
        self._prompt_template = PROMPT_PATH.read_text()

    async def check(
        self,
        sql: str,
        question: str,
        pruned_schema: str,
        content_awareness: list[dict],
    ) -> list[dict]:
        """
        Check SQL against the error taxonomy.

        Args:
            sql: The winning SQL to check.
            question: User question.
            pruned_schema: Schema context.
            content_awareness: Column-level metadata from Stage 2.6.

        Returns:
            List of detected errors:
            [
                {
                    "category": "filter",
                    "subtype": "type_mismatch",
                    "description": "Comparing varchar to integer",
                    "fix": "Cast status to VARCHAR or use string literal",
                },
                ...
            ]
            Empty list means no errors found.
        """
        awareness_str = "\n".join(
            f"- {a['table']}.{a['column']}: {a.get('data_type', '')} | "
            f"null: {a.get('null_pct', '')}% | "
            f"distinct: {a.get('distinct_count', 'N/A')} | "
            f"format: {a.get('format_pattern', '')}"
            for a in content_awareness
        ) or "None"

        prompt = (
            self._prompt_template
            .replace("{user_question}", question)
            .replace("{pruned_schema}", pruned_schema)
            .replace("{content_awareness}", awareness_str)
            .replace("{sql}", sql)
        )

        response = await self.openai.complete(
            prompt=prompt, stage_name="val_taxonomy",
        )

        errors = self._parse_errors(response)
        if errors:
            logger.warning("Taxonomy check found %d errors", len(errors))
        else:
            logger.debug("Taxonomy check: no errors found")
        return errors

    @staticmethod
    def _parse_errors(response: str) -> list[dict]:
        """Parse LLM response for taxonomy errors."""
        errors: list[dict] = []

        # Check if errors were found
        lower = response.lower()
        if "errors_found: false" in lower or "no errors" in lower:
            return []

        if "errors_found: true" not in lower and "error" not in lower:
            return []

        for line in response.split("\n"):
            line = line.strip()
            upper = line.upper()
            if upper.startswith("ERROR") and (":" in line or "—" in line):
                # Parse: "ERROR 1: category — description — fix"
                # or: "ERROR: category/subtype — description — fix"
                sep = "—" if "—" in line else "-"
                parts = line.split(sep)
                category = parts[0].strip() if parts else ""
                # Clean the error prefix
                if ":" in category:
                    category = category.split(":", 1)[1].strip()

                errors.append({
                    "category": category,
                    "subtype": parts[1].strip() if len(parts) > 1 else "",
                    "description": parts[1].strip() if len(parts) > 1 else "",
                    "fix": parts[2].strip() if len(parts) > 2 else "",
                })

        return errors
