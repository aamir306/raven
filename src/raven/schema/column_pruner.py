"""
Column Pruner — Stage 3.4
===========================
Final step of schema selection: prune each selected table to only the
columns needed for the query.  Uses Content Awareness metadata and
documentation snippets to make informed decisions about data types,
NULLability, format patterns, etc.

Output is the "pruned schema" string injected into all downstream
generation and validation prompts.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from ..connectors.openai_client import OpenAIClient

logger = logging.getLogger(__name__)

PROMPT_PATH = Path(__file__).resolve().parents[3] / "prompts" / "ss_column_prune.txt"


class ColumnPruner:
    """Step 4 of Schema Selection — prune to needed columns per table."""

    def __init__(self, openai: OpenAIClient):
        self.openai = openai
        self._prompt_template = PROMPT_PATH.read_text()

    async def prune(
        self,
        question: str,
        selected_tables: list[str],
        content_awareness: list[dict],
        doc_snippets: list[dict],
        full_column_catalog: dict[str, list[dict]] | None = None,
    ) -> str:
        """
        Prune columns for each selected table.

        Args:
            question: User question.
            selected_tables: Tables chosen in Step 3.
            content_awareness: Column-level metadata from Stage 2.6.
            doc_snippets: Documentation snippets from Stage 2.5.
            full_column_catalog: Optional {table_fqn: [{name, type, desc}, ...]}.

        Returns:
            Pruned schema as a formatted string — the canonical input to
            generation and validation prompts.  Format:

            TABLE: gold.finance.orders
              - order_id (bigint) PK
              - customer_id (bigint) FK → gold.crm.customers.id
              - total_amount (decimal(18,2))
              - status (varchar) — ENUM(active, cancelled, pending), 0.2% NULL
            ...
        """
        # Build full column listing per table
        full_cols = self._build_table_columns(selected_tables, full_column_catalog)

        # Build Content Awareness string
        awareness_str = "\n".join(
            f"- {a['table']}.{a['column']}: {a.get('data_type', '')} | "
            f"format: {a.get('format_pattern', '')} | null: {a.get('null_pct', '')}% | "
            f"distinct: {a.get('distinct_count', 'N/A')}"
            for a in content_awareness
        ) or "None"

        # Build doc snippets string
        docs_str = "\n".join(
            f"- [{d.get('source', 'unknown')}] {d.get('table', '')}: "
            f"{d.get('content', '')[:200]}"
            for d in doc_snippets
        ) or "None"

        prompt = (
            self._prompt_template
            .replace("{user_question}", question)
            .replace("{selected_tables_full_columns}", full_cols)
            .replace("{content_awareness}", awareness_str)
            .replace("{doc_snippets}", docs_str)
        )

        response = await self.openai.complete(
            prompt=prompt, stage_name="ss_column_prune",
        )

        pruned_schema = response.strip()
        logger.info(
            "Column pruning: %d tables → %d lines of schema",
            len(selected_tables),
            pruned_schema.count("\n") + 1,
        )
        return pruned_schema

    # ── Helpers ─────────────────────────────────────────────────────────

    @staticmethod
    def _build_table_columns(
        tables: list[str],
        catalog: dict[str, list[dict]] | None,
    ) -> str:
        """
        Build a full column listing for each table.

        If a catalog is provided, use its column metadata.
        Otherwise, generate placeholder text.
        """
        if not catalog:
            return "\n".join(
                f"TABLE: {t}\n  (columns loaded during preprocessing)"
                for t in tables
            )

        lines: list[str] = []
        for t in tables:
            cols = catalog.get(t, [])
            lines.append(f"TABLE: {t}")
            for c in cols:
                name = c.get("name", "?")
                dtype = c.get("type", "unknown")
                desc = c.get("description", "")
                desc_suffix = f" — {desc}" if desc else ""
                lines.append(f"  - {name} ({dtype}){desc_suffix}")
            if not cols:
                lines.append("  (no column metadata available)")
        return "\n".join(lines)
