"""
Table Selector — Stage 3.3
============================
LLM selects 3-8 final tables for SQL generation from the
expanded candidate set (after graph bridge injection).
Also outputs JOIN path recommendations.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from ..connectors.openai_client import OpenAIClient

logger = logging.getLogger(__name__)

PROMPT_PATH = Path(__file__).resolve().parents[3] / "prompts" / "ss_table_select.txt"


class TableSelector:
    """Step 3 of Schema Selection — narrow to 3-8 tables."""

    def __init__(self, openai: OpenAIClient):
        self.openai = openai
        self._prompt_template = PROMPT_PATH.read_text()

    async def select(
        self,
        question: str,
        expanded_tables: list[str],
        graph_join_paths: list[dict] | None = None,
    ) -> tuple[list[str], list[str]]:
        """
        Select final tables and recommended JOIN clauses.

        Args:
            question: User question.
            expanded_tables: Tables after graph path expansion.
            graph_join_paths: Optional pre-computed JOIN paths from graph.

        Returns:
            (selected_tables, join_path_lines)
        """
        # Build table descriptions for prompt
        table_desc_lines: list[str] = []
        for t in expanded_tables:
            table_desc_lines.append(f"- {t}")

        # Append graph-derived join hints
        join_hints = ""
        if graph_join_paths:
            hints = []
            for jp in graph_join_paths:
                path_str = " → ".join(jp.get("path", []))
                keys = jp.get("join_keys", [])
                key_str = ", ".join(f"{k[0]} = {k[1]}" for k in keys) if keys else ""
                hints.append(f"  {path_str}  [{key_str}]")
            join_hints = "\nKnown JOIN paths:\n" + "\n".join(hints)

        prompt = (
            self._prompt_template
            .replace("{user_question}", question)
            .replace(
                "{candidate_tables_with_descriptions}",
                "\n".join(table_desc_lines) + join_hints,
            )
        )

        response = await self.openai.complete(
            prompt=prompt, stage_name="ss_table_select",
        )

        selected_tables, join_paths = self._parse_response(response)
        logger.info(
            "Table selection: %d/%d tables selected with %d join paths",
            len(selected_tables), len(expanded_tables), len(join_paths),
        )
        return selected_tables, join_paths

    # ── Parser ──────────────────────────────────────────────────────────

    @staticmethod
    def _parse_response(response: str) -> tuple[list[str], list[str]]:
        """Parse LLM response into selected tables and join paths."""
        selected_tables: list[str] = []
        join_paths: list[str] = []
        section: str | None = None

        for line in response.strip().split("\n"):
            line = line.strip()
            upper = line.upper()

            if "SELECTED_TABLES" in upper or "SELECTED TABLES" in upper:
                section = "tables"
                continue
            elif "JOIN_PATH" in upper or "JOIN PATH" in upper or "JOIN_PATHS" in upper:
                section = "joins"
                continue

            if section == "tables" and line:
                # Parse: "1. catalog.schema.table — reason — JOIN: ..."
                parts = line.split("—")
                table = parts[0].strip().lstrip("0123456789. -•")
                if table and ("." in table or table[0].isalpha()):
                    # Clean trailing whitespace/punctuation
                    table = table.strip().rstrip(",;")
                    if table:
                        selected_tables.append(table)

            elif section == "joins" and line:
                if "JOIN" in upper or "=" in line or "→" in line:
                    join_paths.append(line)

        return selected_tables, join_paths
