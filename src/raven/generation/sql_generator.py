"""
Stage 5: SQL Generator (CHASE-SQL Multi-Candidate)
====================================================
- SIMPLE queries: 1 candidate via GPT-4o-mini
- COMPLEX queries: 3 diverse candidates via GPT-4o
    A: Divide-and-Conquer (sub-questions → CTEs)
    B: Execution Plan CoT (scan → filter → join → agg)
    C: Few-Shot from past queries
- Revision loop: execute → classify error → targeted fix (up to 2 retries)
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from pathlib import Path
from typing import Any

from ..connectors.openai_client import OpenAIClient
from ..connectors.trino_connector import TrinoConnector
from ..safety.query_validator import validate_read_only

logger = logging.getLogger(__name__)

PROMPTS_DIR = Path(__file__).resolve().parents[3] / "prompts"
CONFIG_DIR = Path(__file__).resolve().parents[3] / "config"

MAX_RETRIES = 2


class SQLGenerator:
    """Generate SQL candidates using CHASE-SQL multi-generator approach."""

    def __init__(self, openai: OpenAIClient, trino: TrinoConnector):
        self.openai = openai
        self.trino = trino

        # Load prompt templates
        self._gen_dc = (PROMPTS_DIR / "gen_divide_conquer.txt").read_text()
        self._gen_ep = (PROMPTS_DIR / "gen_execution_plan.txt").read_text()
        self._gen_fs = (PROMPTS_DIR / "gen_fewshot.txt").read_text()
        self._gen_complex = (PROMPTS_DIR / "gen_complex.txt").read_text()
        self._gen_revision = (PROMPTS_DIR / "gen_revision.txt").read_text()
        self._dialect_rules = (PROMPTS_DIR / "trino_dialect_rules.txt").read_text()

        # Load error taxonomy
        taxonomy_path = CONFIG_DIR / "error_taxonomy.json"
        self._error_taxonomy = json.loads(taxonomy_path.read_text()) if taxonomy_path.exists() else {}

    async def generate(
        self,
        question: str,
        difficulty: Any,
        pruned_schema: str,
        probe_evidence: list[dict],
        glossary_matches: list[dict],
        similar_queries: list[dict],
    ) -> list[str]:
        """
        Generate SQL candidates.

        SIMPLE: 1 candidate (Divide-and-Conquer via mini)
        COMPLEX: 3 candidates in parallel (DC, EP, FS via GPT-4o)
        """
        # Build shared context
        context = self._build_context(
            pruned_schema, probe_evidence, glossary_matches, similar_queries,
        )

        if hasattr(difficulty, "value") and difficulty.value == "SIMPLE":
            # Single candidate — fast path
            sql = await self._generate_candidate(
                "divide_conquer", question, context, stage_name="gen_simple",
            )
            candidates = [sql] if sql else []
        else:
            # 3 diverse candidates — parallel
            # A: Complex structured prompt (decompose → plan → write → verify)
            # B: Execution plan chain-of-thought
            # C: Few-shot pattern follower
            tasks = [
                self._generate_candidate("complex", question, context, "gen_candidate_a"),
                self._generate_candidate("execution_plan", question, context, "gen_candidate_b"),
                self._generate_candidate("fewshot", question, context, "gen_candidate_c"),
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            candidates = [r for r in results if isinstance(r, str) and r.strip()]

        # Validate and attempt revision for failed candidates
        validated = []
        for sql in candidates:
            result = await self._validate_and_revise(sql, question, pruned_schema)
            if result:
                validated.append(result)

        if not validated:
            logger.error("All candidates failed for: %s", question[:60])
            # Fall back to first raw candidate if available
            if candidates:
                validated = [candidates[0]]

        return validated

    async def _generate_candidate(
        self,
        strategy: str,
        question: str,
        context: dict,
        stage_name: str,
    ) -> str:
        """Generate a single SQL candidate using the specified strategy."""
        if strategy == "complex":
            template = self._gen_complex
        elif strategy == "divide_conquer":
            template = self._gen_dc
        elif strategy == "execution_plan":
            template = self._gen_ep
        elif strategy == "fewshot":
            template = self._gen_fs
        else:
            template = self._gen_dc

        prompt = (
            template
            .replace("{user_question}", question)
            .replace("{trino_dialect_rules}", self._dialect_rules)
            .replace("{pruned_schema}", context["pruned_schema"])
            .replace("{probe_evidence}", context["probe_evidence"])
            .replace("{glossary_definitions}", context["glossary_defs"])
            .replace("{few_shot_examples}", context["few_shot"])
        )

        # For fewshot strategy, fill in example slots
        if strategy == "fewshot":
            examples = context.get("similar_queries", [])
            for i in range(3):
                if i < len(examples):
                    prompt = prompt.replace(f"{{similar_q{i+1}}}", examples[i].get("question", "N/A"))
                    prompt = prompt.replace(f"{{similar_sql{i+1}}}", examples[i].get("sql", "N/A"))
                else:
                    prompt = prompt.replace(f"{{similar_q{i+1}}}", "N/A")
                    prompt = prompt.replace(f"{{similar_sql{i+1}}}", "N/A")

        response = await self.openai.complete(prompt=prompt, stage_name=stage_name)
        return self._extract_sql(response)

    async def _validate_and_revise(
        self,
        sql: str,
        question: str,
        pruned_schema: str,
    ) -> str | None:
        """Validate SQL on Trino. If error, classify and retry up to MAX_RETRIES."""
        if not sql or not validate_read_only(sql):
            logger.warning("SQL failed read-only validation, skipping")
            return None

        for attempt in range(MAX_RETRIES + 1):
            try:
                # Try EXPLAIN first (cheaper than executing)
                await asyncio.to_thread(self.trino.explain, sql)
                return sql  # Valid!
            except Exception as e:
                error_msg = str(e)
                if attempt >= MAX_RETRIES:
                    logger.warning("SQL failed after %d retries: %s", MAX_RETRIES, error_msg[:100])
                    return sql  # Return anyway — validator stage may still pick it

                # Classify error and attempt revision
                error_cat, error_sub, error_desc = self._classify_error(error_msg)
                sql = await self._revise_sql(
                    sql, question, pruned_schema, error_cat, error_sub, error_desc, error_msg,
                )

        return sql

    async def _revise_sql(
        self,
        failed_sql: str,
        question: str,
        pruned_schema: str,
        error_category: str,
        error_subtype: str,
        error_description: str,
        error_message: str,
    ) -> str:
        """Use LLM to fix a failed SQL query."""
        prompt = (
            self._gen_revision
            .replace("{user_question}", question)
            .replace("{pruned_schema}", pruned_schema)
            .replace("{failed_sql}", failed_sql)
            .replace("{error_category}", error_category)
            .replace("{error_subtype}", error_subtype)
            .replace("{error_description}", error_description)
            .replace("{error_message}", error_message)
            .replace("{trino_dialect_rules}", self._dialect_rules)
        )
        response = await self.openai.complete(prompt=prompt, stage_name="gen_revision")
        return self._extract_sql(response)

    def _classify_error(self, error_message: str) -> tuple[str, str, str]:
        """Classify a Trino error using the error taxonomy."""
        msg_lower = error_message.lower()

        # Walk taxonomy to find matching category
        for category, subtypes in self._error_taxonomy.items():
            if isinstance(subtypes, dict):
                for subtype, info in subtypes.items():
                    desc = info if isinstance(info, str) else info.get("description", "")
                    # Simple keyword matching
                    keywords = desc.lower().split()
                    if any(kw in msg_lower for kw in keywords if len(kw) > 3):
                        return category, subtype, desc

        # Fallback classification based on common patterns
        if "syntax" in msg_lower or "unexpected" in msg_lower:
            return "syntax", "sql_syntax_error", "SQL syntax error"
        elif "does not exist" in msg_lower or "cannot be resolved" in msg_lower:
            return "schema_link", "col_missing", "Referenced column or table does not exist"
        elif "type mismatch" in msg_lower or "cannot be cast" in msg_lower:
            return "filter", "type_mismatch", "Type mismatch in comparison"
        elif "must be an aggregate" in msg_lower or "group by" in msg_lower:
            return "aggregation", "missing_group_by", "Missing GROUP BY clause"

        return "syntax", "sql_syntax_error", "Unknown error — review SQL syntax"

    def _build_context(
        self,
        pruned_schema: str,
        probe_evidence: list[dict],
        glossary_matches: list[dict],
        similar_queries: list[dict],
    ) -> dict:
        """Build context strings for prompt templates."""
        # Probe evidence
        evidence_lines = []
        for p in probe_evidence:
            if p.get("success"):
                evidence_lines.append(f"Q: {p['question']}\nA: {p['result']}")
        probe_str = "\n\n".join(evidence_lines) if evidence_lines else "No probe evidence available."

        # Glossary definitions
        glossary_lines = []
        for g in glossary_matches:
            glossary_lines.append(
                f"- {g['term']}: {g['definition']}"
                + (f"\n  SQL: {g['sql_fragment']}" if g.get("sql_fragment") else "")
            )
        glossary_str = "\n".join(glossary_lines) if glossary_lines else "No glossary matches."

        # Few-shot examples
        fewshot_lines = []
        for q in similar_queries[:3]:
            fewshot_lines.append(f"Q: {q.get('question', 'N/A')}\nSQL: {q.get('sql', 'N/A')}")
        fewshot_str = "\n\n".join(fewshot_lines) if fewshot_lines else "No similar queries available."

        return {
            "pruned_schema": pruned_schema or "No schema available.",
            "probe_evidence": probe_str,
            "glossary_defs": glossary_str,
            "few_shot": fewshot_str,
            "similar_queries": similar_queries,
        }

    @staticmethod
    def _extract_sql(response: str) -> str:
        """Extract SQL from LLM response (handles markdown code blocks)."""
        # Try to find SQL in code blocks
        code_block = re.search(r"```(?:sql)?\s*\n?(.*?)```", response, re.DOTALL | re.IGNORECASE)
        if code_block:
            return code_block.group(1).strip()

        # Otherwise, look for SELECT/WITH at start of line
        lines = response.strip().split("\n")
        sql_lines = []
        in_sql = False
        for line in lines:
            stripped = line.strip().upper()
            if stripped.startswith(("SELECT", "WITH", "EXPLAIN")):
                in_sql = True
            if in_sql:
                sql_lines.append(line)

        if sql_lines:
            return "\n".join(sql_lines).strip().rstrip(";")

        # Last resort: return the full response
        return response.strip().rstrip(";")
