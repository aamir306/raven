#!/usr/bin/env python3
"""
Preprocessing: Auto-Generate Schema Descriptions
==================================================
Uses GPT-4o-mini to generate table and column descriptions from names,
data types, and structural context.

Prioritizes gold > silver tables. Skips tables that already have descriptions.
Updates schema_catalog.json in-place and optionally re-embeds into pgvector.

Usage:
    PYTHONPATH=. python preprocessing/auto_describe.py \
        --catalog-path data/schema_catalog.json \
        --tier gold \
        --batch-size 5 \
        --dry-run

    PYTHONPATH=. python preprocessing/auto_describe.py \
        --catalog-path data/schema_catalog.json \
        --tier gold \
        --batch-size 5
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from pathlib import Path

import openai

logger = logging.getLogger(__name__)

# ── Prompt Template ───────────────────────────────────────────────────

SYSTEM_PROMPT = """\
You are a data catalog documentation assistant. Given a table from a Trino-Iceberg \
data warehouse, generate concise, accurate descriptions for the table and each column.

Rules:
- Table description: 1-2 sentences explaining what the table stores and its business purpose.
- Column descriptions: 1 short sentence each explaining the column's meaning.
- Infer meaning from naming conventions (e.g., "gold_" = aggregated/cleaned, \
"dim_" = dimension, "fact_" = fact table, "_id" = identifier, "_at" = timestamp, \
"_count" = count metric, "_pct" = percentage).
- Use the schema name and related tables (depends_on) as context clues.
- Be specific: "Unique identifier for the batch" not "An ID field".
- If a column's purpose is genuinely unclear from the name alone, say "Purpose unclear from name".
- Output valid JSON only. No markdown fences.
"""

USER_PROMPT_TEMPLATE = """\
Generate descriptions for this table:

Table: {table_name}
Schema: {schema}
Materialization: {materialization}
Depends on: {depends_on}

Columns:
{columns_text}

Return JSON:
{{
  "table_description": "...",
  "columns": {{
    "column_name": "description",
    ...
  }}
}}
"""


# ── Core Logic ────────────────────────────────────────────────────────


def needs_description(table: dict) -> bool:
    """Check if a table needs description generation."""
    has_table_desc = bool(table.get("description", "").strip())
    if not has_table_desc:
        return True
    # Also check if >50% of columns lack descriptions
    cols = table.get("columns", [])
    if not cols:
        return False
    empty_cols = sum(1 for c in cols if not c.get("description", "").strip())
    return empty_cols / len(cols) > 0.5


def classify_tier(table: dict) -> str:
    """Classify table into gold/silver/bronze tier."""
    name = table.get("table_name", "").lower()
    schema = table.get("schema", "").lower()
    full = f"{schema}.{name}"

    if "gold" in full or "mview" in schema:
        return "gold"
    elif "silver" in full:
        return "silver"
    elif "bronze" in full or "raw" in full:
        return "bronze"
    return "other"


def format_columns_text(columns: list[dict]) -> str:
    """Format columns for the prompt."""
    lines = []
    for col in columns[:60]:  # Cap at 60 columns per table
        name = col.get("name", col.get("column_name", ""))
        dtype = col.get("data_type", "") or ""
        partition = " [PARTITION]" if col.get("is_partition") else ""
        lines.append(f"  - {name} ({dtype}){partition}")
    if len(columns) > 60:
        lines.append(f"  ... and {len(columns) - 60} more columns")
    return "\n".join(lines)


async def generate_descriptions_batch(
    client: openai.AsyncAzureOpenAI,
    tables: list[dict],
    model: str = "gpt4o-mini",
) -> list[dict]:
    """Generate descriptions for a batch of tables using GPT-4o-mini."""
    results = []

    for table in tables:
        table_name = table.get("table_name", "")
        columns_text = format_columns_text(table.get("columns", []))
        depends_on = ", ".join(table.get("depends_on", [])[:10]) or "none"

        prompt = USER_PROMPT_TEMPLATE.format(
            table_name=table_name,
            schema=table.get("schema", ""),
            materialization=table.get("materialization", "unknown"),
            depends_on=depends_on,
            columns_text=columns_text or "(no columns)",
        )

        try:
            response = await client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.2,
                max_tokens=2000,
                response_format={"type": "json_object"},
            )

            content = response.choices[0].message.content
            parsed = json.loads(content)

            results.append({
                "table_name": table_name,
                "table_description": parsed.get("table_description", ""),
                "columns": parsed.get("columns", {}),
                "cost": _estimate_cost(response),
            })

            logger.info(
                "Generated description for %s (%d columns)",
                table_name,
                len(parsed.get("columns", {})),
            )

        except Exception as e:
            logger.warning("Failed to describe %s: %s", table_name, e)
            results.append({
                "table_name": table_name,
                "table_description": "",
                "columns": {},
                "error": str(e),
            })

    return results


def _estimate_cost(response) -> float:
    """Estimate USD cost from token usage (GPT-4o-mini pricing)."""
    usage = response.usage
    if not usage:
        return 0.0
    # GPT-4o-mini: $0.15/1M input, $0.60/1M output
    input_cost = (usage.prompt_tokens / 1_000_000) * 0.15
    output_cost = (usage.completion_tokens / 1_000_000) * 0.60
    return round(input_cost + output_cost, 6)


def apply_descriptions(catalog: list[dict], results: list[dict]) -> tuple[int, int]:
    """Apply generated descriptions back to the catalog. Returns (tables_updated, cols_updated)."""
    result_map = {r["table_name"]: r for r in results if not r.get("error")}

    tables_updated = 0
    cols_updated = 0

    for table in catalog:
        desc = result_map.get(table.get("table_name"))
        if not desc:
            continue

        # Update table description (only if currently empty)
        if not table.get("description", "").strip() and desc["table_description"]:
            table["description"] = desc["table_description"]
            tables_updated += 1

        # Update column descriptions (only if currently empty)
        col_descs = desc.get("columns", {})
        for col in table.get("columns", []):
            col_name = col.get("name", col.get("column_name", ""))
            if not col.get("description", "").strip() and col_name in col_descs:
                col["description"] = col_descs[col_name]
                cols_updated += 1

    return tables_updated, cols_updated


# ── Main ──────────────────────────────────────────────────────────────


async def main():
    parser = argparse.ArgumentParser(description="Auto-generate schema descriptions using GPT-4o-mini")
    parser.add_argument("--catalog-path", default="data/schema_catalog.json")
    parser.add_argument("--tier", choices=["gold", "silver", "all"], default="gold",
                        help="Which tier of tables to describe")
    parser.add_argument("--batch-size", type=int, default=5,
                        help="Number of tables per LLM batch")
    parser.add_argument("--max-tables", type=int, default=500,
                        help="Maximum tables to process")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be described without calling LLM")
    parser.add_argument("--output", default=None,
                        help="Output path (defaults to overwriting input)")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-5s %(message)s",
        datefmt="%H:%M:%S",
    )

    catalog_path = Path(args.catalog_path)
    if not catalog_path.exists():
        logger.error("Catalog not found: %s", catalog_path)
        sys.exit(1)

    with open(catalog_path) as f:
        catalog = json.load(f)

    # Filter tables needing descriptions
    candidates = []
    for t in catalog:
        tier = classify_tier(t)
        if args.tier != "all" and tier != args.tier:
            continue
        if needs_description(t):
            candidates.append(t)

    candidates = candidates[:args.max_tables]

    logger.info(
        "Found %d %s tables needing descriptions (of %d total)",
        len(candidates), args.tier, len(catalog),
    )

    if args.dry_run:
        print(f"\n{'='*60}")
        print(f"DRY RUN: Would describe {len(candidates)} tables")
        print(f"{'='*60}")
        for t in candidates[:20]:
            cols = t.get("columns", [])
            empty_cols = sum(1 for c in cols if not c.get("description", "").strip())
            print(f"  {t['table_name']:60s} {len(cols):3d} cols, {empty_cols:3d} empty")
        if len(candidates) > 20:
            print(f"  ... and {len(candidates) - 20} more")

        # Cost estimate: ~500 tokens/table input, ~300 tokens/table output
        est_input = len(candidates) * 500
        est_output = len(candidates) * 300
        est_cost = (est_input / 1_000_000) * 0.15 + (est_output / 1_000_000) * 0.60
        print(f"\nEstimated cost: ${est_cost:.2f}")
        return

    # Initialize Azure OpenAI client
    client = openai.AsyncAzureOpenAI(
        azure_endpoint=os.environ["AZURE_OPENAI_API_BASE"],
        api_key=os.environ["AZURE_OPENAI_API_KEY"],
        api_version=os.environ.get("AZURE_OPENAI_API_VERSION", "2024-03-01-preview"),
    )

    model = os.environ.get("AZURE_OPENAI_CHAT_MODEL", "gpt4o")

    # Process in batches
    all_results = []
    total_cost = 0.0
    start_time = time.time()

    for i in range(0, len(candidates), args.batch_size):
        batch = candidates[i:i + args.batch_size]
        batch_num = i // args.batch_size + 1
        total_batches = (len(candidates) + args.batch_size - 1) // args.batch_size

        logger.info("Processing batch %d/%d (%d tables)", batch_num, total_batches, len(batch))

        results = await generate_descriptions_batch(client, batch, model)
        all_results.extend(results)

        batch_cost = sum(r.get("cost", 0) for r in results)
        total_cost += batch_cost

        # Rate limiting pause between batches
        if i + args.batch_size < len(candidates):
            await asyncio.sleep(1)

    # Apply descriptions
    tables_updated, cols_updated = apply_descriptions(catalog, all_results)

    elapsed = time.time() - start_time
    errors = sum(1 for r in all_results if r.get("error"))

    # Save
    output_path = Path(args.output or args.catalog_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(catalog, f, indent=2)

    print(f"\n{'='*60}")
    print(f"AUTO-DESCRIBE COMPLETE")
    print(f"{'='*60}")
    print(f"  Tables processed:  {len(all_results)}")
    print(f"  Tables updated:    {tables_updated}")
    print(f"  Columns updated:   {cols_updated}")
    print(f"  Errors:            {errors}")
    print(f"  Total cost:        ${total_cost:.4f}")
    print(f"  Time:              {elapsed:.1f}s")
    print(f"  Output:            {output_path}")


if __name__ == "__main__":
    asyncio.run(main())
