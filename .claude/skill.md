---
name: raven
description: "RAVEN — Retrieval-Augmented Validated Engine for Natural-language SQL. Build and maintain a production text-to-SQL system for Trino-Iceberg data warehouses. Use this skill whenever working on: any RAVEN pipeline component (router, schema selector, candidate generator, validation, probes), preprocessing scripts (dbt extraction, Metabase questions, LSH index, content awareness, glossary), prompt template authoring for SQL generation, Trino dialect SQL rules, error taxonomy integration, CHASE-SQL multi-candidate generation, PExA test probes, NetworkX graph traversal for schema linking, Snowflake-style semantic model YAML, pgvector embedding pipelines, or FastAPI web service for NL-to-SQL. Also trigger for any task involving: Trino SQL dialect rules, OpenAI GPT-4o/4o-mini routing for SQL generation, MinHash LSH entity matching, business glossary authoring, content awareness metadata, documentation ingestion, or accuracy evaluation of text-to-SQL systems."
---

# RAVEN — Retrieval-Augmented Validated Engine for Natural-language SQL

## Overview

This skill guides building RAVEN's 8-stage text-to-SQL pipeline for a Trino-Iceberg platform with 1,200+ tables. The system takes English questions and returns SQL + data + charts + NL summaries.

**This is NOT a fork of any existing project.** We build from scratch using architectural blueprints from CHESS (Stanford), CHASE-SQL (Apple), PExA (Bloomberg), SQL-of-Thought, QueryWeaver, TriSQL, and Snowflake Cortex Analyst.

## Before You Start

1. Read the full implementation plan: `injection/references/text-to-sql-v4-final.md`
2. Read the conversation memory for context: `injection/references/conversation-memory.md`
3. Read the build guide: `docs/build-guide.md`
4. Check which phase/week you're implementing — the plan has explicit checklists per week

## Architecture: 8-Stage Pipeline

```
Stage 1: Router          → SIMPLE / COMPLEX / AMBIGUOUS (GPT-4o-mini)
Stage 2: Context Retrieval → keywords, LSH, few-shot, glossary, docs, content awareness
Stage 3: Schema Selection  → 4-step pruning + NetworkX graph bridge table discovery
Stage 4: Test Probes       → PExA: decompose → probe Trino → collect evidence [COMPLEX ONLY]
Stage 5: SQL Generation    → CHASE-SQL: 1 or 3 candidates + Trino dialect rules
Stage 6: Selection+Validate → pairwise selection + 36-type error taxonomy [COMPLEX ONLY]
Stage 7: Execute+Render    → Trino execution + Plotly chart + NL summary
Stage 8: Respond+Feedback  → SQL + table + chart + summary + confidence + feedback
```

**Simple queries (70%):** Stages 1→2→3→5(1 candidate)→7→8. Cost: ~$0.02, latency: 2-4s.
**Complex queries (30%):** All 8 stages, 3 candidates. Cost: ~$0.10, latency: 8-18s.

## Critical Design Rules

### Rule 1: Never Send Data Values to OpenAI API
- Schema names, column names, descriptions → OK
- Actual row data, query results, sample values → NEVER
- LSH entity matching runs locally
- SQL execution runs locally
- Chart generation uses column names only

### Rule 2: Always Use Trino Dialect
Every SQL generation prompt MUST include the Trino dialect rules from `prompts/trino_dialect_rules.txt`. Key rules:
- `TRY_CAST` not `CAST`
- `approx_distinct` for large tables
- `DATE_ADD('day', -30, CURRENT_DATE)` not `CURRENT_DATE - 30`
- `INTERVAL '30' DAY` (quoted number)
- Always include partition column in WHERE
- `COALESCE` not `IFNULL`
- `CROSS JOIN UNNEST(array_col)` for array operations

### Rule 3: Graph Path Discovery for Schema Selection
After the LLM picks candidate tables, ALWAYS run NetworkX shortest_path between all pairs to find bridge tables. This is the #1 accuracy improvement over pure vector search.

```python
from itertools import combinations
import networkx as nx

for t1, t2 in combinations(candidate_tables, 2):
    try:
        path = nx.shortest_path(dbt_graph, t1, t2)
        full_table_set.update(path)
    except nx.NetworkXNoPath:
        pass
```

### Rule 4: Probes Before Generation (PExA Pattern)
For COMPLEX queries, Stage 4 runs BEFORE Stage 5. The probe evidence (real enum values, row counts, NULL rates) gets passed as context to the SQL generators. This prevents value format mismatches and hallucinated enum values.

### Rule 5: Error Taxonomy is Classified, Not Generic
When SQL fails, classify the error using the 13-category / 36-subtype taxonomy (in `config/error_taxonomy.json`), then apply a targeted repair — not generic "fix this SQL."

### Rule 6: Semantic Model Over Simple Glossary
Business terms are defined in Snowflake Cortex-style YAML (`config/semantic_model.yaml`) with dimensions, facts, metrics, synonyms, time grains, and verified queries. Not just term→SQL fragments.

## Repository Structure

```
raven/
├── .claude/                # Claude Code skill file
│   └── skill.md
├── config/                 # Configuration files
│   ├── error_taxonomy.json
│   ├── trino_dialect_rules.txt
│   ├── settings.yaml
│   ├── model_routing.yaml
│   ├── cost_guards.yaml
│   └── semantic_model.yaml
├── docs/                   # Documentation
│   └── build-guide.md      # Step-by-step implementation guide
├── injection/              # Research references (context for AI)
│   └── references/
├── preprocessing/          # One-time build + weekly refresh
│   ├── extract_dbt_metadata.py
│   ├── extract_metabase_questions.py
│   ├── build_lsh_index.py
│   ├── build_content_awareness.py
│   ├── build_table_graph.py
│   ├── build_glossary.py
│   ├── ingest_documentation.py
│   └── refresh_all.py
├── src/raven/              # Core pipeline
│   ├── pipeline.py         # Main orchestrator
│   ├── router/             # Stage 1
│   ├── retrieval/          # Stage 2 (6 sub-modules)
│   ├── schema/             # Stage 3 (4 sub-modules)
│   ├── probes/             # Stage 4 (3 sub-modules)
│   ├── generation/         # Stage 5 (6 sub-modules)
│   ├── validation/         # Stage 6 (3 sub-modules)
│   ├── output/             # Stage 7 (4 sub-modules)
│   ├── connectors/         # trino, pgvector, openai
│   ├── feedback/           # rating, correction, accuracy tracking
│   └── safety/             # query validator, data policy
├── web/                    # FastAPI + React UI
├── prompts/                # All prompt templates (16 files)
├── data/                   # Generated artifacts (gitignored)
├── tests/                  # Test set + evaluation scripts
└── k8s/                    # Kubernetes manifests
```

See `injection/references/text-to-sql-v4-final.md` Section 6 for the full file-by-file breakdown.

## Implementation Phases

### Phase 1 (Weeks 1-3): Core Pipeline — Target 70-75%
- Week 1: Connectors (Trino, pgvector, OpenAI) + Docker + basic test
- Week 2: Full preprocessing pipeline (dbt, Metabase, LSH, content awareness, graph, glossary)
- Week 3: All 8 stages wired together + 50-question test set + accuracy eval

### Phase 2 (Weeks 4-5): Probes + Validation + Web — Target 75-80%
- Week 4: PExA probes, error taxonomy checker, cost guard, chart/summary output
- Week 5: FastAPI + React UI + K8s deployment + 5-10 beta users

### Phase 3 (Weeks 6-8): Accuracy Optimization — Target 80-85%
- Failure analysis, glossary expansion, more few-shot, feedback loop, multi-turn, caching

### Phase 4 (Weeks 9-12): Production — Target 82-88%
- Auth, monitoring, Slack bot, admin dashboard, load testing

## Component Implementation Guide

### Connectors (implement first)

**trino_connector.py:**
- Use `trino-python-client` library
- Connection pooling
- Read-only user enforcement (reject INSERT/UPDATE/DELETE)
- Resource group: max 4GB memory, 120s timeout, 10K rows
- Method: `execute(sql) → DataFrame`, `explain(sql) → cost_estimate`

**pgvector_store.py:**
- Tables: `schema_embeddings`, `question_embeddings`, `glossary_embeddings`, `doc_embeddings`
- Embedding model: `text-embedding-3-small` (1536-dim)
- Methods: `insert(text, metadata, embedding)`, `search(query_embedding, top_k) → results`, `batch_insert(items)`
- Use `pgvector` extension: `CREATE EXTENSION IF NOT EXISTS vector;`

**openai_client.py:**
- Route by stage using `config/model_routing.yaml`
- GPT-4o for SQL generation (complex), GPT-4o-mini for everything else
- Retry with exponential backoff (3 retries, 1s/2s/4s)
- Track cost per call: log model, input_tokens, output_tokens, cost
- Method: `complete(prompt, stage_name) → response_text`

### Preprocessing (implement second)

Each script is standalone, runnable independently. `refresh_all.py` orchestrates all.

**Key: extract_metabase_questions.py**
```sql
-- This is your training gold mine: 500-2000 (question, SQL) pairs
SELECT rc.name, (rc.dataset_query::json->>'native')::json->>'query'
FROM report_card rc
WHERE (rc.dataset_query::json->>'type') = 'native'
  AND rc.archived = false;
```

**Key: build_table_graph.py**
Three edge sources:
1. dbt `ref()` dependencies (from manifest.json)
2. Metabase JOIN patterns (parse SQL for `JOIN ... ON` clauses)
3. Semantic model relationships (from semantic_model.yaml)

### Pipeline Stages (implement third)

**pipeline.py** is the main orchestrator:
```python
async def generate(question: str, session_id: str) -> Response:
    # Stage 1
    difficulty = await router.classify(question)
    if difficulty == "AMBIGUOUS":
        return ask_clarification(question)
    
    # Stage 2
    context = await retriever.retrieve(question)
    
    # Stage 3
    schema = await schema_selector.select(question, context)
    
    # Stage 4 (complex only)
    evidence = None
    if difficulty == "COMPLEX":
        evidence = await probe_executor.probe(question, schema)
    
    # Stage 5
    if difficulty == "SIMPLE":
        candidates = [await generator.generate_single(question, schema, context)]
    else:
        candidates = await generator.generate_three(question, schema, context, evidence)
    
    # Stage 6 (complex only)
    if difficulty == "COMPLEX":
        winner = await validator.select_and_validate(question, schema, candidates)
    else:
        winner = candidates[0]
    
    # Stage 7
    result = await executor.execute(winner.sql)
    chart = await chart_gen.generate(result)
    summary = await summarizer.summarize(question, winner.sql, result)
    
    # Stage 8
    return Response(sql=winner.sql, data=result, chart=chart, 
                    summary=summary, confidence=winner.confidence)
```

### Prompt Templates

All 16 prompt templates go in `prompts/`. Each follows the pattern:
```
{system_instructions}
{trino_dialect_rules}  # included in all generation prompts
{pruned_schema}
{context_from_stage_2}
{probe_evidence}  # only for complex queries
{few_shot_examples}

Question: {user_question}

Output format: {structured_output_spec}
```

Key: prompts must enforce structured output (e.g., "Output exactly one word: SIMPLE, COMPLEX, or AMBIGUOUS") to prevent LLM verbosity.

## LLM Model Routing

| Stage | Model | Temperature | Max Tokens |
|---|---|---|---|
| Router | gpt-4o-mini | 0 | 50 |
| IR keyword extract | gpt-4o-mini | 0 | 300 |
| Schema selector (all steps) | gpt-4o-mini | 0 | 600-1000 |
| Probe planner | gpt-4o-mini | 0 | 500 |
| SQL generator (simple) | gpt-4o-mini | 0 | 2000 |
| SQL generator (complex) | gpt-4o | 0.3 | 4000 |
| Revision (error fix) | gpt-4o | 0 | 4000 |
| Pairwise selection | gpt-4o-mini | 0 | 200 |
| Error taxonomy check | gpt-4o-mini | 0 | 500 |
| Chart detection | gpt-4o-mini | 0 | 200 |
| NL summary | gpt-4o-mini | 0.3 | 300 |

## Error Taxonomy (36 Sub-Types)

The full taxonomy is in `config/error_taxonomy.json`. Categories:

1. **syntax** (2): sql_syntax_error, invalid_alias
2. **schema_link** (4): table_missing, col_missing, ambiguous_col, incorrect_foreign_key
3. **join** (4): join_missing, join_wrong_type, extra_table, incorrect_col
4. **filter** (3): where_missing, condition_wrong_col, condition_type_mismatch
5. **aggregation** (5): agg_no_groupby, groupby_missing_col, having_without_groupby, having_incorrect, having_vs_where
6. **value** (2): hardcoded_value, value_format_wrong
7. **subquery** (3): unused_subquery, subquery_missing, subquery_correlation_error
8. **set_op** (3): union_missing, intersect_missing, except_missing
9. **others** (5): order_by_missing, limit_missing, duplicate_select, unsupported_function, incorrect_foreign_key_relationship
10. **select** (2): incorrect_extra_values, incorrect_order
11. **trino_specific** (3): missing_partition_filter, cast_instead_of_try_cast, non_trino_function

## Semantic Model Format

Business glossary uses Snowflake Cortex-style YAML. See `references/text-to-sql-v4-final.md` Section 5.5 for the complete format with examples.

Key elements per table: dimensions (with synonyms, is_enum), time_dimensions, facts, metrics.
Key elements at model level: relationships (explicit join columns), business_rules (term + sql_fragment + synonyms), verified_queries (question + SQL + use_as_onboarding flag).

## Testing and Evaluation

**Test set:** `tests/test_set.json` — minimum 50 questions (Phase 1), expanding to 200 (Phase 3).

Format:
```json
[
  {
    "question": "How many active users yesterday?",
    "expected_tables": ["gold.daily_active_users"],
    "expected_sql_contains": ["CURRENT_DATE - INTERVAL '1' DAY", "COUNT"],
    "difficulty": "SIMPLE",
    "category": "user_metrics"
  }
]
```

**Evaluation:** `tests/eval_accuracy.py`
- Execute generated SQL on Trino
- Compare result against expected (execution accuracy)
- Check if correct tables were selected (schema accuracy)
- Check if SQL contains expected fragments
- Report per-category accuracy breakdown

## Common Pitfalls

1. **Forgetting partition filters** — Trino Iceberg tables are partitioned by `ds`/`dt`/`date`. Queries without partition filters cause full table scans (100GB+). The cost guard in Stage 6 catches this, but prompts should also emphasize it.

2. **Case-sensitive string comparison** — Trino string comparison is case-sensitive. If user says "Enterprise" but the column stores "enterprise", the query returns nothing. Content Awareness layer stores this info. Probes discover actual values.

3. **CAST vs TRY_CAST** — `CAST('abc' AS INTEGER)` fails the entire query. `TRY_CAST` returns NULL. Always use TRY_CAST for user-supplied values.

4. **Value format mismatches** — batch_id stored as "000042" not "42". Content Awareness records these patterns. PExA probes discover them at runtime.

5. **Bridge table omission** — Vector search finds semantically related tables but misses structurally required intermediate tables. Graph path discovery on dbt lineage fixes this.

6. **Data values leaking to API** — Every LLM call must be checked: does it contain actual data values? If yes, it's a policy violation. Only metadata/schema goes to OpenAI.

## Reference Files

| File | Purpose | When to Read |
|---|---|---|
| `injection/references/text-to-sql-v4-final.md` | Complete implementation plan (1,667 lines) | Before starting any phase. Contains exact checklists, prompt templates, config formats, cost analysis. |
| `injection/references/conversation-memory.md` | Full context from design conversations | When you need background on WHY a decision was made. |
| `docs/build-guide.md` | Step-by-step build instructions | When implementing any phase/week. |
| `config/error_taxonomy.json` | SQL-of-Thought 36-subtype error taxonomy | When implementing Stage 6 error taxonomy checker. |
| `config/semantic_model.yaml` | Snowflake-style business glossary | When implementing preprocessing/build_glossary.py or Stage 2 glossary retrieval. |
| `config/trino_dialect_rules.txt` | 20 Trino SQL rules | Inject into every SQL generation prompt. |
