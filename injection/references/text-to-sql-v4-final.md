# Text-to-SQL Production System — Final Implementation Plan v4

## Trino-Iceberg CDP | 1,200+ Tables | March 2026

---

## 1. Executive Summary

This is the definitive implementation plan for a production text-to-SQL system targeting a Trino-Iceberg data platform with 1,200+ tables across bronze/silver/gold layers. This document is designed to be handed directly to Claude Code for execution.

**Critical architectural decision (v4 change):** We are NOT forking CHESS. The CHESS GitHub repo is dead — 9 commits, last update Nov 2024, tightly coupled to SQLite. Instead, we **build from scratch** using CHESS's architecture and prompt patterns as the blueprint. This is cleaner and avoids inheriting SQLite assumptions buried throughout.

### 1.1 What We Take from Each Research System

| Source | What We Steal | Implementation Impact |
|---|---|---|
| **CHESS** (Stanford, 71.10% BIRD) | 4-agent pipeline architecture: IR → SS → CG → UT. Prompt template patterns. | Architectural blueprint. Build our own, don't fork. |
| **CHASE-SQL** (Apple, 73.0% BIRD) | 3-generator diverse candidates + pairwise selection agent. | Build from paper description. Confirmed: no public code exists anywhere. |
| **QueryWeaver** (FalkorDB) | Graph-based schema traversal for bridge table discovery. Content Awareness layer (value formats, enums, NULL rates). | NetworkX on dbt lineage graph — no FalkorDB needed. |
| **SQL-of-Thought** (NeurIPS 2025, ~91.6% Spider) | Exact error taxonomy: 10 categories, 33 sub-types with repair directives. | Have the actual `error_taxonomy.json`. Integrate directly. |
| **PExA** (Bloomberg, 70.2% Spider 2.0) | Test-probe-BEFORE-generation: decompose question into sub-queries, execute probes on DB, use evidence to write final SQL. | Restructured pipeline: probe stage runs BEFORE SQL generation, not after. |
| **TriSQL** (Nature, 2026) | Difficulty-based routing: simple → fast path, complex → full pipeline. | 3× latency reduction on ~70% of queries. |
| **Snowflake Cortex Analyst** | Semantic View YAML format for glossary/metadata. Dimensions, measures, time grains, synonyms, verified queries, relationships. | Business glossary upgraded from simple term→SQL to full semantic model YAML. |
| **Databricks AI/BI Genie** | Knowledge Store with synonyms, sample queries, join instructions. Structured semantic metadata beats raw descriptions. | Documentation ingestion as first-class feature. |

### 1.2 Target Accuracy Trajectory

| Milestone | Accuracy | Key Driver |
|---|---|---|
| Naive GPT-4o (no pipeline) | 40-50% | Baseline — full schema dump, single-shot |
| + Schema Selection (CHESS-style) | 55-60% | 5× token reduction, focused context |
| + Few-shot from Metabase | 60-65% | Domain-specific examples |
| + Trino dialect rules | 63-68% | Eliminates syntax class errors |
| + LSH entity matching | 65-70% | Correct value resolution |
| + Graph path discovery (QueryWeaver-style) | 67-72% | Bridge table accuracy |
| + Content Awareness | 68-73% | Value format accuracy |
| + PExA test probes | 71-76% | Real data evidence before generation |
| + 3 candidates + selection (CHASE-SQL) | 75-80% | +5-7% from diversity |
| + Error taxonomy repair (SQL-of-Thought) | 77-82% | Targeted classified fixes |
| + Semantic model glossary (Snowflake-style) | 79-84% | Business logic precision |
| + Expanded few-shot + feedback loop | 82-86% | Continuous improvement |
| + Difficulty routing (TriSQL) | 83-88% | Better on easy queries, cost savings |

Ceiling without fine-tuning: ~88%. Beyond requires RLVR fine-tune on accumulated data (needs GPU — future project).

### 1.3 Cost Per Query

| Path | Cost | Latency |
|---|---|---|
| Simple (70% of queries) | $0.015-0.025 | 2-4 seconds |
| Complex (30% of queries) | $0.09-0.14 | 8-18 seconds |
| Blended average | ~$0.04 | ~5 seconds |
| Monthly at 200 users × 5 queries/day | ~$1,200 | — |

---

## 2. System Context and Constraints

### 2.1 Data Platform

- **Query engine:** Trino with Iceberg catalog
- **Tables:** 1,200+ across bronze (raw), silver (cleaned), gold (aggregated) layers
- **Catalog:** `cdp.schema_name.table_name` — 155 schemas, 8,400+ tables (including ~5,000 inactive)
- **SQL dialect:** Trino-specific — UNNEST, TRY_CAST, approx_distinct, DATE_ADD, INTERVAL syntax, FOR TIMESTAMP AS OF (time-travel), partitioned tables
- **Query patterns:** Multi-join CTEs, window functions, UNNEST of nested arrays, approximate aggregations on billion-row tables

### 2.2 Metadata Sources

| Source | What It Contains | How We Access |
|---|---|---|
| **dbt project** | Table/column descriptions, tags, `ref()` lineage graph, tests, `manifest.json` | File system / Git |
| **Metabase PostgreSQL** | 450K+ queries/week logged; saved questions = (English name, SQL) pairs | Direct PostgreSQL query |
| **Trino system tables** | `$snapshots`, `$partitions`, `$properties`, `SHOW STATS`, query logs | Trino SQL |
| **OpenMetadata** (optional) | Table/column descriptions, tags, owners, glossary terms, lineage | REST API |
| **Human-written docs** | Data dictionaries, business rules, table usage guides (Word/PDF/Markdown) | File upload |

### 2.3 Technical Constraints

- **LLM:** OpenAI API only — GPT-4o and GPT-4o-mini. No local GPU.
- **Data privacy:** Schema/column names/descriptions → OK to send to API. Actual row data values → NEVER sent to API.
- **Embeddings:** Start with `text-embedding-3-small` (1536-dim, cheap). Benchmark against `text-embedding-3-large` on 50-question held-out set. Upgrade only if measurably better retrieval.
- **Infrastructure:** Docker on Kubernetes. pgvector available. 1-2 engineers. Claude Opus via Claude Code for development.
- **Budget:** No hard cap, optimize. Target < $3,000/month at full scale.

### 2.4 Users

- 50-200 users: data analysts, engineers, product managers, business users
- Questions range from "how many active users yesterday?" to "weekly retention cohorts by acquisition channel for Q4, excluding trial users"

---

## 3. Architecture: 8-Stage Pipeline

```
User Question (English)
        │
        ▼
┌──────────────────────────┐
│  STAGE 1: ROUTER         │  GPT-4o-mini
│  Classify: SIMPLE /      │  Simple → Fast Path (1 candidate, skip Stages 4.5, 5)
│  COMPLEX / AMBIGUOUS     │  Complex → Full Path (probes + 3 candidates + selection)
└──────────────────────────┘  Ambiguous → Ask clarifying question
        │
        ▼
┌──────────────────────────┐
│  STAGE 2: CONTEXT        │  GPT-4o-mini + pgvector + LSH (local)
│  RETRIEVAL               │  • Keyword/entity extraction
│  (Information Retriever) │  • LSH entity matching (local, no API)
│                          │  • Similar Q-SQL pairs from Metabase (pgvector)
│                          │  • Business glossary / semantic model lookup (pgvector)
│                          │  • Documentation retrieval (pgvector)
│                          │  • Content Awareness metadata
└──────────────────────────┘
        │
        ▼
┌──────────────────────────┐
│  STAGE 3: SCHEMA         │  GPT-4o-mini
│  SELECTION               │  • Column filtering: 1,200 tables → ~60 candidate columns
│                          │  • Graph path discovery: NetworkX on dbt lineage
│                          │  • Bridge table injection via graph traversal
│                          │  • Table selection: → 3-8 tables
│                          │  • Final column pruning: → <15 columns
│                          │  • Content Awareness + doc snippets injected per column
└──────────────────────────┘
        │
        ▼ (Complex queries only)
┌──────────────────────────┐
│  STAGE 4: TEST PROBES    │  GPT-4o-mini + Trino execution
│  (PExA-inspired)         │  • Decompose question into 3-5 sub-questions
│                          │  • Generate simple probe queries (SELECT DISTINCT, COUNT, etc.)
│                          │  • Execute probes on Trino → collect evidence
│                          │  • "orders has 2.3M rows, statuses: completed/paid/refunded"
│                          │  • Evidence passed to generators as additional context
└──────────────────────────┘
        │
        ▼
┌──────────────────────────┐
│  STAGE 5: SQL            │  GPT-4o (complex) / GPT-4o-mini (simple)
│  GENERATION              │  Simple: 1 candidate
│  (CHASE-SQL multi-gen)   │  Complex: 3 diverse candidates:
│                          │    A: Divide-and-Conquer (sub-questions → CTEs)
│                          │    B: Execution Plan CoT (scan→filter→join→agg)
│                          │    C: Few-Shot from Metabase history
│                          │  Each gets: pruned schema + entities + probe evidence
│                          │             + Trino dialect rules + glossary defs
│                          │  Revision loop: execute → if error → classify → targeted fix
└──────────────────────────┘
        │
        ▼ (Complex queries only)
┌──────────────────────────┐
│  STAGE 6: SELECT         │  GPT-4o-mini
│  + VALIDATE              │  • Pairwise comparison (A vs B, A vs C, B vs C)
│                          │  • Error taxonomy check (10 categories, 33 sub-types)
│                          │  • Trino EXPLAIN cost guard
│                          │  • Partition pruning validation
│                          │  → Winner SQL with confidence score
└──────────────────────────┘
        │
        ▼
┌──────────────────────────┐
│  STAGE 7: EXECUTE        │  Trino (read-only user) + GPT-4o-mini
│  + RENDER                │  • Execute SQL → DataFrame
│                          │  • Auto-detect chart type
│                          │  • Generate Plotly visualization
│                          │  • Generate NL summary
└──────────────────────────┘
        │
        ▼
┌──────────────────────────┐
│  STAGE 8: RESPOND        │  • SQL (editable) + Data table (paginated)
│  + FEEDBACK              │  • Chart + NL summary
│                          │  • Confidence indicator (HIGH/MEDIUM/LOW)
│                          │  • Thumbs up/down + correction input
│                          │  • Thumbs-up → auto-add to few-shot index
│                          │  • Thumbs-down → correction review pipeline
└──────────────────────────┘
```

---

## 4. Detailed Stage Design

### 4.1 Stage 1: Difficulty Router

**Model:** GPT-4o-mini | **Cost:** ~$0.001

Classifies incoming question into SIMPLE, COMPLEX, or AMBIGUOUS.

**Prompt (template: `prompts/router_classify.txt`):**

```
You are a SQL query difficulty classifier for a Trino-Iceberg data warehouse with 1,200+ tables.

Classify the user's question into exactly one category:

SIMPLE — Single table, basic aggregation, direct lookup, straightforward filter.
Examples: "How many active users yesterday?", "Total revenue for January", "List all batches"

COMPLEX — Multiple tables needed, window functions, cohort analysis, nested logic, time-series 
comparison, ambiguous business terms requiring joins.
Examples: "Weekly retention by acquisition channel for Q4", "Compare DAU trend vs revenue trend 
last 3 months", "Top 10 batches by completion rate excluding test batches"

AMBIGUOUS — Question is underspecified, unclear what data is needed, or could mean multiple things.
Examples: "Show me the data", "How are things going?", "Tell me about users"

Question: {user_question}

Output exactly one word: SIMPLE, COMPLEX, or AMBIGUOUS
```

**Routing:**
- SIMPLE → Stages 2, 3, 5 (1 candidate, GPT-4o-mini), 7, 8. Skip Stages 4 and 6.
- COMPLEX → All 8 stages. 3 candidates with GPT-4o.
- AMBIGUOUS → Return clarifying question to user. Do not generate SQL.

### 4.2 Stage 2: Context Retrieval (Information Retriever)

**Model:** GPT-4o-mini | **Cost:** ~$0.002

Five parallel retrieval operations, all running concurrently:

#### 4.2.1 Keyword & Entity Extraction

LLM extracts from the question:
- **Domain nouns:** "active users", "revenue", "batch 42", "enterprise segment"
- **Time references:** parsed to Trino expressions ("last week" → `CURRENT_DATE - INTERVAL '7' DAY`)
- **Metric names:** matched to glossary entries ("churn rate", "DAU", "ARPU")

**Prompt (template: `prompts/ir_keyword_extract.txt`):**

```
Extract keywords from this question for a Trino data warehouse search.

Output format (strict):
KEYWORDS: keyword1, keyword2, keyword3
TIME_RANGE: <trino date expression or NONE>
METRICS: metric1, metric2 or NONE

Question: {user_question}
```

#### 4.2.2 Entity Matching via MinHash LSH (Local — No API Call)

For each extracted keyword, search the pre-built LSH index:
- "Enterprise" → matches `gold.dim_customers.segment` containing value "Enterprise"
- "Batch 42" → matches `gold.batches.batch_name` containing "Batch 42"
- Fuzzy: "Enterprize" still matches "Enterprise" (MinHash handles typos)

**Runs entirely locally.** Only matched column/table names enter the pipeline — never data values.

#### 4.2.3 Similar Query Retrieval (pgvector)

Embed user question using `text-embedding-3-small`. Search Metabase few-shot index for top-3 similar past (question, SQL) pairs.

```python
# pgvector query
SELECT question_text, sql_query, 
       1 - (embedding <=> query_embedding) as similarity
FROM metabase_questions_index
ORDER BY embedding <=> query_embedding
LIMIT 3;
```

These become in-context examples for Stage 5 generators.

#### 4.2.4 Semantic Model / Glossary Lookup (pgvector)

Search the semantic model for matching business terms:

```python
# Returns glossary entries like:
{
  "term": "active user",
  "definition": "User with at least one session in the last 30 days",
  "sql_fragment": "WHERE last_activity > CURRENT_DATE - INTERVAL '30' DAY",
  "synonyms": ["active customer", "engaged user"],
  "tables": ["gold.daily_active_users", "silver.user_sessions"],
  "time_grain": "daily"
}
```

#### 4.2.5 Documentation Retrieval (pgvector)

Search the documentation index for relevant context — human-written data dictionaries, OpenMetadata descriptions, table usage guides:

```python
# Returns doc snippets like:
{
  "source": "data_dictionary.docx",
  "table": "gold.daily_active_users",
  "content": "This table is refreshed at 3am IST. Data before 2024-01-01 is incomplete. 
               Always filter on is_deleted = false. The segment column uses lowercase values."
}
```

#### 4.2.6 Content Awareness Metadata Lookup

For each entity-matched column, retrieve its format metadata:

```json
{
  "table": "gold.batches",
  "column": "batch_id",
  "data_type": "VARCHAR",
  "format_pattern": "6-digit zero-padded string (e.g., '000042' not '42')",
  "distinct_count": 8500,
  "null_pct": 0.0,
  "sample_values": ["000001", "000042", "008500"],
  "notes": "Always use LPAD(CAST(id AS VARCHAR), 6, '0') when joining from numeric sources"
}
```

**Output of Stage 2:** Context bundle containing: keywords, entity matches, top-3 Q-SQL pairs, glossary definitions, documentation snippets, content awareness metadata.

### 4.3 Stage 3: Schema Selection

**Model:** GPT-4o-mini | **Cost:** ~$0.004

The most accuracy-critical stage. Combines CHESS's 3-stage pruning with QueryWeaver's graph traversal.

#### Step 1: Column Filtering (1,200 tables → ~60 columns)

The LLM receives:
- User question
- Entity matches from Stage 2
- Glossary definitions pointing to specific tables
- Condensed catalog: each table as one line (`table_name | one-line dbt description`)

Output: List of ~20-60 potentially relevant columns across all tables.

#### Step 2: Graph Path Discovery (Bridge Table Killer)

Using NetworkX graph built from dbt `manifest.json`:

```python
import networkx as nx
from itertools import combinations

# Pre-loaded graph: nodes = tables, edges = ref() dependencies + Metabase JOIN patterns
dbt_graph = nx.read_gpickle("table_graph.gpickle")

candidate_tables = step1_output  # e.g., ["gold.users", "gold.orders", "gold.revenue"]
full_table_set = set(candidate_tables)

for t1, t2 in combinations(candidate_tables, 2):
    try:
        path = nx.shortest_path(dbt_graph, t1, t2)
        full_table_set.update(path)  # Adds bridge/intermediate tables
    except nx.NetworkXNoPath:
        pass

# full_table_set now includes bridge tables the LLM would never have guessed
```

This solves the exact problem QueryWeaver identified: if `gold.user_engagement` and `gold.revenue` are selected, and both derive from `silver.user_sessions`, that silver table gets included even if semantically unrelated to the question.

#### Step 3: Table Selection (→ 3-8 tables)

LLM sees full candidate set (including graph-discovered bridges) with dbt descriptions and column lists. Selects 3-8 most relevant tables and explains JOIN logic.

#### Step 4: Final Column Pruning (→ <15 columns per table)

For selected tables, prune to needed columns. Inject Content Awareness + documentation per column:

```
Table: gold.daily_active_users
  Description: Daily active user counts by segment (from data_dictionary.docx: 
               refreshed 3am IST, data before 2024 incomplete, filter is_deleted=false)
  Partition: ds (DATE) — ALWAYS include in WHERE
  Columns:
    - ds (DATE) — partition key, format: YYYY-MM-DD
    - user_id (VARCHAR) — unique user identifier, never NULL
    - segment (VARCHAR) — enum: ['free', 'premium', 'enterprise'], CASE-SENSITIVE LOWERCASE
    - session_count (BIGINT) — daily sessions, can be 0, NULL means no data
    - is_deleted (BOOLEAN) — always filter: is_deleted = false
```

**Output of Stage 3:** Pruned schema with Content Awareness + documentation context + JOIN paths.

### 4.4 Stage 4: Test Probes (PExA-Inspired) — COMPLEX QUERIES ONLY

**Model:** GPT-4o-mini for probe generation | **Cost:** ~$0.003 + Trino execution

This is the key v4 change. Bloomberg's PExA showed that **exploring the database before writing SQL** dramatically improves accuracy. The system probes the data to understand what exists, then passes that evidence to the generators.

#### Step 1: Decompose into Sub-Questions

```
Question: "Show weekly retention cohorts by acquisition channel for Q4"
Sub-questions:
1. What acquisition channels exist? → SELECT DISTINCT acquisition_channel FROM gold.user_attributes LIMIT 20
2. What date range is Q4? → (computed: 2025-10-01 to 2025-12-31)
3. How many users per channel? → SELECT acquisition_channel, COUNT(*) FROM gold.user_attributes GROUP BY 1
4. What does the activity table look like? → SELECT * FROM gold.user_sessions LIMIT 5
5. Are there users with NULL channel? → SELECT COUNT(*) FROM gold.user_attributes WHERE acquisition_channel IS NULL
```

#### Step 2: Execute Probes on Trino

Run each probe query (with LIMIT, read-only, timeout 10s). Collect results as structured evidence:

```json
{
  "probes": [
    {"question": "What acquisition channels exist?", 
     "result": "5 values: organic, paid_search, referral, social, direct"},
    {"question": "Users per channel", 
     "result": "organic: 45K, paid_search: 32K, referral: 18K, social: 12K, direct: 8K"},
    {"question": "NULL channels?", 
     "result": "2,340 users have NULL acquisition_channel"},
    {"question": "Activity table columns", 
     "result": "user_id, ds, session_count, duration_minutes, platform"}
  ]
}
```

#### Step 3: Pass Evidence to Generators

This evidence becomes additional context for Stage 5. The generators now know:
- Exact enum values (don't hallucinate channel names)
- Data volumes (can judge if results look reasonable)
- NULL presence (can add COALESCE or WHERE IS NOT NULL)
- Actual column names (from LIMIT 5 output)

**Why this matters:** Without probes, the generator might write `WHERE channel = 'Organic'` when the actual value is `'organic'` (lowercase). With probes, it knows the exact values.

### 4.5 Stage 5: SQL Generation (CHASE-SQL Multi-Candidate)

**Model:** GPT-4o for complex ($0.02/candidate) | GPT-4o-mini for simple ($0.003)

#### 4.5.1 Three Generators (Complex) or One (Simple)

**Generator A: Divide-and-Conquer**

Prompt strategy: Break question into sub-questions, answer each with a CTE, combine.

```
Template: prompts/gen_divide_conquer.txt

Break this question into independent sub-questions. For each, write a CTE.
Then combine CTEs into a final SELECT.

Example:
Question: "Monthly revenue by segment for users acquired in 2024"
Sub-questions:
1. Which users were acquired in 2024? → CTE: users_2024
2. What is each user's segment? → CTE: user_segments (JOIN with users_2024)
3. What is monthly revenue per user? → CTE: monthly_rev
4. Combine: GROUP BY segment, month → Final SELECT

{trino_dialect_rules}
{pruned_schema}
{probe_evidence}
{glossary_definitions}
{few_shot_examples}

Question: {user_question}
```

Best for: multi-step analytics, cohort analysis, funnel queries.

**Generator B: Execution Plan CoT**

Prompt strategy: Think like a query engine — plan the physical execution.

```
Template: prompts/gen_execution_plan.txt

Plan this query as if you are a Trino query engine:
1. Which tables to SCAN (with partition filters)
2. What WHERE filters to apply
3. How to JOIN tables (keys, type)
4. What to GROUP BY / aggregate
5. How to ORDER / LIMIT

Then write the SQL following your plan exactly.

{trino_dialect_rules}
{pruned_schema}
{probe_evidence}
...
```

Best for: complex JOINs, queries on large tables, performance-sensitive queries.

**Generator C: Few-Shot from Metabase**

Prompt strategy: Follow patterns from similar past queries.

```
Template: prompts/gen_fewshot.txt

Here are 3 similar questions that were answered correctly in our system.
Follow their SQL patterns, adapting to the current question.

Example 1:
Question: {similar_q1}
SQL: {similar_sql1}

Example 2: ...
Example 3: ...

Now answer this question following similar patterns:
Question: {user_question}

{trino_dialect_rules}
{pruned_schema}
{probe_evidence}
```

Best for: standard reporting patterns, queries similar to existing dashboards.

#### 4.5.2 Trino Dialect Rules (Injected into ALL generators)

```
Template: prompts/trino_dialect_rules.txt

TRINO SQL RULES — FOLLOW EXACTLY:

1.  TRY_CAST(x AS type) instead of CAST — prevents query failure on bad data
2.  approx_distinct(col) instead of COUNT(DISTINCT col) for tables > 1M rows
3.  Date arithmetic: DATE_ADD('day', -30, CURRENT_DATE) — NOT CURRENT_DATE - 30
4.  Intervals: INTERVAL '30' DAY — number MUST be quoted string
5.  Time travel: FOR TIMESTAMP AS OF TIMESTAMP '2025-01-01 00:00:00'
6.  ALWAYS include partition columns (ds, dt, date) in WHERE clause
7.  Use WITH (CTE names) for multi-step queries — never nested subqueries > 2 levels
8.  String comparison is case-sensitive — use lower(col) = lower(value) if unsure
9.  Array: CROSS JOIN UNNEST(array_col) AS t(element)
10. Approximate: approx_percentile(col, 0.5), approx_most_frequent(col, k, capacity)
11. No IFNULL in Trino — use COALESCE(col, default)
12. Timestamps: TIMESTAMP '2025-01-01 00:00:00' — not string comparison
13. LIMIT goes AFTER ORDER BY — never before
14. Boolean: use col = true, not col = 1
15. DATE_TRUNC('week', ts) for weekly aggregation — first arg is string
```

#### 4.5.3 Revision Loop (Error-Aware)

After generating each candidate, execute on Trino:
- **Success:** Keep candidate.
- **Syntax/execution error:** Classify error using the taxonomy (Section 4.6.2), feed classified error + original SQL back to LLM for targeted fix. Up to 2 retries.
- **Timeout (>30s):** Suggest simplification — add LIMIT, use approximate functions, narrow date range.
- **Empty result:** Flag as suspicious, keep. Might be correct.

### 4.6 Stage 6: Selection + Validation — COMPLEX QUERIES ONLY

**Model:** GPT-4o-mini | **Cost:** ~$0.015

#### 4.6.1 Pairwise Selection (CHASE-SQL)

Compare each pair:

```
Template: prompts/val_pairwise_compare.txt

Given this question and database schema, which SQL query is more likely to 
return the correct answer?

Question: {user_question}
Schema: {pruned_schema}

Candidate A:
{sql_a}

Candidate B:
{sql_b}

Consider: correct tables, correct JOINs, correct filters, correct aggregation,
correct column names, Trino syntax compliance.

Output: A or B, with one-sentence explanation.
```

Run 3 comparisons (A vs B, A vs C, B vs C). Score each candidate. Pick winner.

Research shows pairwise selection is 14% more accurate than majority-vote self-consistency.

#### 4.6.2 Error Taxonomy Check (SQL-of-Thought — Exact Taxonomy)

The winning SQL is checked against the full SQL-of-Thought error taxonomy.

**Complete taxonomy (from `error_taxonomy.json`):**

```json
{
  "syntax": {
    "sql_syntax_error": "SQL syntax error",
    "invalid_alias": "Invalid alias reference"
  },
  "schema_link": {
    "table_missing": "Referenced table does not exist",
    "col_missing": "Referenced column does not exist",
    "ambiguous_col": "Ambiguous column reference",
    "incorrect_foreign_key": "Incorrect column used as foreign key"
  },
  "join": {
    "join_missing": "Missing JOIN condition",
    "join_wrong_type": "Incorrect join type",
    "extra_table": "Unused table in FROM/JOIN",
    "incorrect_col": "Using incorrect column name to perform join"
  },
  "filter": {
    "where_missing": "Missing WHERE for filter in question",
    "condition_wrong_col": "Condition uses wrong column",
    "condition_type_mismatch": "Type mismatch in WHERE condition"
  },
  "aggregation": {
    "agg_no_groupby": "Aggregation without GROUP BY",
    "groupby_missing_col": "Missing column in GROUP BY",
    "having_without_groupby": "HAVING used without GROUP BY",
    "having_incorrect": "Incorrect placement of HAVING",
    "having_vs_where": "Usage of HAVING confused with usage of WHERE"
  },
  "value": {
    "hardcoded_value": "Hardcoded literal instead of column",
    "value_format_wrong": "Value format incompatible"
  },
  "subquery": {
    "unused_subquery": "Subquery not used",
    "subquery_missing": "Needed subquery missing",
    "subquery_correlation_error": "Correlation error in subquery"
  },
  "set_op": {
    "union_missing": "UNION query without UNION operator",
    "intersect_missing": "INTERSECT missing",
    "except_missing": "EXCEPT missing"
  },
  "others": {
    "order_by_missing": "ORDER BY needed but missing",
    "limit_missing": "LIMIT needed but missing",
    "duplicate_select": "Duplicate columns in SELECT",
    "unsupported_function": "Function not supported",
    "incorrect_foreign_key_relationship": "Used incorrect foreign key relationship"
  },
  "select": {
    "incorrect_extra_values": "Columns or values selected are incorrect or extra",
    "incorrect_order": "Values selected are in the wrong order"
  }
}
```

**Plus 3 Trino-specific additions:**

```json
{
  "trino_specific": {
    "missing_partition_filter": "Partition column (ds/dt/date) not in WHERE — will cause full table scan",
    "cast_instead_of_try_cast": "Using CAST instead of TRY_CAST — will fail on bad data",
    "non_trino_function": "Using function not available in Trino (e.g., IFNULL, DATEADD without quotes)"
  }
}
```

**Validation prompt:**

```
Template: prompts/val_error_taxonomy.txt

Review this SQL for errors. Check EACH category below and report any issues found.

Categories to check:
- syntax: SQL syntax errors, invalid aliases
- schema_link: wrong table/column names, ambiguous columns
- join: missing JOINs, wrong join type, extra unused tables, wrong join columns
- filter: missing WHERE clauses, wrong filter columns, type mismatches
- aggregation: missing GROUP BY, wrong HAVING usage
- value: hardcoded values that should be columns, wrong value formats
- subquery: missing/unused subqueries, correlation errors
- set_op: missing UNION/INTERSECT/EXCEPT
- select: wrong columns selected, wrong order
- trino_specific: missing partition filter, CAST vs TRY_CAST, non-Trino functions

Question: {user_question}
Schema: {pruned_schema}
SQL: {winning_sql}

Output format:
ERRORS_FOUND: true/false
If true, list each error as: CATEGORY.SUBTYPE: description and fix
```

If errors found → targeted fix prompt with specific repair directive → re-validate once.

#### 4.6.3 Cost Guard

```python
# Execute EXPLAIN on winning SQL
explain_result = trino.execute(f"EXPLAIN (TYPE DISTRIBUTED) {winning_sql}")

# Check thresholds
if estimated_scan_bytes > 500_GB:
    reject("Query scans too much data. Add date filters or use a gold table.")
if not has_partition_filter(winning_sql, partitioned_tables):
    inject_partition_filter()  # Auto-add ds >= CURRENT_DATE - INTERVAL '30' DAY
if references_bronze_table(winning_sql) and gold_alternative_exists:
    suggest("Consider using gold.{table} instead of bronze.{table} for faster results.")
```

**Output of Stage 6:** Validated SQL + confidence score:
- **HIGH:** All 3 candidates agreed, no taxonomy errors, cost guard passed
- **MEDIUM:** Winner selected by margin, minor taxonomy issues fixed
- **LOW:** Candidates disagreed significantly, or taxonomy found multiple errors

### 4.7 Stage 7: Execute + Render

**Model:** GPT-4o-mini for chart detection + NL summary | **Cost:** ~$0.002

#### 4.7.1 Execute on Trino

Read-only user with resource group limits:
- Max query memory: 4GB
- Max execution time: 120 seconds
- Max rows returned: 10,000 (paginated beyond)

#### 4.7.2 Auto-Detect Chart Type

| Result Shape | Chart Type |
|---|---|
| 1 row, 1 numeric column | Number card |
| Time column + numeric column(s) | Line chart |
| Categorical column + numeric column | Bar chart (horizontal if > 8 categories) |
| Categorical + single numeric summing to ~100% | Pie chart |
| Multiple columns, multiple rows | Data table |
| 2 numeric columns | Scatter plot |
| Geographic column + numeric | Map (if supported) |

#### 4.7.3 Generate Plotly Chart

GPT-4o-mini generates Plotly chart spec based on column names + detected type. Chart rendered server-side, returned as interactive HTML.

#### 4.7.4 NL Summary

GPT-4o-mini generates 1-3 sentence summary from the SQL and result statistics (row count, min/max/avg of numeric columns — metadata only, no raw values sent to API):

> "Enterprise segment WAU grew 12% over 8 weeks, from 4,200 to 4,700 users. The strongest growth was in Week 6 (+4.2%). Three of five channels showed positive trends."

### 4.8 Stage 8: Respond + Feedback

Return to user:
1. **SQL query** — syntax-highlighted, editable, copy button
2. **Data table** — paginated, sortable, CSV download
3. **Chart** — interactive Plotly
4. **NL summary** — plain English
5. **Confidence** — HIGH (green) / MEDIUM (yellow) / LOW (red)
6. **Feedback** — thumbs up/down + optional text correction

**Feedback loop:**
- **Thumbs up:** (question, SQL) pair auto-added to Metabase few-shot index after execution verification
- **Thumbs down + correction:** Human-corrected SQL stored. Weekly batch: corrections reviewed, added to few-shot index, failure patterns added to prompt negative examples

---

## 5. Preprocessing Pipeline

Everything runs locally. No data values leave your infrastructure.

### 5.1 dbt Metadata Extraction

**Script:** `preprocessing/extract_dbt_metadata.py`

**Input:** `manifest.json` + YAML files from dbt project

**Extracts:**
- Table name, schema, description, materialization, tags
- Column name, description, data type, tests (unique, not_null, accepted_values)
- `ref()` dependency graph → NetworkX edges
- dbt test results → constraint hints

**Outputs:**
- `data/schema_catalog.json` — flat catalog of all tables with descriptions
- `data/dbt_lineage_graph.gpickle` — NetworkX directed graph of table dependencies
- pgvector: embeddings of all `"{table_name}: {table_description}. Columns: {col1} ({type1}) - {desc1}, ..."` strings

**Embedding model:** `text-embedding-3-small` (1536-dim)

### 5.2 Metabase Question Extraction

**Script:** `preprocessing/extract_metabase_questions.py`

**Input:** Direct PostgreSQL query to Metabase database

```sql
SELECT 
    rc.name AS question_text,
    (rc.dataset_query::json->>'native')::json->>'query' AS sql_query,
    rc.created_at,
    rc.updated_at
FROM report_card rc
WHERE (rc.dataset_query::json->>'type') = 'native'
  AND rc.archived = false
  AND (rc.dataset_query::json->>'native')::json->>'query' IS NOT NULL;
```

**Processing:**
- Filter: only valid Trino SQL (try parse, discard broken)
- Deduplicate: by SQL hash (different names, same SQL → keep one)
- Extract JOIN patterns: parse SQL for `JOIN ... ON` clauses → add as edges to table graph

**Outputs:**
- `data/metabase_questions.json` — (question_text, sql_query) pairs
- pgvector: embeddings of question_text for few-shot retrieval
- JOIN pattern edges added to table graph

**Expected yield:** 500-2,000 (question, SQL) pairs.

### 5.3 LSH Index Build

**Script:** `preprocessing/build_lsh_index.py`

**Input:** Trino — sample actual column values from gold + silver layers (~300 tables)

```python
from datasketch import MinHash, MinHashLSH

lsh = MinHashLSH(threshold=0.3, num_perm=128)

for table in gold_silver_tables:
    for col in categorical_columns(table):  # VARCHAR with < 10,000 distinct values
        values = trino.execute(f"SELECT DISTINCT {col} FROM {table} LIMIT 10000")
        for value in values:
            m = MinHash(num_perm=128)
            for char_ngram in ngrams(str(value).lower(), 3):
                m.update(char_ngram.encode('utf8'))
            lsh.insert(f"{table}.{col}::{value}", m)
```

**Output:** `data/lsh_index.pkl` — serialized LSH index (~50-200MB)

### 5.4 Content Awareness Build

**Script:** `preprocessing/build_content_awareness.py`

**Input:** Trino — sample data patterns per column

For each column in schema catalog:
```sql
SELECT 
    approx_distinct({col}) as distinct_count,
    COUNT(*) FILTER (WHERE {col} IS NULL) * 100.0 / COUNT(*) as null_pct,
    typeof({col}) as data_type
FROM {table}
-- For string columns with low cardinality:
SELECT DISTINCT {col} FROM {table} LIMIT 50;
-- For numeric columns:
SELECT MIN({col}), MAX({col}), AVG(TRY_CAST({col} AS DOUBLE)) FROM {table};
```

**Output:** `data/content_awareness.json`

```json
{
  "gold.daily_active_users": {
    "ds": {
      "data_type": "DATE",
      "format": "YYYY-MM-DD",
      "is_partition": true,
      "min": "2023-06-01",
      "max": "2026-03-03",
      "null_pct": 0.0,
      "note": "ALWAYS include in WHERE"
    },
    "segment": {
      "data_type": "VARCHAR",
      "distinct_count": 3,
      "enum_values": ["free", "premium", "enterprise"],
      "null_pct": 0.2,
      "case_sensitive": true,
      "note": "Values are lowercase. Use lower() for user input comparison."
    },
    "batch_id": {
      "data_type": "VARCHAR",
      "format_pattern": "6-digit zero-padded (e.g., '000042')",
      "distinct_count": 8500,
      "null_pct": 0.0,
      "note": "Use LPAD(CAST(id AS VARCHAR), 6, '0') when joining from numeric sources"
    }
  }
}
```

### 5.5 Semantic Model / Business Glossary (Snowflake Cortex-Style)

**Script:** `preprocessing/build_glossary.py` (embeds YAML in pgvector)

**Input:** Manually authored `config/semantic_model.yaml`, modeled after Snowflake Cortex Analyst.

This is the most impactful manual effort. The format:

```yaml
# config/semantic_model.yaml
# Modeled after Snowflake Cortex Analyst Semantic View YAML

name: cdp_analytics
description: "Semantic model for CDP education analytics platform"

tables:
  - name: gold.daily_active_users
    description: "Daily active user counts by segment and platform"
    base_table:
      catalog: cdp
      schema: gold_dbt
      table: daily_active_users
    dimensions:
      - name: segment
        synonyms: ["user type", "plan", "tier", "subscription type"]
        description: "User subscription segment"
        expr: segment
        data_type: VARCHAR
        is_enum: true
        enum_values: ["free", "premium", "enterprise"]
      - name: platform
        synonyms: ["device", "app"]
        description: "Platform where activity occurred"
        expr: platform
        data_type: VARCHAR
        is_enum: true
    time_dimensions:
      - name: date
        synonyms: ["day", "ds"]
        description: "Activity date"
        expr: ds
        data_type: DATE
        is_partition: true
    facts:
      - name: active_users
        synonyms: ["DAU", "daily active users", "active user count"]
        description: "Count of distinct users with at least one session"
        expr: active_user_count
        data_type: BIGINT
    metrics:
      - name: dau
        description: "Daily Active Users"
        expr: "SUM(active_user_count)"
        synonyms: ["DAU", "daily actives"]
      - name: wau
        description: "Weekly Active Users (rolling 7-day)"
        expr: "COUNT(DISTINCT user_id) over 7-day window"
        synonyms: ["WAU", "weekly actives"]

  - name: gold.orders
    description: "Completed order transactions"
    base_table:
      catalog: cdp
      schema: gold_dbt
      table: orders
    dimensions:
      - name: order_status
        synonyms: ["status", "payment status"]
        description: "Order completion status"
        expr: status
        data_type: VARCHAR
        is_enum: true
        enum_values: ["completed", "paid", "refunded", "pending", "failed"]
    time_dimensions:
      - name: order_date
        synonyms: ["purchase date", "transaction date"]
        description: "Date order was placed"
        expr: order_date
        data_type: DATE
        is_partition: true
    facts:
      - name: amount
        synonyms: ["revenue", "order value", "price", "payment"]
        description: "Order amount in INR"
        expr: amount
        data_type: DOUBLE
    metrics:
      - name: total_revenue
        description: "Total revenue from completed orders"
        expr: "SUM(amount) WHERE status IN ('completed', 'paid')"
        synonyms: ["revenue", "total sales", "GMV"]
      - name: average_order_value
        description: "Average order value"
        expr: "AVG(amount) WHERE status IN ('completed', 'paid')"
        synonyms: ["AOV", "avg order"]

relationships:
  - name: users_to_orders
    left_table: gold.daily_active_users
    right_table: gold.orders
    join_type: LEFT JOIN
    relationship_columns:
      - left_column: user_id
        right_column: user_id
    note: "Not all active users have orders"

business_rules:
  - term: "active user"
    definition: "User with at least one session in the last 30 days"
    sql_fragment: "WHERE last_activity > CURRENT_DATE - INTERVAL '30' DAY"
    synonyms: ["active customer", "engaged user", "live user"]

  - term: "churn"
    definition: "Previously active user with no activity for 90+ days"
    sql_fragment: "WHERE last_activity < CURRENT_DATE - INTERVAL '90' DAY AND was_active = true"
    synonyms: ["churned user", "lost user", "inactive user"]

  - term: "revenue"
    definition: "Total payment amount for completed/paid orders"
    sql_fragment: "SUM(amount) WHERE status IN ('completed', 'paid')"
    synonyms: ["sales", "GMV", "earnings", "income"]

  - term: "batch"
    definition: "A cohort of students enrolled together in a course"
    sql_fragment: "-- Join via batch_id (zero-padded VARCHAR)"
    synonyms: ["cohort", "class", "group", "enrollment group"]

  # Target: 50+ business rules

verified_queries:
  - name: daily_active_users_by_segment
    question: "How many active users by segment yesterday?"
    sql: |
      SELECT segment, COUNT(DISTINCT user_id) as active_users
      FROM cdp.gold_dbt.daily_active_users
      WHERE ds = CURRENT_DATE - INTERVAL '1' DAY
        AND is_deleted = false
      GROUP BY segment
      ORDER BY active_users DESC
    use_as_onboarding: true

  - name: monthly_revenue_trend
    question: "What is the monthly revenue trend for the last 6 months?"
    sql: |
      SELECT DATE_TRUNC('month', order_date) as month,
             SUM(amount) as total_revenue
      FROM cdp.gold_dbt.orders
      WHERE status IN ('completed', 'paid')
        AND order_date >= DATE_ADD('month', -6, CURRENT_DATE)
      GROUP BY 1
      ORDER BY 1
    use_as_onboarding: true

  # Target: 20+ verified queries
```

**Processing:** Each table, dimension, metric, business rule, and verified query gets embedded in pgvector. Synonyms are expanded (each synonym gets its own embedding pointing to the same entry).

### 5.6 Documentation Ingestion Layer

**Script:** `preprocessing/ingest_documentation.py`

Accepts three input types:

#### Type 1: Human-Written Docs (Word/PDF/Markdown)

```python
# Upload via web UI or drop in /data/docs/ folder
# Pipeline: extract text → chunk by section → embed → store in pgvector

for doc_file in docs_folder.glob("*.docx"):
    text = extract_text(doc_file)  # pandoc
    chunks = chunk_by_section(text, max_tokens=500)
    for chunk in chunks:
        # Attempt to associate with a table name
        table_match = match_table_reference(chunk, schema_catalog)
        embedding = embed(chunk.text)
        pgvector.insert(embedding, {
            "source": doc_file.name,
            "table": table_match,
            "content": chunk.text,
            "type": "documentation"
        })
```

#### Type 2: OpenMetadata Exports

```python
# Pull from OpenMetadata API
tables = openmetadata_client.get_tables(database="cdp")
for table in tables:
    # Merge with dbt catalog — OpenMetadata descriptions override if richer
    if len(table.description) > len(dbt_catalog[table.fqn].description):
        schema_catalog[table.fqn].description = table.description
    for col in table.columns:
        if col.description:
            schema_catalog[table.fqn].columns[col.name].description = col.description
    # Also store tags, owners, glossary terms
```

#### Type 3: Ad-Hoc Table Annotations (YAML)

```yaml
# config/table_annotations.yaml
annotations:
  gold.daily_active_users:
    warnings:
      - "Data before 2024-01-01 is incomplete due to migration"
      - "Always filter on is_deleted = false"
    refresh_schedule: "Daily at 3:00 AM IST"
    owner: "data-engineering@company.com"
    
  bronze.raw_events:
    warnings:
      - "Contains duplicates — always use silver.events instead for analytics"
      - "Schema changed on 2025-06-15 — event_params structure differs before/after"
    notes: "This table is 2TB+ — never scan without date filter"
```

These annotations are injected into Stage 3 column pruning output and Stage 5 generation prompts.

### 5.7 Table Relationship Graph (Enhanced)

**Script:** `preprocessing/build_table_graph.py`

Three edge sources combined into one NetworkX multigraph:

```python
import networkx as nx

G = nx.DiGraph()

# Source 1: dbt lineage (from manifest.json)
for model in manifest['nodes'].values():
    for dep in model.get('depends_on', {}).get('nodes', []):
        G.add_edge(dep_table_name, model_table_name, type='lineage')

# Source 2: Metabase JOIN patterns (from parsed SQL)
for question in metabase_questions:
    joins = parse_join_clauses(question['sql_query'])
    for left_table, right_table, left_col, right_col in joins:
        G.add_edge(left_table, right_table, 
                   type='join', 
                   left_col=left_col, 
                   right_col=right_col,
                   frequency=frequency_count)

# Source 3: Semantic model relationships
for rel in semantic_model['relationships']:
    G.add_edge(rel['left_table'], rel['right_table'],
               type='semantic',
               join_columns=rel['relationship_columns'])

nx.write_gpickle(G, "data/table_graph.gpickle")
```

### 5.8 Refresh Schedule

| Index | Frequency | Trigger |
|---|---|---|
| dbt metadata + embeddings | On dbt deploy (CI/CD webhook) or weekly cron |
| Metabase questions + embeddings | Weekly cron (Sunday 2am) |
| LSH index | Weekly cron (Sunday 3am) |
| Content Awareness | Weekly cron (Sunday 4am) |
| Table relationship graph | On dbt deploy or weekly cron |
| Documentation index | On file upload or weekly cron |
| Semantic model / glossary | Manual (on business logic change) |

**K8s CronJob:** `preprocessing/refresh_all.py` runs all extractors sequentially.

---

## 6. Repository Structure

```
chess-trino/
│
├── preprocessing/                         # One-time build + weekly refresh
│   ├── extract_dbt_metadata.py            # dbt → schema catalog + embeddings + lineage graph
│   ├── extract_metabase_questions.py      # Metabase PostgreSQL → Q-SQL pairs + embeddings
│   ├── build_lsh_index.py                 # Trino samples → MinHash LSH index
│   ├── build_content_awareness.py         # Trino samples → column format metadata
│   ├── build_table_graph.py               # dbt + Metabase + semantic model → NetworkX graph
│   ├── build_glossary.py                  # Embed semantic model YAML in pgvector
│   ├── ingest_documentation.py            # Word/PDF/Markdown/OpenMetadata → pgvector
│   └── refresh_all.py                     # Orchestrator for weekly refresh
│
├── src/
│   ├── pipeline.py                        # Main orchestrator: routes through all stages
│   │
│   ├── router/
│   │   └── difficulty_router.py           # Stage 1: SIMPLE / COMPLEX / AMBIGUOUS
│   │
│   ├── retrieval/                         # Stage 2: Context Retrieval
│   │   ├── information_retriever.py       # Orchestrates all 5 parallel retrievals
│   │   ├── keyword_extractor.py           # LLM keyword + entity extraction
│   │   ├── lsh_matcher.py                 # Local MinHash entity matching
│   │   ├── fewshot_retriever.py           # pgvector: similar Metabase Q-SQL pairs
│   │   ├── glossary_retriever.py          # pgvector: semantic model / glossary lookup
│   │   ├── doc_retriever.py               # pgvector: documentation snippets
│   │   └── content_awareness.py           # Column format metadata lookup
│   │
│   ├── schema/                            # Stage 3: Schema Selection
│   │   ├── schema_selector.py             # Orchestrates 4-step pipeline
│   │   ├── column_filter.py               # Step 1: broad column filtering
│   │   ├── graph_path_finder.py           # Step 2: NetworkX bridge table discovery
│   │   ├── table_selector.py              # Step 3: LLM table selection
│   │   └── column_pruner.py               # Step 4: final pruning + metadata injection
│   │
│   ├── probes/                            # Stage 4: Test Probes (PExA-inspired)
│   │   ├── probe_planner.py               # Decompose question → sub-questions
│   │   ├── probe_generator.py             # Generate simple probe queries
│   │   └── probe_executor.py              # Execute probes, collect evidence
│   │
│   ├── generation/                        # Stage 5: SQL Generation (CHASE-SQL)
│   │   ├── candidate_generator.py         # Orchestrates 1 or 3 generators
│   │   ├── divide_and_conquer.py          # Generator A: sub-questions → CTEs
│   │   ├── execution_plan_cot.py          # Generator B: scan→filter→join→agg
│   │   ├── fewshot_generator.py           # Generator C: Metabase pattern following
│   │   ├── trino_dialect.py               # Dialect rules injection
│   │   └── revision_loop.py              # Execute → classify error → targeted fix
│   │
│   ├── validation/                        # Stage 6: Selection + Validation
│   │   ├── selection_agent.py             # CHASE-SQL pairwise comparison
│   │   ├── error_taxonomy_checker.py      # SQL-of-Thought 10-category / 33-subtype + 3 Trino
│   │   └── cost_guard.py                 # EXPLAIN cost + partition pruning check
│   │
│   ├── output/                            # Stage 7: Execute + Render
│   │   ├── query_executor.py              # Trino execution with resource limits
│   │   ├── chart_detector.py              # Auto-detect chart type from result shape
│   │   ├── chart_generator.py             # Plotly chart generation
│   │   └── nl_summarizer.py               # NL summary of results
│   │
│   ├── connectors/
│   │   ├── trino_connector.py             # trino-python-client wrapper + connection pool
│   │   ├── pgvector_store.py              # pgvector CRUD (embeddings, search, insert)
│   │   └── openai_client.py               # GPT-4o / 4o-mini routing + retry + cost tracking
│   │
│   ├── feedback/
│   │   ├── rating_store.py                # Store thumbs up/down + corrections
│   │   ├── correction_pipeline.py         # Corrections → new training pairs (weekly batch)
│   │   └── accuracy_tracker.py            # Track metrics over time
│   │
│   └── safety/
│       ├── query_validator.py             # Read-only enforcement, injection prevention
│       └── data_policy.py                 # Ensure no data values leak to API
│
├── web/
│   ├── app.py                             # FastAPI main application
│   ├── routes/
│   │   ├── chat.py                        # POST /generate, POST /feedback
│   │   ├── health.py                      # GET /health, GET /metrics (Prometheus)
│   │   └── admin.py                       # GET /accuracy, POST /refresh, POST /upload-doc
│   ├── middleware/
│   │   ├── auth.py                        # User authentication (SSO)
│   │   └── rate_limiter.py                # Per-user rate limiting
│   └── ui/                                # React frontend
│
├── prompts/                               # All prompt templates — version controlled
│   ├── router_classify.txt
│   ├── ir_keyword_extract.txt
│   ├── ss_column_filter.txt
│   ├── ss_table_select.txt
│   ├── ss_column_prune.txt
│   ├── probe_decompose.txt
│   ├── probe_generate.txt
│   ├── gen_divide_conquer.txt
│   ├── gen_execution_plan.txt
│   ├── gen_fewshot.txt
│   ├── gen_revision.txt
│   ├── val_pairwise_compare.txt
│   ├── val_error_taxonomy.txt
│   ├── out_chart_detect.txt
│   ├── out_nl_summary.txt
│   └── trino_dialect_rules.txt
│
├── config/
│   ├── semantic_model.yaml                # Snowflake-style semantic model (main glossary)
│   ├── table_annotations.yaml             # Ad-hoc warnings and notes per table
│   ├── error_taxonomy.json                # SQL-of-Thought 10+3 categories / 36 sub-types
│   ├── model_routing.yaml                 # Which LLM model for which stage
│   ├── cost_guards.yaml                   # EXPLAIN thresholds, scan limits
│   └── settings.yaml                      # All other configuration
│
├── data/                                  # Generated artifacts (gitignored except structure)
│   ├── schema_catalog.json
│   ├── metabase_questions.json
│   ├── content_awareness.json
│   ├── table_graph.gpickle
│   └── lsh_index.pkl
│
├── tests/
│   ├── test_set.json                      # 100+ (question, expected_SQL, expected_tables)
│   ├── eval_accuracy.py                   # Automated accuracy evaluation
│   ├── eval_components.py                 # Per-stage unit tests
│   └── eval_retrieval.py                  # Embedding retrieval quality benchmark
│
├── k8s/
│   ├── deployment.yaml
│   ├── service.yaml
│   ├── configmap.yaml
│   └── cronjob-refresh.yaml
│
├── docker-compose.yaml                    # Local dev: FastAPI + pgvector + UI
├── Dockerfile
├── requirements.txt
└── README.md
```

---

## 7. LLM Model Routing

```yaml
# config/model_routing.yaml
stages:
  router:
    model: gpt-4o-mini
    max_tokens: 50
    temperature: 0
    cost_per_call: 0.001
    
  information_retriever:
    model: gpt-4o-mini
    max_tokens: 300
    temperature: 0
    cost_per_call: 0.001
    
  schema_selector:
    column_filter:
      model: gpt-4o-mini
      max_tokens: 1000
      temperature: 0
      cost_per_call: 0.001
    table_selector:
      model: gpt-4o-mini
      max_tokens: 800
      temperature: 0
      cost_per_call: 0.002
    column_pruner:
      model: gpt-4o-mini
      max_tokens: 600
      temperature: 0
      cost_per_call: 0.001
      
  probe_planner:
    model: gpt-4o-mini
    max_tokens: 500
    temperature: 0
    cost_per_call: 0.002
    
  sql_generator:
    simple:
      model: gpt-4o-mini
      max_tokens: 2000
      temperature: 0.0
      cost_per_call: 0.003
    complex:
      model: gpt-4o
      max_tokens: 4000
      temperature: 0.3  # slight variation for diversity across 3 generators
      cost_per_call: 0.020
      
  revision:
    model: gpt-4o
    max_tokens: 4000
    temperature: 0
    cost_per_call: 0.020
    
  selection_agent:
    model: gpt-4o-mini
    max_tokens: 200
    temperature: 0
    cost_per_call: 0.003  # per pairwise comparison
    
  error_taxonomy_checker:
    model: gpt-4o-mini
    max_tokens: 500
    temperature: 0
    cost_per_call: 0.003
    
  chart_detector:
    model: gpt-4o-mini
    max_tokens: 200
    temperature: 0
    cost_per_call: 0.001
    
  nl_summarizer:
    model: gpt-4o-mini
    max_tokens: 300
    temperature: 0.3
    cost_per_call: 0.001

embeddings:
  model: text-embedding-3-small
  dimensions: 1536
  cost_per_1k_tokens: 0.00002
```

---

## 8. Implementation Phases

### Phase 1: Core Pipeline (Weeks 1-3) — Target: 70-75%

#### Week 1: Foundation

```
[ ] Set up repo structure (as defined in Section 6)
[ ] Implement src/connectors/trino_connector.py
    - trino-python-client wrapper
    - Connection pooling
    - Read-only user enforcement
    - Resource group configuration
[ ] Implement src/connectors/pgvector_store.py
    - Create tables: schema_embeddings, question_embeddings, glossary_embeddings, doc_embeddings
    - Insert, search (cosine similarity), batch operations
[ ] Implement src/connectors/openai_client.py
    - GPT-4o / GPT-4o-mini routing based on model_routing.yaml
    - Retry with exponential backoff
    - Cost tracking per call (log model, tokens, cost)
    - Rate limit handling
[ ] Write prompts/trino_dialect_rules.txt (the 15 rules from Section 4.5.2)
[ ] Docker Compose for local dev (FastAPI + PostgreSQL/pgvector)
[ ] Basic test: hardcode a schema, send a question through OpenAI, verify SQL executes on Trino
```

#### Week 2: Preprocessing Pipeline

```
[ ] preprocessing/extract_dbt_metadata.py
    - Parse manifest.json
    - Build schema_catalog.json
    - Generate embeddings, store in pgvector
    - Build NetworkX lineage graph
[ ] preprocessing/extract_metabase_questions.py
    - Query Metabase PostgreSQL
    - Filter/deduplicate
    - Parse JOIN patterns → add to graph
    - Embed questions in pgvector
[ ] preprocessing/build_lsh_index.py
    - Sample gold + silver tables (~300 tables)
    - Build MinHash LSH with datasketch
    - Serialize to lsh_index.pkl
[ ] preprocessing/build_content_awareness.py
    - Sample column patterns from Trino
    - Build content_awareness.json
[ ] preprocessing/build_table_graph.py
    - Combine dbt lineage + Metabase JOINs
    - Save as table_graph.gpickle
[ ] config/semantic_model.yaml
    - Author initial version: 10 key tables, 30 business rules, 10 verified queries
[ ] preprocessing/build_glossary.py
    - Embed semantic model entries in pgvector
[ ] Run full preprocessing pipeline, validate all outputs
```

#### Week 3: 8-Stage Pipeline

```
[ ] src/router/difficulty_router.py — Stage 1
[ ] src/retrieval/ — Stage 2 (all 5 retrieval sub-tasks)
    - information_retriever.py (orchestrator)
    - keyword_extractor.py
    - lsh_matcher.py
    - fewshot_retriever.py
    - glossary_retriever.py + doc_retriever.py
    - content_awareness.py
[ ] src/schema/ — Stage 3 (4-step with graph path discovery)
    - schema_selector.py (orchestrator)
    - column_filter.py
    - graph_path_finder.py (NetworkX shortest path)
    - table_selector.py
    - column_pruner.py (inject Content Awareness + docs)
[ ] src/generation/ — Stage 5 (3 generators + dialect)
    - candidate_generator.py (orchestrator: 1 or 3)
    - divide_and_conquer.py
    - execution_plan_cot.py
    - fewshot_generator.py
    - trino_dialect.py
    - revision_loop.py
[ ] src/validation/selection_agent.py — Stage 6 (pairwise comparison)
[ ] src/output/query_executor.py — Stage 7 (basic: execute + return DataFrame)
[ ] src/pipeline.py — Main orchestrator wiring all stages
[ ] Build test set: extract 50 questions from Metabase with known-good SQL
[ ] Run accuracy evaluation: tests/eval_accuracy.py
[ ] Identify top 5 failure modes, document in README
```

**Exit criteria:** End-to-end pipeline working. 70-75% accuracy on 50-question test set.

### Phase 2: Probes + Validation + Web Service (Weeks 4-5) — Target: 75-80%

#### Week 4: PExA Probes + Validation + Output

```
[ ] src/probes/ — Stage 4 (PExA-inspired test probes)
    - probe_planner.py (decompose question → sub-questions)
    - probe_generator.py (sub-questions → simple SQL probes)
    - probe_executor.py (execute probes, collect evidence)
    - Wire probe evidence into Stage 5 generator prompts
[ ] src/validation/error_taxonomy_checker.py — Stage 6
    - Load error_taxonomy.json (10+3 categories, 36 sub-types)
    - Validation prompt with targeted repair
[ ] src/validation/cost_guard.py — Stage 6
    - EXPLAIN cost check
    - Partition pruning validation
    - Bronze table warning
[ ] src/output/ — Stage 7 (full)
    - chart_detector.py (auto-detect chart type)
    - chart_generator.py (Plotly generation)
    - nl_summarizer.py (NL summary)
[ ] Expand test set to 100 questions (add hard cases: multi-join, UNNEST, time-travel)
[ ] Re-run accuracy evaluation
```

#### Week 5: Web Service + UI

```
[ ] web/app.py — FastAPI application
    - POST /api/generate — full pipeline
    - POST /api/feedback — thumbs up/down + correction
    - GET /api/health — health check
    - GET /api/metrics — Prometheus metrics
    - POST /api/admin/upload-doc — documentation upload
    - POST /api/admin/refresh — trigger preprocessing refresh
[ ] web/routes/ — route handlers
[ ] web/middleware/ — auth (basic initially), rate limiter
[ ] web/ui/ — React frontend
    - Question input (text box + send button)
    - SQL display (syntax highlighted, editable, copy)
    - Data table (paginated, sortable, CSV download)
    - Chart (interactive Plotly)
    - NL summary
    - Confidence indicator (colored badge)
    - Feedback (thumbs up/down + correction text box)
    - "Ask a follow-up" button
[ ] Docker Compose: FastAPI + pgvector + React UI
[ ] K8s manifests: deployment, service, configmap
[ ] Read-only Trino user + resource group setup
[ ] Deploy to staging, invite 5-10 beta users
```

**Exit criteria:** Deployed web service. 75-80% accuracy on 100 questions. 5-10 beta users.

### Phase 3: Accuracy Optimization (Weeks 6-8) — Target: 80-85%

```
Week 6: Failure Analysis
[ ] Categorize all failures from 100-question test + beta feedback
    - Use error taxonomy categories to classify failures
    - Track: which stage failed? (retrieval? schema? generation? validation?)
[ ] Expand semantic_model.yaml based on failures (target: 50+ business rules)
[ ] Add more Metabase Q-SQL pairs to few-shot index (target: 1,000+)
[ ] Add failure-specific negative examples to prompt templates
[ ] Tune Schema Selector: adjust column count thresholds, table count limits
[ ] Re-evaluate

Week 7: Advanced Features
[ ] Query result caching (same question within 1 hour → cached)
[ ] Multi-turn conversation (follow-up questions with session context)
[ ] "Did you mean?" suggestions for AMBIGUOUS queries
[ ] Feedback loop automation:
    - Thumbs-up → verify SQL executes → auto-add to few-shot index
    - Thumbs-down + correction → queue for weekly review
[ ] preprocessing/ingest_documentation.py — upload Word/PDF docs
[ ] Documentation retrieval integrated into Stage 2

Week 8: Performance + Evaluation
[ ] Parallelize 3 generators (asyncio.gather)
[ ] A/B test: 1 candidate vs 3 candidates (quantify CHASE-SQL value)
[ ] Embedding model benchmark: test text-embedding-3-large on 50-question set
[ ] Confidence calibration: correlate score with actual accuracy
[ ] Expand test set to 200 questions
[ ] Full evaluation
```

**Exit criteria:** 80-85% on 200 questions. Multi-turn working. Caching active. 50+ users.

### Phase 4: Scale + Production (Weeks 9-12) — Target: 82-88%

```
Week 9-10: Production Hardening
[ ] SSO authentication
[ ] Row-level security (user → allowed schemas mapping)
[ ] Rate limiting (per-user: 20 queries/hour)
[ ] Prometheus + Grafana dashboards:
    - Latency (p50, p95, p99) by simple/complex
    - Accuracy (weekly evaluation on test set)
    - Cost per query (daily)
    - Cache hit rate
    - Error rate by stage
    - Top failure categories (error taxonomy distribution)
[ ] Alerting: accuracy drop > 5%, cost spike, Trino timeout spike
[ ] Graceful degradation: OpenAI down → cached responses + "temporarily limited" message

Week 11: Advanced
[ ] Auto-refresh preprocessing on dbt deploy (CI/CD webhook)
[ ] Schema change detection: alert when new tables/columns appear, auto-embed
[ ] Accumulated corrections batch: monthly reprocess all feedback into indexes
[ ] OpenMetadata integration (if available): pull richer descriptions

Week 12: Ecosystem
[ ] Slack bot: /ask [question] → SQL + chart inline in Slack
[ ] API documentation (OpenAPI spec) for programmatic access
[ ] Self-service semantic model editor (web UI for table owners to add/edit glossary)
[ ] Admin dashboard: accuracy trends, most-asked questions, cost breakdown
[ ] Load test: 200 concurrent users, measure p99 latency
[ ] Final evaluation on full 200-question test set
```

**Exit criteria:** Production-ready. 82-88% accuracy. 200+ users. Full monitoring. Slack bot.

---

## 9. Key Risks and Mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Building from scratch takes longer than forking CHESS | Medium | Medium | CHESS architecture is simple (4 sequential agents). Each agent is 1 Python file + 1 prompt template. Claude Code can generate this in days. |
| GPT-4o generates non-Trino SQL | High | Medium | Trino dialect rules in every prompt + all few-shot examples are Trino SQL + revision loop catches + error taxonomy flags non-Trino functions |
| Bridge table misses on complex joins | Medium | High | Graph path discovery on dbt lineage catches structurally required tables. Metabase JOIN patterns add edges for non-dbt relationships. |
| Value format mismatches (e.g., "42" vs "000042") | Medium | Medium | Content Awareness layer stores actual formats. PExA probes discover real values before generation. |
| Schema Selector picks wrong tables | Medium | High | 4-step pipeline (broad filter → graph → LLM select → prune) is much more robust than single-step. Probe evidence further validates table choice. |
| PExA probes add latency | Low | Low | Only for complex queries (~30%). Probes are simple queries (SELECT DISTINCT, COUNT) that execute in <1s each. Total: 2-5s added. |
| Data values leak to OpenAI API | Low | Critical | Architecture guarantees: only schema/metadata → API. LSH runs locally. Probes execute locally. Chart generation uses column names only. data_policy.py enforces. |
| Hallucinated table/column names | Medium | High | Schema Selector only presents real tables from catalog. Error taxonomy catches `table_missing` and `col_missing`. Post-validation checks all referenced tables exist in schema_catalog. |
| OpenAI rate limits at scale | Low | Medium | Retry with exponential backoff. Model routing: most calls are GPT-4o-mini (high rate limits). Simple queries use only mini. Cache for repeated questions. |
| Semantic model maintenance burden | Medium | Low | Start with 10 tables, 30 rules. Expand based on failure analysis. GPT-4o can semi-automate: "Given these Metabase questions, suggest glossary entries." |
| 1,200+ tables overwhelm Schema Selector | Low | Medium | 3-stage pruning: catalog has one-line descriptions → column filter picks ~60 → graph adds bridges → LLM selects 3-8. Never sends full catalog. |

---

## 10. Success Metrics

### Accuracy

| Metric | Definition | Target |
|---|---|---|
| Execution accuracy | % of queries that execute AND return correct results | > 80% by Phase 3 |
| Schema accuracy | % where correct tables were selected | > 90% by Phase 3 |
| Syntax pass rate | % that execute without Trino errors | > 95% by Phase 2 |
| Error taxonomy distribution | Which error categories are most common | Track weekly |

### Operational

| Metric | Target |
|---|---|
| P50 latency (simple) | < 3 seconds |
| P50 latency (complex) | < 12 seconds |
| P99 latency | < 25 seconds |
| System error rate | < 3% |
| Cache hit rate | > 15% after 1 month |
| Cost per query (blended) | < $0.05 |

### User

| Metric | Target |
|---|---|
| Thumbs-up rate | > 70% |
| Daily active users | Track adoption curve |
| Repeat usage (next week) | > 60% |
| Questions per user per day | > 3 |

---

## 11. What This Plan Does NOT Include (Future Work)

1. **RLVR fine-tuning:** Train Qwen2.5-Coder-32B on accumulated (question, SQL, execution_result) triples with execution-based rewards. Needs GPU. Would push accuracy 88% → 92%. After 6 months of data.

2. **Multi-database:** Only targets Trino-Iceberg CDP. PostgreSQL, BigQuery, etc. would need dialect adapters.

3. **Write queries:** System is strictly read-only. INSERT/UPDATE/DELETE excluded for safety.

4. **Full PageIndex integration:** Hierarchical reasoning tree over schema. Currently using NetworkX for graph traversal. If retrieval accuracy is a bottleneck in Phase 3, implement PageIndex-style tree search over the schema hierarchy.

5. **NL-to-Dashboard:** Generating full Metabase dashboard definitions from NL. Possible future extension.

6. **Self-correcting semantic model:** Auto-detect when glossary entries are wrong based on user corrections. Currently manual. Could be automated with enough feedback data.

---

*Document version: v4.0 (FINAL) | Date: March 4, 2026*
*Author: Generated for Aamir's CDP Text-to-SQL Project*
*Research synthesis: CHESS + CHASE-SQL + QueryWeaver + SQL-of-Thought + PExA + TriSQL + Snowflake Cortex + Databricks Genie + PageIndex*
*Intended audience: Claude Code for implementation execution*
