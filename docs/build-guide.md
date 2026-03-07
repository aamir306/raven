# RAVEN — Build Guide

Archive note:

- This file is a historical build sequence and is partly stale.
- Use [docs/ai-handoff.md](./ai-handoff.md) for the current implementation state.
- Use [docs/accuracy-first-10-10-roadmap.md](./accuracy-first-10-10-roadmap.md) for the active roadmap.
- Some references below point to earlier planning documents and should not be treated as current architecture authority.

## Step-by-step implementation for Claude Code / developers

---

## How to Use This File

This is the step-by-step execution guide. Follow it sequentially. Each section has:
- **What to build** — the exact files and their purpose
- **Dependencies** — what must exist before you start
- **Implementation details** — code patterns, imports, key logic
- **Validation** — how to verify it works before moving on

**Reference files (read these first):**
- `docs/ai-handoff.md` — current implementation state and active backend path
- `docs/accuracy-first-10-10-roadmap.md` — target architecture and roadmap
- `config/error_taxonomy.json` — SQL error classification (36 sub-types)
- `config/trino_dialect_rules.txt` — 20 Trino SQL rules

Historical note:
- older `injection/references/...` planning documents were part of an earlier workflow and should be treated as archive context only if they still exist locally

---

## Phase 1, Week 1: Foundation

### Step 1.1: Initialize Project

The project structure is already initialized. Key directories:

Create `requirements.txt`:
```
# Core
fastapi>=0.109.0
uvicorn>=0.27.0
pydantic>=2.5.0

# Trino
trino>=0.327.0

# OpenAI
openai>=1.12.0
tiktoken>=0.6.0

# Embeddings + Vector
pgvector>=0.2.4
psycopg2-binary>=2.9.9
numpy>=1.26.0

# LSH + Entity Matching
datasketch>=1.6.0

# Graph
networkx>=3.2.0

# Data Processing
pandas>=2.2.0
sqlparse>=0.4.4

# Charts
plotly>=5.18.0

# Async
aiohttp>=3.9.0
asyncio>=3.4.3

# Utilities
python-dotenv>=1.0.0
pyyaml>=6.0.1
structlog>=24.1.0
prometheus-client>=0.20.0
```

Create `.env.example`:
```
# OpenAI
OPENAI_API_KEY=sk-...

# Trino
TRINO_HOST=your-trino-host
TRINO_PORT=443
TRINO_USER=text2sql_readonly
TRINO_CATALOG=cdp
TRINO_SCHEMA=gold_dbt
TRINO_HTTP_SCHEME=https

# PostgreSQL (pgvector)
PGVECTOR_HOST=your-pg-host
PGVECTOR_PORT=5432
PGVECTOR_DB=text2sql
PGVECTOR_USER=text2sql
PGVECTOR_PASSWORD=...

# Metabase PostgreSQL (for extraction)
METABASE_PG_HOST=your-metabase-pg-host
METABASE_PG_PORT=5432
METABASE_PG_DB=metabase
METABASE_PG_USER=readonly
METABASE_PG_PASSWORD=...
```

Create `config/settings.yaml`:
```yaml
project:
  name: raven
  version: "1.0.0"

trino:
  max_query_memory: "4GB"
  max_execution_time_seconds: 120
  max_rows_returned: 10000
  resource_group: "text2sql"

pipeline:
  simple_path_stages: [1, 2, 3, 5, 7, 8]
  complex_path_stages: [1, 2, 3, 4, 5, 6, 7, 8]
  max_candidates_complex: 3
  max_candidates_simple: 1
  revision_max_retries: 2
  probe_timeout_seconds: 10
  probe_max_queries: 5

schema_selector:
  max_candidate_columns: 60
  max_selected_tables: 8
  max_columns_per_table: 15

cost_guard:
  max_scan_bytes_gb: 500
  require_partition_filter: true
  warn_bronze_table: true

cache:
  enabled: true
  ttl_seconds: 3600

feedback:
  auto_add_thumbs_up: true
  require_execution_verify: true
```

**Validation:** `pip install -r requirements.txt` completes without errors.

---

### Step 1.2: Build Connectors

These are the 3 foundational modules everything else depends on. Build them first, test them independently.

#### 1.2.1: `src/connectors/trino_connector.py`

```python
"""
Trino connection wrapper.

Key requirements:
- Read-only enforcement: reject any INSERT/UPDATE/DELETE/DROP/ALTER/CREATE
- Connection pooling via trino-python-client
- Resource group limits (configured in settings.yaml)
- Methods: execute(sql) → DataFrame, explain(sql) → dict, test_connection() → bool
"""
```

**Implementation details:**
- Use `trino.dbapi.connect()` with `http_scheme`, `auth` (BasicAuthentication or JWT)
- Parse SQL before execution: use `sqlparse` to detect statement type. Reject non-SELECT.
- Allow EXPLAIN, DESCRIBE, SHOW as read-only operations.
- Wrap results in pandas DataFrame
- Handle Trino errors: `TrinoUserError`, `TrinoQueryError` — extract error message for revision loop
- Add timeout via `request_timeout` parameter
- Log every query: query_id, sql (first 200 chars), duration, rows_returned, error (if any)

**Test:** Connect to Trino, run `SELECT 1`, verify returns DataFrame with value 1.

#### 1.2.2: `src/connectors/pgvector_store.py`

```python
"""
pgvector wrapper for embedding storage and similarity search.

Tables to create on init:
- schema_embeddings: id, table_name, column_name, description, embedding vector(1536), metadata jsonb
- question_embeddings: id, question_text, sql_query, embedding vector(1536), source varchar, created_at
- glossary_embeddings: id, term, definition, sql_fragment, synonyms text[], embedding vector(1536)
- doc_embeddings: id, source_file, table_ref, content, embedding vector(1536), doc_type varchar

Methods:
- init_tables() — CREATE EXTENSION vector; CREATE TABLE IF NOT EXISTS ...
- insert(table, text, embedding, metadata) → id
- batch_insert(table, items) → count
- search(table, query_embedding, top_k=5, filter=None) → list[dict]
- delete_by_source(table, source) — for re-indexing
"""
```

**Implementation details:**
- Use `psycopg2` with `pgvector.psycopg2.register_vector()`
- Index: `CREATE INDEX ON {table} USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);`
- Search query: `SELECT *, 1 - (embedding <=> %s) as similarity FROM {table} ORDER BY embedding <=> %s LIMIT %s`
- Connection pooling via `psycopg2.pool.ThreadedConnectionPool`

**Test:** Create tables, insert one embedding, search for it, verify it returns.

#### 1.2.3: `src/connectors/openai_client.py`

```python
"""
OpenAI API client with model routing and cost tracking.

Reads config/model_routing.yaml to determine which model for which stage.
Tracks cost per call in structured log.

Methods:
- complete(prompt, stage_name, system_prompt=None) → str
- embed(text) → list[float]  (using text-embedding-3-small)
- batch_embed(texts) → list[list[float]]  (chunked, max 2048 per batch)
- get_cost_summary() → dict  (total cost by stage)
"""
```

**Implementation details:**
- Use `openai.AsyncOpenAI` for async support
- Model routing: load `model_routing.yaml`, lookup by stage_name
- Retry: 3 attempts with exponential backoff (1s, 2s, 4s) on RateLimitError, APITimeoutError
- Cost tracking: calculate from token counts using pricing table
  - gpt-4o: $2.50/1M input, $10.00/1M output
  - gpt-4o-mini: $0.15/1M input, $0.60/1M output
  - text-embedding-3-small: $0.02/1M tokens
- Log every call: stage, model, input_tokens, output_tokens, cost, latency_ms
- For embed: chunk texts into batches of 2048, handle rate limits

**Create `config/model_routing.yaml`:** Copy from v4 plan Section 7.

**Test:** Call complete() with "Say hello", verify response. Call embed() with "test", verify 1536-dim vector returned.

---

### Step 1.3: Build Safety Module

#### `src/safety/query_validator.py`

```python
"""
Validates SQL before execution.

Methods:
- validate_read_only(sql) → bool  (reject INSERT/UPDATE/DELETE/DROP/ALTER/CREATE/TRUNCATE/MERGE)
- validate_no_data_leak(prompt) → bool  (check prompt doesn't contain known data patterns)
- sanitize_for_api(text) → str  (strip any accidentally included data values)
"""
```

Use `sqlparse.parse(sql)` to get statement type. Whitelist: SELECT, WITH, EXPLAIN, DESCRIBE, SHOW.

#### `src/safety/data_policy.py`

```python
"""
Ensures no actual data values are sent to OpenAI API.

Methods:
- check_prompt(prompt, schema_catalog) → bool
  Scans prompt for patterns that look like data values rather than metadata.
  Flags: numeric strings > 6 digits, email patterns, phone patterns, 
  anything not in schema_catalog column/table names.
- strip_data_values(text) → str
  Replaces suspected data values with [REDACTED]
"""
```

**Validation:** Write a unit test that attempts to send a prompt with fake data values, verify it's caught.

---

### Step 1.4: Write Core Prompt Templates

Create all 16 files in `prompts/`. Each is a plain text file with placeholders `{variable_name}`.

**Priority order (build these first, rest can wait):**

#### `prompts/trino_dialect_rules.txt`
Copy from `config/trino_dialect_rules.txt` (already created).

#### `prompts/router_classify.txt`
```
You are a SQL query difficulty classifier for a Trino-Iceberg data warehouse.

Classify the user's question into exactly one category:

SIMPLE — Single table, basic aggregation, direct lookup, straightforward filter.
Examples:
- "How many active users yesterday?" → SIMPLE
- "Total revenue for January" → SIMPLE
- "List all batches" → SIMPLE
- "What is the average order value?" → SIMPLE

COMPLEX — Multiple tables, window functions, cohort analysis, nested logic, time-series, 
          ambiguous terms requiring joins, comparisons across dimensions.
Examples:
- "Weekly retention by acquisition channel for Q4" → COMPLEX
- "Compare DAU trend vs revenue trend last 3 months" → COMPLEX
- "Top 10 batches by completion rate excluding test batches" → COMPLEX
- "Show me funnel conversion rates by platform" → COMPLEX

AMBIGUOUS — Underspecified, unclear what data is needed, could mean multiple things.
Examples:
- "Show me the data" → AMBIGUOUS
- "How are things going?" → AMBIGUOUS
- "Tell me about users" → AMBIGUOUS

Question: {user_question}

Output EXACTLY one word: SIMPLE, COMPLEX, or AMBIGUOUS
```

#### `prompts/ir_keyword_extract.txt`
```
Extract search keywords from this question for a Trino data warehouse.

Rules:
- Extract domain-specific nouns and terms (e.g., "active users", "revenue", "batch")
- Parse time references into Trino date expressions
- Identify metric names that might match business glossary entries
- Do NOT include generic SQL words (SELECT, COUNT, etc.)

Output format (strict — follow exactly):
KEYWORDS: keyword1, keyword2, keyword3
TIME_RANGE: <trino date expression> or NONE
METRICS: metric1, metric2 or NONE
ENTITIES: entity1, entity2 or NONE

Example:
Question: "How many enterprise users were active last week?"
KEYWORDS: enterprise, users, active
TIME_RANGE: CURRENT_DATE - INTERVAL '7' DAY
METRICS: active users, DAU
ENTITIES: enterprise

Question: {user_question}
```

#### `prompts/ss_column_filter.txt`
```
You are a schema selector for a Trino-Iceberg data warehouse with {table_count} tables.

Given the user's question and search context, identify ALL potentially relevant columns.
Cast a wide net — it's better to include too many than miss a critical column.

User question: {user_question}

Search context:
- Entity matches: {entity_matches}
- Glossary matches: {glossary_matches}
- Similar past queries used these tables: {fewshot_tables}

Available tables (name | description):
{condensed_catalog}

Output format (strict):
List each relevant column as: table_name.column_name — reason
Include 20-60 columns. Do NOT include columns with no plausible connection to the question.
```

#### `prompts/ss_table_select.txt`
```
You are selecting the final set of tables needed to answer this question.

User question: {user_question}

Candidate tables (from column filtering + graph path discovery):
{candidate_tables_with_descriptions}

For each table, you see: table name, description, relevant columns with types.

Select 3-8 tables that are needed to answer the question.
For each selected table, explain the JOIN logic (which columns to join on).

Output format (strict):
SELECTED_TABLES:
1. table_name — reason for inclusion — JOIN: table_a.col = table_b.col
2. ...

JOIN_PATH:
table_a JOIN table_b ON a.col = b.col
table_b JOIN table_c ON b.col = c.col
```

#### `prompts/ss_column_prune.txt`
```
You are pruning columns for SQL generation. Keep ONLY columns needed to answer the question.

User question: {user_question}

Selected tables and ALL their columns:
{selected_tables_full_columns}

Content Awareness metadata:
{content_awareness}

Documentation notes:
{doc_snippets}

For each table, output ONLY the columns needed (max 15 per table).
Include the column's Content Awareness info (data type, format, enum values, NULL rate, notes).

Output format (strict):
TABLE: table_name
  - column_name (TYPE) — description. Format: X. Enum: [a, b, c]. NULL: X%. Note: X.
  - ...
TABLE: ...
```

#### `prompts/probe_decompose.txt`
```
Decompose this question into 3-5 simple sub-questions that can be answered with basic SQL queries.
The purpose is to probe the database to understand what data exists before writing the final SQL.

User question: {user_question}
Available tables: {selected_tables_summary}

Good probe questions:
- "What distinct values exist in column X?" → SELECT DISTINCT col FROM table LIMIT 20
- "How many rows in the table?" → SELECT COUNT(*) FROM table WHERE partition_filter
- "What's the date range?" → SELECT MIN(date_col), MAX(date_col) FROM table
- "Are there NULLs?" → SELECT COUNT(*) FILTER (WHERE col IS NULL) FROM table
- "What does the data look like?" → SELECT * FROM table LIMIT 5

Output format (strict):
PROBE 1: [sub-question]
SQL: [simple SELECT query with LIMIT]
PROBE 2: ...
(3-5 probes total)

Rules:
- Every probe MUST include partition filter if table is partitioned
- Every probe MUST have LIMIT (max 50)
- Only SELECT queries (no INSERT/UPDATE/DELETE)
- Keep probes fast — simple aggregations, DISTINCT, LIMIT
```

#### `prompts/gen_divide_conquer.txt`
```
You are Generator A: Divide-and-Conquer.

Break the question into independent sub-questions. Answer each with a CTE (WITH clause).
Combine all CTEs into a final SELECT.

{trino_dialect_rules}

Schema:
{pruned_schema}

Probe evidence (real data from the database):
{probe_evidence}

Business glossary matches:
{glossary_definitions}

Similar past queries (follow these patterns):
{few_shot_examples}

Question: {user_question}

Step 1: List the sub-questions (2-5)
Step 2: Write a CTE for each sub-question
Step 3: Write the final SELECT combining all CTEs

Output the complete SQL query only. No explanation.
```

#### `prompts/gen_execution_plan.txt`
```
You are Generator B: Execution Plan Chain-of-Thought.

Think like a Trino query engine. Plan the execution step by step, then write SQL.

{trino_dialect_rules}

Schema:
{pruned_schema}

Probe evidence:
{probe_evidence}

Business glossary matches:
{glossary_definitions}

Similar past queries:
{few_shot_examples}

Question: {user_question}

Plan (think through each step):
1. SCAN: Which tables to read? What partition filters?
2. FILTER: What WHERE conditions from the question?
3. JOIN: How to connect tables? Which keys? What join type?
4. AGGREGATE: What GROUP BY? What aggregation functions?
5. SORT/LIMIT: What ORDER BY? Any LIMIT?

Now write the SQL following your plan exactly.
Output the complete SQL query only. No explanation.
```

#### `prompts/gen_fewshot.txt`
```
You are Generator C: Few-Shot Pattern Follower.

Here are similar questions that were answered correctly in this system.
Follow their SQL patterns, adapting to the current question.

{trino_dialect_rules}

Schema:
{pruned_schema}

Probe evidence:
{probe_evidence}

Business glossary matches:
{glossary_definitions}

--- Example 1 ---
Question: {similar_q1}
SQL: {similar_sql1}

--- Example 2 ---
Question: {similar_q2}
SQL: {similar_sql2}

--- Example 3 ---
Question: {similar_q3}
SQL: {similar_sql3}

Now answer this question following similar patterns:
Question: {user_question}

Output the complete SQL query only. No explanation.
```

#### `prompts/gen_revision.txt`
```
The SQL query below failed with an error. Fix it.

Original question: {user_question}
Schema: {pruned_schema}

Failed SQL:
{failed_sql}

Error type: {error_category}.{error_subtype}
Error message: {error_message}

{trino_dialect_rules}

Fix the SQL to resolve the {error_category} error.
Specific guidance for {error_subtype}: {error_description}

Output the corrected SQL query only. No explanation.
```

#### `prompts/val_pairwise_compare.txt`
```
Compare two SQL queries. Which is more likely to correctly answer the question?

Question: {user_question}
Schema: {pruned_schema}

Candidate A:
{sql_a}

Candidate B:
{sql_b}

Check each:
- Correct tables selected?
- Correct JOIN conditions?
- Correct WHERE filters matching the question?
- Correct aggregation (GROUP BY, SUM/COUNT/AVG)?
- Correct column names?
- Trino syntax compliant?
- Partition filter included?

Output format (strict):
WINNER: A or B
REASON: [one sentence]
```

#### `prompts/val_error_taxonomy.txt`
```
Review this SQL for errors using the error taxonomy below.

Question: {user_question}
Schema: {pruned_schema}
Content Awareness: {content_awareness}

SQL to review:
{sql}

Error categories to check:
1. syntax: SQL syntax errors, invalid aliases
2. schema_link: wrong/missing table or column names, ambiguous columns, wrong FK
3. join: missing JOINs, wrong join type, extra unused tables, wrong join columns
4. filter: missing WHERE clauses, wrong filter columns, type mismatches
5. aggregation: missing GROUP BY, wrong HAVING usage, HAVING vs WHERE confusion
6. value: hardcoded values that should be columns, wrong value formats
7. subquery: missing/unused/correlated subquery errors
8. set_op: missing UNION/INTERSECT/EXCEPT
9. select: wrong columns selected, wrong column order
10. trino_specific: missing partition filter, CAST vs TRY_CAST, non-Trino functions

Output format (strict):
ERRORS_FOUND: true or false
If true:
ERROR 1: category.subtype — description — FIX: specific fix instruction
ERROR 2: ...
```

#### `prompts/out_chart_detect.txt`
```
Determine the best chart type for this query result.

SQL: {sql}
Column names and types: {column_info}
Row count: {row_count}
Sample values (first 3 rows): {sample_summary}

Chart types:
- NUMBER_CARD: single row, single numeric value
- LINE_CHART: time column + numeric column(s), showing trend
- BAR_CHART: categorical column + numeric column, comparing categories
- HORIZONTAL_BAR: same as bar but > 8 categories
- PIE_CHART: categorical + numeric that sums to approximately 100%
- SCATTER: two numeric columns
- TABLE: many columns or no clear visualization pattern

Output format (strict):
CHART_TYPE: <type>
X_AXIS: <column_name> or NONE
Y_AXIS: <column_name or list> or NONE
TITLE: <descriptive chart title>
```

#### `prompts/out_nl_summary.txt`
```
Write a 1-3 sentence natural language summary of these query results.

Question: {user_question}
SQL: {sql}
Result summary:
- Row count: {row_count}
- Columns: {column_names}
- Numeric summaries: {numeric_summaries}

Rules:
- Be specific with numbers (e.g., "grew 12% from 4,200 to 4,700")
- Mention the most notable finding
- Keep to 1-3 sentences
- If comparing periods, mention the trend direction
- Use plain business language, no SQL jargon

Output the summary only. No preamble.
```

**Validation:** All 16 files exist in `prompts/`. Each has at least one `{placeholder}`.

---

### Step 1.5: Copy Config Files

#### `config/error_taxonomy.json`
Already created in skill. Copy from `config/error_taxonomy.json`.

#### `config/model_routing.yaml`
Copy from v4 plan Section 7.

#### `config/cost_guards.yaml`
```yaml
thresholds:
  max_scan_gb: 500
  max_execution_seconds: 120
  max_rows: 10000

partition_tables:
  # Tables that MUST have partition filter in WHERE
  # Format: table_name: partition_column
  "cdp.gold_dbt.daily_active_users": "ds"
  "cdp.gold_dbt.orders": "order_date"
  "cdp.gold_dbt.video_stats": "ds"
  "cdp.silver.events": "dt"
  "cdp.silver.user_sessions": "ds"
  # Add more as discovered

bronze_alternatives:
  # When user references bronze table, suggest gold/silver alternative
  "cdp.bronze.raw_events": "cdp.silver.events"
  "cdp.bronze.raw_orders": "cdp.gold_dbt.orders"
  # Add more as discovered
```

**Validation:** All config files parseable. `yaml.safe_load()` and `json.load()` succeed on each.

---

### Step 1.6: Docker Compose for Local Dev

Create `docker-compose.yaml`:
```yaml
version: '3.8'
services:
  api:
    build: .
    ports:
      - "8000:8000"
    env_file: .env
    volumes:
      - .:/app
    command: uvicorn web.app:app --host 0.0.0.0 --port 8000 --reload
    depends_on:
      - pgvector

  pgvector:
    image: pgvector/pgvector:pg16
    ports:
      - "5433:5432"
    environment:
      POSTGRES_DB: text2sql
      POSTGRES_USER: text2sql
      POSTGRES_PASSWORD: dev_password
    volumes:
      - pgdata:/var/lib/postgresql/data

volumes:
  pgdata:
```

Create `Dockerfile`:
```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["uvicorn", "web.app:app", "--host", "0.0.0.0", "--port", "8000"]
```

**Validation:** `docker-compose up -d pgvector` starts. Connect to PostgreSQL on port 5433.

---

### Step 1.7: End-of-Week-1 Integration Test

Create `tests/test_basic.py`:
```python
"""
Basic integration test: manually send a question through the pipeline.

1. Connect to Trino — verify connection
2. Connect to pgvector — verify tables created
3. Call OpenAI — verify API key works
4. Hardcode a small schema (2 tables)
5. Send a simple question through: keyword extraction → SQL generation → execution
6. Verify: SQL executes on Trino, returns DataFrame
"""
```

**Exit criteria for Week 1:**
- [ ] All 3 connectors work independently (Trino, pgvector, OpenAI)
- [ ] Safety module rejects non-SELECT queries
- [ ] All 16 prompt templates exist
- [ ] All config files parse correctly
- [ ] Docker Compose starts API + pgvector
- [ ] Integration test: one hardcoded question → SQL → Trino execution → DataFrame

---

## Phase 1, Week 2: Preprocessing Pipeline

### Step 2.1: `preprocessing/extract_dbt_metadata.py`

```python
"""
Input: dbt manifest.json + YAML files
Output: data/schema_catalog.json, data/dbt_lineage_graph.gpickle, pgvector embeddings

Steps:
1. Parse manifest.json → extract all nodes (models, sources)
2. For each node: table_name, schema, description, columns (name, type, description, tests)
3. Build NetworkX DiGraph: for each model, add edges from depends_on nodes
4. Save schema_catalog.json (flat dict: fqn → {description, columns, tags, materialization})
5. Save dbt_lineage_graph.gpickle
6. Generate embeddings for each table: "{table_name}: {description}. Columns: {col1} ({type}) - {desc}, ..."
7. Batch insert embeddings into pgvector schema_embeddings table
"""
```

**Key detail:** The dbt manifest path and structure should be configurable. The user's dbt project has well-documented YAMLs — the descriptions are the primary source of schema context.

**Validation:** `schema_catalog.json` has 1,200+ entries. Graph has nodes and edges. pgvector has embeddings.

### Step 2.2: `preprocessing/extract_metabase_questions.py`

```python
"""
Input: Metabase PostgreSQL database (direct connection)
Output: data/metabase_questions.json, pgvector embeddings, JOIN pattern edges for graph

Steps:
1. Query Metabase for native SQL saved questions (see SQL in v4 plan Section 5.2)
2. Filter: only valid Trino SQL (try sqlparse.parse, discard failures)
3. Deduplicate: hash SQL, keep unique
4. Extract JOIN patterns: parse each SQL for JOIN ... ON clauses
   - Use sqlparse to find JOIN tokens and ON conditions
   - Extract: (left_table, right_table, left_col, right_col) tuples
   - Count frequency of each join pattern
5. Save metabase_questions.json: [{question_text, sql_query, tables_used, created_at}]
6. Embed question_text, insert into pgvector question_embeddings table
7. Return join_patterns list for graph builder
"""
```

**Expected yield:** 500-2,000 (question, SQL) pairs. This is the training gold mine.

**Validation:** metabase_questions.json has 500+ entries. pgvector has question embeddings.

### Step 2.3: `preprocessing/build_lsh_index.py`

```python
"""
Input: Trino — sample column values from gold + silver layer tables
Output: data/lsh_index.pkl

Steps:
1. Load schema_catalog.json to get list of gold + silver tables (~300)
2. For each table, identify categorical columns:
   - VARCHAR/CHAR type
   - approx_distinct < 10,000 (query Trino)
3. For each categorical column:
   - SELECT DISTINCT {col} FROM {table} LIMIT 10000
   - For each value: create MinHash (128 perms, char 3-grams)
   - Insert into LSH index: key = "{table}.{col}::{value}", value = MinHash
4. Serialize LSH index to data/lsh_index.pkl using pickle

Config:
- LSH threshold: 0.3 (for fuzzy matching)
- MinHash num_perm: 128
- N-gram size: 3 (char-level)
"""
```

**Note:** This step queries Trino extensively. Batch tables, add delays to avoid overloading cluster. Start with gold layer only, add silver in a second pass.

**Validation:** lsh_index.pkl exists, ~50-200MB. Query for "Enterprise" returns matches.

### Step 2.4: `preprocessing/build_content_awareness.py`

```python
"""
Input: Trino — sample data patterns per column
Output: data/content_awareness.json

Steps:
1. Load schema_catalog.json
2. For each table in gold + silver layers:
   For each column:
   a. Query: approx_distinct, null percentage, typeof
   b. If string + low cardinality (< 50 distinct): SELECT DISTINCT LIMIT 50 → enum_values
   c. If numeric: SELECT MIN, MAX, AVG
   d. If date: SELECT MIN, MAX
   e. Detect format patterns: zero-padding, case sensitivity, special characters
3. Save as data/content_awareness.json (nested: table → column → metadata)

Schema per column:
{
  "data_type": "VARCHAR",
  "distinct_count": 3,
  "null_pct": 0.2,
  "enum_values": ["free", "premium", "enterprise"],  // if low cardinality
  "min": null, "max": null,  // if numeric/date
  "format_pattern": "lowercase string",  // detected pattern
  "is_partition": false,
  "notes": "Case-sensitive. Values are lowercase."  // auto-generated note
}
"""
```

**Validation:** content_awareness.json has entries for 300+ tables with column metadata.

### Step 2.5: `preprocessing/build_table_graph.py`

```python
"""
Input: dbt lineage graph + Metabase JOIN patterns + semantic model relationships
Output: data/table_graph.gpickle (NetworkX DiGraph)

Steps:
1. Load dbt_lineage_graph.gpickle (from Step 2.1)
2. Load join_patterns from metabase extraction (from Step 2.2)
3. Load config/semantic_model.yaml relationships
4. Merge all edges into single graph:
   - dbt edges: type='lineage'
   - Metabase edges: type='join', with left_col, right_col, frequency
   - Semantic model edges: type='semantic', with join_columns
5. Save as data/table_graph.gpickle

This graph is used in Stage 3 for bridge table discovery:
  nx.shortest_path(G, table_a, table_b) → finds intermediate tables
"""
```

**Validation:** Graph has 300+ nodes (tables), 500+ edges. Test: shortest_path between two known tables returns expected path.

### Step 2.6: `config/semantic_model.yaml` + `preprocessing/build_glossary.py`

**Manual step:** Author initial `config/semantic_model.yaml` with:
- 10 key gold tables (with dimensions, time_dimensions, facts, metrics, synonyms)
- 30 business rules (term + definition + sql_fragment + synonyms)
- 10 verified queries (question + SQL + use_as_onboarding)
- 5 relationships (explicit join columns between key tables)

See v4 plan Section 5.5 for the complete format with examples.

**build_glossary.py:**
```python
"""
Embeds semantic_model.yaml entries into pgvector.

Steps:
1. Parse semantic_model.yaml
2. For each table: embed "{table_name}: {description}. Dimensions: ... Metrics: ..."
3. For each business_rule: embed "{term}: {definition}. Synonyms: {synonyms}"
   Also embed each synonym separately pointing to same entry
4. For each verified_query: embed question_text, store with SQL
5. Insert all into glossary_embeddings table
"""
```

**Validation:** pgvector glossary_embeddings has 100+ entries. Search for "active user" returns matching glossary entry.

### Step 2.7: `preprocessing/refresh_all.py`

```python
"""
Orchestrator: runs all preprocessing scripts in sequence.
Used by K8s CronJob for weekly refresh.

Order:
1. extract_dbt_metadata (depends on: dbt manifest)
2. extract_metabase_questions (depends on: Metabase PostgreSQL)
3. build_lsh_index (depends on: schema_catalog, Trino)
4. build_content_awareness (depends on: schema_catalog, Trino)
5. build_table_graph (depends on: dbt graph, metabase joins, semantic model)
6. build_glossary (depends on: semantic_model.yaml)

Each step:
- Logs start/end time
- Reports count of items processed
- Catches errors without stopping pipeline (log and continue)
- Total runtime estimate: 30-60 minutes
"""
```

**Exit criteria for Week 2:**
- [ ] All preprocessing scripts run without error
- [ ] data/ folder contains: schema_catalog.json, metabase_questions.json, lsh_index.pkl, content_awareness.json, table_graph.gpickle
- [ ] pgvector has populated: schema_embeddings, question_embeddings, glossary_embeddings
- [ ] refresh_all.py completes end-to-end

---

## Phase 1, Week 3: Full Pipeline

### Step 3.1: Implement Each Stage

Build in this order (each depends on the previous):

1. **`src/router/difficulty_router.py`** — Stage 1. Simple: load prompt, call OpenAI, parse response.

2. **`src/retrieval/`** — Stage 2. Build information_retriever.py as orchestrator that runs 5 sub-modules in parallel (asyncio.gather):
   - keyword_extractor.py → OpenAI call
   - lsh_matcher.py → local LSH lookup (pickle load)
   - fewshot_retriever.py → pgvector search
   - glossary_retriever.py → pgvector search
   - content_awareness.py → JSON file lookup

3. **`src/schema/`** — Stage 3. Build schema_selector.py as orchestrator for 4 sequential steps:
   - column_filter.py → OpenAI call with condensed catalog
   - graph_path_finder.py → NetworkX shortest_path (pure Python, no LLM)
   - table_selector.py → OpenAI call with candidates + descriptions
   - column_pruner.py → OpenAI call, inject content awareness + docs

4. **`src/generation/`** — Stage 5 (skip Stage 4 probes for now). Build candidate_generator.py:
   - For SIMPLE: call one generator (fewshot_generator.py is best default)
   - For COMPLEX: call all 3 in parallel (asyncio.gather)
   - Each generator: load its prompt template, fill placeholders, call OpenAI
   - revision_loop.py: execute SQL on Trino → if error → classify with taxonomy → call gen_revision prompt → retry (max 2)

5. **`src/validation/selection_agent.py`** — Stage 6. Pairwise comparison of 3 candidates.

6. **`src/output/query_executor.py`** — Stage 7. Execute on Trino, return DataFrame.

7. **`src/pipeline.py`** — Main orchestrator wiring all stages together (see code pattern in SKILL.md).

### Step 3.2: Build Test Set

Create `tests/test_set.json` with 50 questions extracted from Metabase saved questions:

```python
"""
Steps:
1. Load metabase_questions.json
2. Select 50 diverse questions covering:
   - 25 SIMPLE (single table, basic aggregation)
   - 20 COMPLEX (multi-table, window functions, cohorts)
   - 5 edge cases (UNNEST, time-travel, approx functions)
3. For each: record question_text, expected_tables, expected_sql (from Metabase)
4. Manually verify 10 most important ones have correct expected SQL
"""
```

### Step 3.3: Accuracy Evaluation

Create `tests/eval_accuracy.py`:

```python
"""
Run full pipeline on test set, measure accuracy.

For each question in test_set.json:
1. Run pipeline.generate(question)
2. Check: did SQL execute without error? (syntax_pass)
3. Check: were expected tables selected? (schema_accuracy)
4. Check: does SQL contain expected fragments? (fragment_match)
5. Execute both generated SQL and expected SQL, compare results (execution_accuracy)

Output:
- Overall accuracy (execution_accuracy)
- Schema accuracy
- Syntax pass rate
- Per-category breakdown
- Top 5 failure modes with examples
"""
```

**Exit criteria for Week 3 / Phase 1:**
- [ ] Full pipeline works end-to-end: question → SQL → Trino execution → DataFrame
- [ ] Simple and complex paths both functional
- [ ] 50-question test set built
- [ ] Accuracy evaluation runs, reports metrics
- [ ] Target: 70-75% execution accuracy
- [ ] Top 5 failure modes documented

---

## Phase 2, Week 4: Probes + Validation + Output

### Step 4.1: PExA Test Probes (Stage 4)

Build `src/probes/`:
- **probe_planner.py**: Load `prompts/probe_decompose.txt`, call OpenAI, parse sub-questions + SQL
- **probe_generator.py**: Validate generated probe SQLs (must be SELECT, must have LIMIT, must have partition filter)
- **probe_executor.py**: Execute each probe on Trino (timeout 10s), collect results as structured evidence dict

Wire into pipeline.py: Stage 4 runs after Stage 3, before Stage 5. Evidence dict passed to all generators.

### Step 4.2: Error Taxonomy Checker (Stage 6)

Build `src/validation/error_taxonomy_checker.py`:
- Load `config/error_taxonomy.json`
- Load `prompts/val_error_taxonomy.txt`
- Call OpenAI with winning SQL + taxonomy categories
- Parse response: ERRORS_FOUND true/false, list of errors
- If errors found: call gen_revision.txt with classified error type → re-validate once

### Step 4.3: Cost Guard (Stage 6)

Build `src/validation/cost_guard.py`:
- Run `EXPLAIN (TYPE DISTRIBUTED)` on winning SQL
- Parse output for estimated scan size
- Check against thresholds in `config/cost_guards.yaml`
- Check partition filter presence for known partitioned tables
- If bronze table detected, suggest gold/silver alternative

### Step 4.4: Output Layer (Stage 7)

Build `src/output/`:
- **chart_detector.py**: Load `prompts/out_chart_detect.txt`, call OpenAI, parse chart type
- **chart_generator.py**: Given chart type + DataFrame, generate Plotly figure (JSON spec)
- **nl_summarizer.py**: Load `prompts/out_nl_summary.txt`, call OpenAI with result stats, return summary

---

## Phase 2, Week 5: Web Service + UI

### Step 5.1: FastAPI Application

Build `web/app.py` + `web/routes/`:

```
POST /api/generate
  Body: { question: str, session_id: str? }
  Response: { sql, data (first 100 rows), chart (plotly json), summary, confidence, query_id }

POST /api/feedback
  Body: { query_id: str, rating: "up"|"down", correction_sql: str? }
  Response: { status: "ok" }

GET /api/health
  Response: { status, trino: bool, pgvector: bool, openai: bool }

GET /api/metrics
  Response: Prometheus format metrics

POST /api/admin/upload-doc
  Body: multipart file (docx/pdf/md)
  Response: { status, chunks_indexed: int }

POST /api/admin/refresh
  Response: { status, duration_seconds: float }
```

### Step 5.2: React UI

Minimal but functional:
- Text input box + Send button
- Loading spinner during generation
- Results panel: tabs for SQL / Data / Chart / Summary
- SQL tab: syntax-highlighted, copy button, editable
- Data tab: paginated table, sort columns, CSV download button
- Chart tab: interactive Plotly chart
- Summary tab: NL text
- Confidence badge: colored (green/yellow/red)
- Feedback: thumbs up/down buttons + text correction field
- History sidebar: past questions in this session

### Step 5.3: Deploy

- Docker Compose: API + pgvector + React (nginx)
- K8s manifests: deployment (2 replicas), service (ClusterIP), configmap, cronjob (weekly refresh)
- Read-only Trino user configured
- Invite 5-10 beta testers

**Exit criteria for Phase 2:**
- [ ] PExA probes working for complex queries
- [ ] Error taxonomy catching and classifying errors
- [ ] Cost guard rejecting expensive queries
- [ ] Charts and summaries generating correctly
- [ ] Web UI functional with all tabs
- [ ] Deployed, 5-10 users testing
- [ ] Target: 75-80% accuracy on 100-question test set

---

## Phases 3-4: Optimization + Production

See v4 plan Sections 8.3 and 8.4 for detailed checklists. Key tasks:

**Phase 3 (Weeks 6-8):**
- Failure analysis → expand glossary, few-shot, prompts
- Multi-turn conversation support
- Query caching
- Feedback loop automation
- Documentation ingestion (preprocessing/ingest_documentation.py)
- Parallelize generators (asyncio)
- A/B test 1 vs 3 candidates
- Expand test set to 200 questions

**Phase 4 (Weeks 9-12):**
- SSO auth, row-level security, rate limiting
- Prometheus + Grafana monitoring
- Slack bot integration
- Admin dashboard
- Auto-refresh on dbt deploy
- Load testing at 200 concurrent users
- Final evaluation: target 82-88%

---

## Quick Reference: File → Stage Mapping

| Stage | Files | Prompt Template |
|---|---|---|
| 1 Router | `src/router/difficulty_router.py` | `router_classify.txt` |
| 2 Retrieval | `src/retrieval/*.py` (6 files) | `ir_keyword_extract.txt` |
| 3 Schema | `src/schema/*.py` (4 files) | `ss_column_filter.txt`, `ss_table_select.txt`, `ss_column_prune.txt` |
| 4 Probes | `src/probes/*.py` (3 files) | `probe_decompose.txt` |
| 5 Generation | `src/generation/*.py` (6 files) | `gen_divide_conquer.txt`, `gen_execution_plan.txt`, `gen_fewshot.txt`, `gen_revision.txt` |
| 6 Validation | `src/validation/*.py` (3 files) | `val_pairwise_compare.txt`, `val_error_taxonomy.txt` |
| 7 Output | `src/output/*.py` (4 files) | `out_chart_detect.txt`, `out_nl_summary.txt` |
| 8 Response | `web/routes/chat.py` | — |

## Quick Reference: What Calls What

```
pipeline.py
├── router.classify(question) → SIMPLE/COMPLEX/AMBIGUOUS
├── retriever.retrieve(question) → context_bundle
│   ├── keyword_extractor → OpenAI
│   ├── lsh_matcher → local pickle
│   ├── fewshot_retriever → pgvector
│   ├── glossary_retriever → pgvector
│   └── content_awareness → local JSON
├── schema_selector.select(question, context) → pruned_schema
│   ├── column_filter → OpenAI
│   ├── graph_path_finder → NetworkX (no LLM)
│   ├── table_selector → OpenAI
│   └── column_pruner → OpenAI
├── [COMPLEX] probe_executor.probe(question, schema) → evidence
│   ├── probe_planner → OpenAI
│   └── probe_executor → Trino (multiple simple queries)
├── generator.generate(question, schema, context, evidence) → candidates
│   ├── [SIMPLE] fewshot_generator → OpenAI (1 candidate)
│   └── [COMPLEX] 3× parallel → OpenAI (3 candidates)
│       ├── divide_and_conquer
│       ├── execution_plan_cot
│       └── fewshot_generator
├── [per candidate] revision_loop → Trino execute → if error → classify → OpenAI fix → retry
├── [COMPLEX] validator.select_and_validate(candidates) → winner
│   ├── selection_agent (3 pairwise comparisons) → OpenAI
│   ├── error_taxonomy_checker → OpenAI
│   └── cost_guard → Trino EXPLAIN
├── executor.execute(winner.sql) → DataFrame
├── chart_gen.generate(dataframe) → Plotly JSON
│   └── chart_detector → OpenAI
└── summarizer.summarize(question, sql, result) → NL text
    └── nl_summarizer → OpenAI
```
