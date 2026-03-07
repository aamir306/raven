# GPT-5.4 Codebase Evaluation Prompt — RAVEN

Current-state note:

- Before using this prompt, first read `docs/ai-handoff.md` and `docs/accuracy-first-10-10-roadmap.md`.
- This prompt still contains some historical architecture framing and some legacy file references.
- Treat the active runtime path in `docs/ai-handoff.md` as the source of truth if this prompt conflicts with the repo's current implementation state.

> **How to use**: Copy everything below the `---` line into ChatGPT (GPT-5.4 Thinking). Upload the **entire repo as a zip**, or use the Codex "Open repo" feature pointing at `https://github.com/aamir306/raven`. Say **"think hard about this"** in the prompt to trigger deep reasoning mode (xhigh effort).

---

Think hard about this. Take your time — use extended reasoning.

## Role

You are a principal-level software architect and performance engineer conducting a comprehensive code audit of a production text-to-SQL system. Your analysis must be concrete: every finding must cite a specific file, function, and line range, and every recommendation must include implementation-ready code or a clear refactoring plan.

## System Under Review: RAVEN

RAVEN is an 8-stage text-to-SQL pipeline for a Trino-Iceberg CDP with 1,200+ tables across bronze/silver/gold layers. It takes natural language → SQL → data → chart → NL summary.

### Architecture

```
Stage 1: Router        → SIMPLE / COMPLEX / AMBIGUOUS classification
Stage 2: Retrieval     → keywords, LSH entities, few-shot, glossary, docs, content awareness, OM semantic search
Stage 3: Schema Select → 4-step pruning: column filter (pgvector) → table selector → graph expansion (NetworkX/OM lineage) → column pruner
Stage 4: Probes        → decompose question → probe queries on Trino → collect evidence [COMPLEX only]
Stage 5: Generation    → 3 diverse SQL candidates (or 1 for SIMPLE) with Trino dialect rules
Stage 6: Validation    → pairwise selection + 36-type error taxonomy correction [COMPLEX only]
Stage 7: Execute       → Trino execution + Plotly chart + NL summary
Stage 8: Feedback      → thumbs up/down → few-shot store + OM Knowledge Center write-back
```

### Tech Stack
- Python 3.13, FastAPI, asyncio
- React 18 + antd 5.x frontend (plain JS, not TypeScript)
- OpenAI GPT-4o (generation) + GPT-4o-mini (routing, extraction, summarization)
- pgvector (5 tables: schema/question/glossary/doc embeddings + query_log) with text-embedding-3-large (3072-dim)
- NetworkX DiGraph for table lineage (fallback when OpenMetadata unavailable)
- OpenMetadata 1.12 MCP integration (semantic search, lineage, glossary, column profiles, quality checks, write-back)
- Metabase MCP integration (dashboard/card creation via @cognitionai/metabase-mcp-server)
- 13 preprocessing scripts (~4,075 lines) for dbt metadata, Metabase questions, LSH index, glossary, docs

### Codebase Size
| Layer | Files | Lines |
|---|---|---|
| `src/raven/` (core engine) | 61 .py | ~8,500 |
| `web/routes/` (FastAPI backend) | 1 file | 1,082 |
| `web/ui/src/` (React frontend) | 28 .js | ~4,500 |
| `preprocessing/` | 13 .py | ~4,075 |
| **Total** | ~105 files | ~18,300 |

### Key Files to Focus On (by impact)
| File | Lines | Why It Matters |
|---|---|---|
| `src/raven/pipeline.py` | 664 | Orchestrator — all 8 stages, PipelineContext, error handling |
| `src/raven/semantic_assets.py` | active | Semantic asset loading, trusted query evidence, domain-pack access |
| `src/raven/planning/deterministic_planner.py` | active | Deterministic plan formation for trusted/planned lanes |
| `src/raven/planning/query_plan.py` | active | Typed query plans |
| `src/raven/sql/ast_builder.py` | active | Narrow AST-style deterministic SQL construction |
| `src/raven/sql/trino_compiler.py` | active | Deterministic Trino SQL compilation |
| `src/raven/connectors/openmetadata_mcp.py` | 736 | NEW — OM MCP client, all tool methods, composite operations |
| `src/raven/connectors/metabase_mcp.py` | 515 | NEW — Metabase MCP client, stdio JSON-RPC subprocess |
| `src/raven/connectors/pgvector_store.py` | 481 | Embedding store — all 5 tables, search, upsert |
| `src/raven/retrieval/information_retriever.py` | 305 | Stage 2 — parallel retrieval with OM fallback |
| `src/raven/schema/schema_selector.py` | 320 | Stage 3 — 4-step schema pruning with OM lineage |
| `src/raven/query_families/matcher.py` | active | Trusted query-family matching |
| `src/raven/query_families/compiler.py` | active | Trusted query-family SQL reuse/rewrites |
| `src/raven/generation/revision_loop.py` | 154 | Error correction with taxonomy |
| `src/raven/validation/candidate_selector.py` | 277 | Stage 6 — pairwise tournament selection |
| `src/raven/validation/query_plan_validator.py` | active | Plan-aware SQL validation |
| `src/raven/validation/execution_judge.py` | active | Post-execution sanity checks and abstention support |
| `src/raven/focus.py` | 394 | Focus Mode — domain scoping, document enhancement |
| `web/routes/__init__.py` | 1,082 | ALL API routes in one file |
| `web/ui/src/App.js` | 768 | Entire React app in one component |
| `src/raven/feedback/collector.py` | 263 | Feedback loop + OM write-back |

Historical note:
- `src/raven/generation/sql_generator.py`, `src/raven/schema/selector.py`, and `src/raven/retrieval/context_retriever.py` still exist but should be treated as legacy or secondary unless the current handoff doc says otherwise.

### Config Files
- `config/error_taxonomy.json` — 36 error sub-types across 10 categories (syntax, schema_link, join, filter, aggregation, value, subquery, set_op, others, select, trino_specific)
- `config/trino_dialect_rules.txt` — 20 Trino-specific SQL rules (TRY_CAST, approx_distinct, DATE_ADD, partition filters, etc.)
- `config/openmetadata.yaml` — OM connection + feature flags + fallback paths
- `config/metabase_mcp.yaml` — Metabase MCP config + viz mapping

### Dependencies
fastapi, uvicorn, pydantic, openai, trino[sqlalchemy], psycopg2-binary, pgvector, pandas, numpy, networkx, datasketch, sqlparse, pyyaml, httpx, structlog, aiohttp

---

## Evaluation Dimensions

Analyze the codebase across ALL of the following dimensions. For each, provide:
1. **Current state** (what exists, with file:line citations)
2. **Issues found** (severity: Critical / High / Medium / Low)
3. **Specific fix** (code snippet or refactoring plan, not vague advice)

### 1. Latency & Performance

The pipeline processes user queries through up to 8 sequential LLM calls. Analyze:

- **LLM call fan-out**: How many OpenAI API calls does a COMPLEX query make? Map every `openai_client` call across all 8 stages. Where can calls be parallelized with `asyncio.gather()` that aren't already?
- **Embedding search**: pgvector queries happen in `column_filter.py`, `glossary_retriever.py`, `fewshot_retriever.py`, `doc_retriever.py`, `context_retriever.py`. Are these parallelized? Could they share a single connection pool?
- **OpenMetadata overhead**: The new OM MCP integration (`openmetadata_mcp.py`) adds HTTP calls into Stage 2 and Stage 3. Analyze the fallback pattern — is the availability check (`_check_om_available`) cached properly? Is there a thundering-herd risk on timeout?
- **Metabase MCP subprocess**: `metabase_mcp.py` manages a stdio subprocess with an asyncio.Lock. Analyze: is the lock contention a bottleneck? Could requests be pipelined? What happens if the subprocess dies mid-request?
- **Probe execution**: Stage 4 runs SQL probe queries on Trino. How many probes per question? Is there a timeout/circuit-breaker? Check `probe_runner.py` and `probe_executor.py`.
- **NetworkX graph**: `graph_path_finder.py` loads a pickled DiGraph. Is it loaded once or per-request? Memory footprint for 1,200+ table graph?

### 2. Accuracy & SQL Quality

- **Schema linking accuracy**: The 4-step pruning (column_filter → table_selector → graph_path_finder → column_pruner) is the most accuracy-critical path. Analyze scoring functions, thresholds, and potential failure modes. Are there hardcoded thresholds that should be configurable?
- **Prompt engineering**: Read every system prompt and user prompt template in `sql_generator.py`, `candidate_generator.py`, `execution_plan_cot.py`, `divide_and_conquer.py`. Evaluate:
  - Are Trino dialect rules (`trino_dialect_rules.txt`) injected consistently?
  - Is the glossary surfaced in the right format?
  - Are few-shot examples selected by semantic similarity or random?
  - Is the schema presented in CREATE TABLE format or something else?
- **Error taxonomy effectiveness**: `error_taxonomy_checker.py` implements 36 error types. How does it interact with `revision_loop.py`? Is the revision loop bounded? What's the max retry count?
- **Candidate diversity**: CHASE-SQL's value comes from diverse candidates. Check `candidate_generator.py` — does it actually generate diverse SQL (different JOINs, CTEs vs subqueries, etc.) or just temperature-varied copies?
- **Pairwise selection**: `candidate_selector.py` does tournament-style selection. Analyze: bias toward first candidate? Token-length bias? Is the selection agent prompt well-designed?

### 3. Reliability & Error Handling

- **Pipeline error propagation**: In `pipeline.py`, what happens when Stage 3 (Schema Selection) returns zero tables? Does Stage 5 get a meaningful error or silently produce garbage SQL?
- **LLM output parsing**: Every stage parses LLM JSON responses. Audit all JSON parsing — is there consistent error handling? What happens on malformed JSON? Check for bare `json.loads()` without try/except.
- **Connection resilience**: 
  - Trino connector (`trino_connector.py`) — retry logic? Connection pooling?
  - pgvector (`pgvector_store.py`) — connection lifecycle? Pool exhaustion?
  - OpenAI client (`openai_client.py`) — rate limiting? Exponential backoff?
  - OM MCP client — session management? What happens on 401/403?
- **Subprocess management**: `metabase_mcp.py` spawns a Node.js subprocess. What happens on OOM? Is there a watchdog? How is cleanup handled on server shutdown?
- **Data validation**: Check all Pydantic models. Are request/response schemas strict enough? Any `dict` or `Any` types that should be typed?

### 4. Security

- **SQL injection**: The system generates SQL from user input. Trace the path from user question to Trino execution. Is there any path where user text could be interpolated into SQL without parameterization?
- **Data access control**: `data_policy.py` and `query_validator.py` — what do they actually enforce? Can a user query PII columns? Is there row-level security?
- **API authentication**: `web/routes/__init__.py` — is there any auth on the API endpoints? Or is everything open?
- **Secret management**: Check for hardcoded API keys, tokens, or passwords anywhere. Check `.env` handling, YAML config env interpolation.
- **OM/Metabase credentials**: How are PAT tokens and API keys stored and rotated?

### 5. Code Architecture & Maintainability

- **Monolith routes**: `web/routes/__init__.py` is 1,082 lines — all routes in one file. Propose a router decomposition.
- **Monolith frontend**: `App.js` is 768 lines — entire React app in one component. Propose component extraction.
- **Duplicate code**: Are there duplicate patterns between `schema_selector.py` (320 lines) and `selector.py` (237 lines)? Between `metabase_client.py` (276 lines, legacy) and `metabase_mcp.py` (515 lines, new)?
- **Import cycles**: With 61 Python files, are there any circular import risks? Especially with the new `connectors/__init__.py` importing from submodules?
- **Configuration sprawl**: Config is split across YAML files, environment variables, and hardcoded defaults. Map all configuration sources and propose a unified approach.
- **Testing gaps**: Check `pytest` test files (if any). What's the test coverage? Which critical paths are untested?

### 6. Scalability

- **Concurrent users**: The pipeline is async but does it handle concurrent requests properly? Is there shared mutable state in `Pipeline` or its stages?
- **Memory**: NetworkX graph + LSH index + in-memory caches. What's the memory footprint? Could it cause OOM at scale?
- **pgvector at scale**: 1,200+ tables × columns → how many embedding rows? Query performance at scale? Index type (IVFFlat vs HNSW)?
- **Preprocessing efficiency**: 13 scripts, ~4,075 lines. Some run > 1 hour. Which stages are candidates for incremental updates instead of full rebuilds?

### 7. Observability

- **Logging**: Is `structlog` used consistently? Are pipeline stages emitting structured events with correlation IDs?
- **Metrics**: `metrics.py` (231 lines) — what does it track? Are latency percentiles, error rates, and LLM token usage captured?
- **Tracing**: Is there distributed tracing (OpenTelemetry)? Can you trace a single user query through all 8 stages?
- **Cost tracking**: LLM costs are significant. Is there a `cost_guard.py` (147 lines) — does it actually prevent runaway costs?

---

## Output Format

Structure your response as:

```
## Executive Summary
[3-5 sentence overview of codebase health, top 3 risks, top 3 opportunities]

## Critical Findings (fix immediately)
[Each with file:line, severity, impact, fix]

## High-Priority Improvements  
[Each with file:line, effort estimate, expected impact]

## Architecture Recommendations
[Structural changes with migration plan]

## Performance Optimization Roadmap
[Ordered by impact/effort ratio, with estimated latency savings]

## Quick Wins (< 1 hour each)
[Low-effort, high-value changes]
```

Be brutally honest. No filler. Every recommendation must be actionable with a specific code change. If something is well-designed, say so briefly and move on — spend your tokens on problems and solutions.
