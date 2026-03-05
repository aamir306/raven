# Text-to-SQL Project — Conversation Memory

## Last Updated: March 5, 2026

---

## Project Overview

Building a production text-to-SQL system for a Trino-Iceberg CDP platform (1,200+ tables, bronze/silver/gold layers). System takes English questions → SQL + data + chart + NL summary.

---

## Final Architecture Decision: v4 (Build from Scratch)

### NOT forking CHESS. Building our own 8-stage pipeline.

**Why:** CHESS GitHub repo is dead — 9 commits, last Nov 2024, tightly coupled to SQLite. CHASE-SQL has no public code (paper only, confirmed). We use both as architectural blueprints and build clean Python.

### The 8-Stage Pipeline

```
Stage 1: Difficulty Router (TriSQL) → SIMPLE / COMPLEX / AMBIGUOUS
Stage 2: Context Retrieval (CHESS IR) → keywords, LSH entities, few-shot, glossary, docs, content awareness
Stage 3: Schema Selection (CHESS SS + QueryWeaver) → 4-step pruning with NetworkX graph bridge table discovery
Stage 4: Test Probes (PExA) → decompose question, execute probe queries on Trino, collect evidence [COMPLEX ONLY]
Stage 5: SQL Generation (CHASE-SQL) → 3 diverse candidates (or 1 for simple) with Trino dialect rules
Stage 6: Selection + Validation (CHASE-SQL + SQL-of-Thought) → pairwise selection + 36-type error taxonomy [COMPLEX ONLY]
Stage 7: Execute + Render → Trino execution + Plotly chart + NL summary
Stage 8: Respond + Feedback → SQL + table + chart + summary + confidence + thumbs up/down
```

### Key Design Decisions

| Decision | Choice | Reasoning |
|---|---|---|
| Core architecture | Build from scratch, CHESS as blueprint | CHESS repo dead, SQLite-coupled |
| Multi-candidate generation | CHASE-SQL 3-generator approach | +5-7% accuracy. No code exists, build from paper. |
| Schema linking | CHESS 3-stage pruning + QueryWeaver graph traversal | Graph finds bridge tables vector search misses |
| Pre-generation probes | PExA test-probe-before-generate | Bloomberg's insight: explore DB before writing SQL, not just validate after |
| Error correction | SQL-of-Thought exact taxonomy (10+3 categories, 36 sub-types) | Classified repair > generic "fix this error" |
| Difficulty routing | TriSQL-inspired | 3× latency reduction on 70% of queries |
| Glossary format | Snowflake Cortex Analyst Semantic View YAML | Dimensions, facts, metrics, synonyms, time grains, verified queries, relationships |
| Embedding model | text-embedding-3-large (3072-dim) via Azure OpenAI | Separate Azure endpoint, deployment: embedlarge |
| Documentation | First-class ingestion: Word/PDF/Markdown + OpenMetadata + YAML annotations | Databricks + Snowflake both learned: structured metadata beats raw descriptions |
| Graph database | NetworkX (no FalkorDB) | dbt lineage + Metabase JOINs give us the graph for free |
| Vector DB | pgvector (existing) | Already available, sufficient for embeddings |
| LLM | OpenAI GPT-4o (generation) + GPT-4o-mini (everything else) | Only available API, no GPU |

---

## Research Systems Evaluated

### Systems We STEAL From

| System | Score | What We Take | What We Skip |
|---|---|---|---|
| **CHESS** (Stanford) | 71.10% BIRD | 4-agent architecture, prompt template patterns | SQLite code, ChromaDB |
| **CHASE-SQL** (Apple) | 73.0% BIRD | 3-generator + pairwise selection design | N/A — paper only, no code |
| **QueryWeaver** (FalkorDB) | No benchmark | Graph schema traversal, Content Awareness layer | FalkorDB dependency, their SQL generator |
| **SQL-of-Thought** | ~91.6% Spider | Exact error_taxonomy.json (10 cats, 33 sub-types) | Their full agent framework |
| **PExA** (Bloomberg) | 70.2% Spider 2.0 | Test-probe-before-generate pattern | Proprietary implementation (paper pending) |
| **TriSQL** (Nature 2026) | N/A | Difficulty-based routing | Their specific implementation |
| **Snowflake Cortex** | Production | Semantic View YAML format for glossary | Their managed service |
| **Databricks Genie** | Production | Knowledge Store concept, structured metadata emphasis | Their managed service |
| **PageIndex** (VectifyAI) | 98.7% FinanceBench | Hierarchical tree index (Phase 3/4 future upgrade) | Not needed initially |

### Systems We EVALUATED but REJECTED

| System | Why Rejected |
|---|---|
| **Vanna AI** | Convenience wrapper, no benchmark scores, basic vector RAG, no schema linking |
| **MAC-SQL** | Superseded by CHESS + CHASE-SQL combination |
| **DAIL-SQL** | Prompt engineering only, no multi-agent architecture |
| **CodeS** | Requires fine-tuning + GPU, not applicable to our constraints |

---

## Key Research Findings

### Finding 1: CHESS is dead
- 9 commits total, last Nov 13, 2024
- SQLite-hardwired (Spider benchmark databases)
- No active maintenance, few open issues
- **Impact:** Build from scratch, don't fork

### Finding 2: CHASE-SQL has no code
- Paper only (arXiv/ICLR). Confirmed: no GitHub release, no open implementation.
- Leaderboards list as "code: None"
- **Impact:** Implement 3-generator + pairwise selection from paper description

### Finding 3: PExA is probe-BEFORE-generate (not validate-after)
- Bloomberg's key insight: Planner decomposes question → Test-Case Generator executes probes on DB → SQL Proposer uses evidence to write SQL
- This is "reason before writing" not "check after writing"
- **Impact:** Added Stage 4 (probes) between Schema Selection and SQL Generation

### Finding 4: Databricks + Snowflake converge on same lesson
- Both emphasize: structured semantic metadata >> raw schema descriptions
- Snowflake: Semantic Views YAML (dimensions, facts, metrics, synonyms)
- Databricks: Knowledge Store with curated descriptions, sample queries, join instructions
- **Impact:** Upgraded glossary from simple term→SQL to full Snowflake-style semantic model

### Finding 5: SQL-of-Thought error taxonomy
- Exact taxonomy: 10 categories, 33 sub-types (from error_taxonomy.json)
- Categories: syntax, schema_link, join, filter, aggregation, value, subquery, set_op, others, select
- We added 3 Trino-specific: missing_partition_filter, cast_instead_of_try_cast, non_trino_function
- Total: 13 categories, 36 sub-types

### Finding 6: Embedding model — no clear SQL-specific winner
- Industry guidance: start with text-embedding-3-small, benchmark later
- text-embedding-3-large may or may not improve SQL retrieval
- **Decision:** Start small, benchmark on 50-question held-out set in Phase 3

### Finding 7: Spider 2.0 reality check
- GPT-4 alone: ~6% on Spider 2.0 (enterprise queries)
- This proves: for 1,200-table Trino env, naive prompting is useless
- Multi-agent approach is mandatory, not optional

---

## Accuracy Targets

| Phase | Timeline | Target | Actual | Key Drivers |
|---|---|---|---|---|
| Phase 1 | Weeks 1-3 | 70-75% | — | Core pipeline + dbt + Metabase few-shot + Trino dialect |
| Phase 2 | Weeks 4-5 | 75-80% | — | PExA probes + error taxonomy + chart/summary |
| Phase 3 | Weeks 6-8 | 80-85% | **53.3%** (30q sample) | Expanded glossary + feedback loop + more few-shot |
| Phase 5 | Weeks 13-16 | — | — | UI redesign + open-source sanitization |
| Phase 6 | Weeks 17-20 | — | **in progress** | Prometheus metrics + data quality tooling |
| Phase 4 | Weeks 9-12 | 82-88% | — | Production hardening + accumulated corrections |
| Future | 6+ months | 88-92% | — | RLVR fine-tune Qwen2.5-Coder-32B (needs GPU) |

**Key gap:** 53.3% actual vs 80-85% target. Root cause: 99.6% of tables have no descriptions, 100% of content awareness stats are null. Data quality population (auto_describe + content awareness) is the highest-leverage next step.

---

## Cost Model

| Path | Cost/Query | Latency |
|---|---|---|
| Simple (70%) | $0.015-0.025 | 2-4s |
| Complex (30%) | $0.09-0.14 | 8-18s |
| Blended | ~$0.04 | ~5s |
| Monthly (200 users × 5/day) | ~$1,200 | — |

---

## Technical Constraints (Locked)

- **LLM:** Azure OpenAI (GPT-4o deployment `gpt4o` via APIM gateway). No GPU.
- **Data privacy:** Schema/metadata → API OK. Row values → NEVER to API.
- **DB:** Trino-Iceberg, 1,200+ tables, 155 schemas, bronze/silver/gold
- **Vector DB:** pgvector on AWS RDS PostgreSQL (antondb)
- **Infra:** Docker on K8s, 1-2 engineers, Claude Opus via Claude Code
- **Budget:** No hard cap, target < $3,000/month at full scale

---

## 🔒 SECURITY RESTRICTIONS — MANDATORY

**NEVER commit credentials to Git. NEVER include secrets in any committed file.**

- All credentials live in `.env` (which is in `.gitignore`)
- `.env.example` contains placeholder values only (`REPLACE_ME`, `changeme`, etc.)
- **NEVER** put real API keys, passwords, or connection strings in:
  - Python source files
  - YAML/JSON config files
  - Docker Compose files
  - K8s manifests (use `stringData: REPLACE_ME`)
  - Markdown documentation
  - Commit messages
  - Test files
- Before every `git add` / `git commit`, verify no secrets are staged
- Use `os.getenv()` / `dotenv` to read credentials at runtime
- Real credentials: Azure OpenAI key, Trino password, pgvector password — all in `.env` only

---

## Preprocessing Artifacts

| Artifact | Source | Refresh |
|---|---|---|
| schema_catalog.json | dbt manifest.json + YAML | On dbt deploy / weekly |
| metabase_questions.json | Metabase PostgreSQL | Weekly |
| lsh_index.pkl | Trino column samples (gold+silver) | Weekly |
| content_awareness.json | Trino column patterns | Weekly |
| table_graph.gpickle | dbt lineage + Metabase JOINs + semantic model | On dbt deploy / weekly |
| semantic_model.yaml | Manual authoring (Snowflake Cortex format) | On business logic change |
| table_annotations.yaml | Manual (warnings, notes per table) | As needed |
| pgvector embeddings | All of the above | With each source refresh |
| documentation index | Word/PDF/Markdown/OpenMetadata uploads | On upload / weekly |

---

## Document Versions

| Version | Date | Key Change | File |
|---|---|---|---|
| v1 | Mar 3, 2026 | Initial plan with Vanna AI | text-to-sql-implementation-plan.md |
| v2 | Mar 3, 2026 | Pivoted to CHESS + CHASE-SQL | text-to-sql-chess-implementation-plan-v2.md |
| v3 | Mar 4, 2026 | Added QueryWeaver graph, SQL-of-Thought, PExA, TriSQL | text-to-sql-final-plan-v3.md |
| **v4 (FINAL)** | **Mar 4, 2026** | **Build from scratch (not fork). PExA probes before generation. Full error taxonomy. Snowflake-style semantic model. Documentation ingestion.** | **text-to-sql-v4-final.md** |

---

## Conversation Evolution (Decision Trail)

1. **Initial research session** (Mar 3): Deep research identified CHESS, CHASE-SQL, RLVR, multi-agent frameworks. Initial recommendation was Vanna AI.

2. **Pivot from Vanna to CHESS** (Mar 3): User challenged whether Vanna was truly best. Correct answer: No. Vanna is a convenience wrapper with no benchmarks. CHESS (71.10% BIRD) is the actual SOTA open-source. Built v2 plan.

3. **User's own research** (Mar 4): User uploaded Clarifying Questions doc covering SQL-of-Thought, PExA, TriSQL, MAC-SQL, Arctic-Text2SQL, Databricks, Snowflake. Also pointed to PageIndex and QueryWeaver.

4. **QueryWeaver analysis** (Mar 4): Graph-based schema traversal solves bridge table problem. Adopted the concept using NetworkX on dbt lineage — no FalkorDB needed. Also adopted Content Awareness layer.

5. **Research gaps identified** (Mar 4): Six gaps — CHESS code audit, CHASE-SQL code existence, SQL-of-Thought taxonomy, embedding model choice, Databricks/Snowflake patterns, PExA details.

6. **User's targeted research** (Mar 4): Found CHESS is dead (9 commits), CHASE-SQL is paper-only, PExA is probe-before-generate (not validate-after), Snowflake Cortex YAML spec, SQL-of-Thought error_taxonomy.json.

7. **Final v4 built** (Mar 4): Six structural changes from v3. Build from scratch. PExA probes as Stage 4. Full error taxonomy embedded. Snowflake-style semantic model. Documentation ingestion. 1,667-line document ready for Claude Code.

---

## E2E Test Results — Faculty Utilization (March 5, 2026)

### Data Exploration Findings
- **lecture_status values**: `PENDING`, `COMPLETED`, `CANCELED`, `"PENDING"` (quoted). NOT "CONDUCTED".
- **lecturetype values**: `LIVE`, `RECORDED`, `PDF`, `SECONDARY`, `None`
- **financeexamcategory**: `JEE` (not `IIT-JEE`), `NEET`, `SSC`, `Banking`, `GATE`, `UPSC Online`, `Power Batch`, etc.
- **Schedule planner date range**: 2015-10 to 2028-08 (includes future planned). March 2026: 69,934 rows.
- **Lecture batch info date range**: Top end = Dec 2025 (~555K rows). **No 2026 data** — replica snapshot.
- Tables overlap only pre-2026: Must use `WHERE month = '2025-12-01'` style filters for realistic tests.

### Test Pass 1 — 6 Natural Language Questions (first generation)
| # | Question | Exec | Rows | Issue |
|---|---|---|---|---|
| Q1 | Total planned lectures | ✅ | 516,497 | — |
| Q2 | Top subjects by conducted | ✅ | 0 | Used `lecture_status='CONDUCTED'` (wrong; should be `'COMPLETED'`) |
| Q3 | Distinct faculty count | ✅ | 0 | Same `lecture_status` issue |
| Q4 | Planned+conducted hours by nexam | ❌ | — | `date_diff('minute', DATE, DATE)` — needs TIMESTAMP |
| Q5 | Highest utilisation faculty | ❌ | — | `ROW_NUMBER()` in WHERE (not allowed in Trino) |
| Q6 | Leader comparison | ❌ | — | Column alias `leader` in UNION ALL GROUP BY |
| | **Result** | **3/6 exec (50%)** | | **LLM cost: $0.07** |

### Test Pass 2 — Revision Loop (self-repair on 3 failures)
| # | Original Error | Attempt | Result |
|---|---|---|---|
| Q4 | date_diff on DATE | 1 | ✅ Fixed — removed TRY_CAST to DATE, used timestamps directly. 59 rows returned. |
| Q5 | ROW_NUMBER in WHERE | 2 | ✅ Fixed (attempt 2) — CTE with rnk, then filter. 0 rows (date range). |
| Q6 | Column alias in UNION | 2 | ✅ Fixed (attempt 2) — separate CTEs, full CASE in GROUP BY. 0 rows (date range). |
| | **Revision cost** | | **$0.025 (5 LLM calls)** |

### Key Insights
1. **Revision loop works**: 3/3 syntax errors fixed via LLM self-repair (100% recovery after max 2 attempts).
2. **Semantic errors undetected**: LLM guessed `lecture_status='CONDUCTED'` instead of `'COMPLETED'` — needs domain context.
3. **Date range gap**: lecture_batch_info ends Dec 2025; schedule_planner has 2026+ data. "Current month" filters return 0.
4. **Overall pipeline**: 6/6 SQL generation success → 3/6 execute on first try → 6/6 after revision = **100% with self-repair**.
5. **Error taxonomy validated**: All 3 failure patterns are in `config/error_taxonomy.json` (trino_specific, syntax, subquery).

---

## Open Items / Future Research

- [ ] **PageIndex** — file for Phase 3/4 if schema retrieval accuracy is a bottleneck
- [ ] **RLVR fine-tuning** — after 6 months of accumulated (question, SQL) data, fine-tune Qwen2.5-Coder-32B with execution-based rewards (needs GPU)
- [ ] **Embedding model benchmark** — Phase 3: compare text-embedding-3-small vs text-embedding-3-large on 50-question held-out set
- [ ] **A/B test CHASE-SQL value** — Phase 3: quantify 1 candidate vs 3 candidates accuracy gap on real queries
- [ ] **Snowflake Cortex Analyst full YAML spec** — deeper study of their semantic view features if glossary needs expansion

---

## Key File Locations

| File | Location |
|---|---|
| v4 Final Plan (hand to Claude Code) | `/mnt/user-data/outputs/text-to-sql-v4-final.md` |
| v3 Plan | `/mnt/user-data/outputs/text-to-sql-final-plan-v3.md` |
| v2 Plan | `/mnt/user-data/outputs/text-to-sql-chess-implementation-plan-v2.md` |
| v1 Plan | `/mnt/user-data/outputs/text-to-sql-implementation-plan.md` |
| Deep Research Report | `/mnt/user-data/outputs/text-to-sql-deep-research.md` |
| User's Research (Clarifying Questions) | `/mnt/user-data/uploads/Clarifying_Questions.docx` |
| User's Research (CHESS + Benchmarks) | `/mnt/user-data/uploads/CHESS__GitHub_code____Multi-Agent_Text-to-SQL_framework.pdf` |
| User's Research (Overview + Benchmarks) | `/mnt/user-data/uploads/Overview_and_Benchmarks.pdf` |
| User's Research (Error Taxonomy + Cortex YAML) | `/mnt/user-data/uploads/SQL-of-Thought_Error_Taxonomy_and_Snowflake_Cortex_Analyst_YAML.pdf` |
| Conversation Transcript | `/mnt/transcripts/2026-03-04-08-04-36-chess-text-to-sql-implementation-plan.txt` |
| Previous Session Transcript | `/mnt/transcripts/2026-03-03-07-23-27-text-to-sql-trino-research-consultation.txt` |

---

## Real Infrastructure Connections

| Component | Endpoint | Status |
|---|---|---|
| Azure OpenAI Chat | APIM gateway (gpt4o) | Connected |
| Azure OpenAI Embed | Azure endpoint (embedlarge, 3072-dim) | Connected |
| Trino | Production replica (port 443) | Connected |
| pgvector | RDS antondb (PostgreSQL 14.17, pgvector 0.8.0) | Connected |

---

## Preprocessing Results (Real Data)

| Stage | Output | Count |
|---|---|---|
| extract_dbt_metadata | schema_catalog.json + lineage | 2,399 tables, 2,273 lineage nodes, 2,377 edges |
| enrich_schema_catalog | Trino columns → schema_catalog.json | 72,826 columns across 2,109 tables |
| extract_metabase_questions | metabase_questions.json | 7,213 Q-SQL pairs, 1,253 JOIN patterns |
| build_table_graph | table_graph.gpickle | 3,025 nodes, 3,339 edges |
| build_content_awareness | content_awareness.json | 391 tables, 12,257 column entries |
| ingest_documentation | doc_chunks.json | 76 chunks |
| load_embeddings | pgvector tables | schema: 2,399 / question: 7,213 / doc: 76 |

**Not run:** build_lsh_index (needs Trino column value sampling), build_glossary (empty semantic_model.yaml)

---

## pgvector Limitations

- **PostgreSQL 14.17, pgvector 0.8.0**: Both IVFFlat and HNSW indexes limited to ≤2000 dimensions
- Our embeddings are 3072-dim → **no vector indexes possible**
- Using sequential scan: ~5s per search across 2,399 rows (acceptable for <10K rows)
- **Future options:** Upgrade pgvector to ≥0.9.0, or use `dimensions=2000` param in OpenAI API (requires re-embedding)

---

## E2E Test Results (March 6, 2026)

**6/6 questions pass (100%)** — commit c500deb

| # | Question | Complexity | Rows | Time | Result |
|---|---|---|---|---|---|
| Q0 | How many active batches? | SIMPLE | 1 | 82s | 1,191,784 active batches |
| Q1 | Students who completed all lectures in Dec 2025 | SIMPLE | 1 | 48s | 0 (no completions found) |
| Q2 | Total revenue all time | SIMPLE | 1 | 52s | ~8.83 billion |
| Q3 | Top 10 batches by student count | COMPLEX | 10 | 52s | Top batch ~3M students |
| Q4 | Average lectures per batch (active) | COMPLEX | 1 | 47s | 275.33 avg lectures/batch |
| Q5 | Top 10 faculty by lecture count | COMPLEX | 10 | 85s | Top faculty: 21,519 lectures |

**Totals:** 365s runtime, $1.71 cost, avg 61s/query

---

## Current Project Status (March 5, 2026)

**HEAD:** `4eaed6a` (Phase 5.2) on `main`, pushed to GitHub

| Phase | Status | Commit |
|---|---|---|
| Phase 1 (Weeks 1-3) | ✅ Complete | 63fbe72 |
| Phase 2 (Weeks 4-5) | ✅ Complete | fd548cf |
| Phase 3 (Weeks 6-8) | ✅ Complete | 152f3f8 |
| Phase 5 (Weeks 13-16) | ✅ Complete | b43c4f7, 17129cc, 4eaed6a |
| Phase 6 (Weeks 17-20) | 🟡 Tooling done, data population pending | 0600663 |
| Phase 4 (Weeks 9-12) | ⬜ Not started | — |

**Phase 6 remaining:**
- [ ] Run `auto_describe.py` on gold+silver tables (~$0.36, fills 99.6% empty descriptions)
- [ ] Run `build_content_awareness.py --from-env` (fills 49K null column stats from Trino)
- [ ] Re-embed updated schema_catalog into pgvector
- [ ] Re-run 30-question eval to measure accuracy lift
- [ ] Run `export_finetuning_data.py` once query_log accumulates thumbs-up pairs

**Phase 4 next (production hardening):**
- [ ] SSO authentication + row-level security
- [ ] Rate limiting (per-user: 20 queries/hour)
- [ ] Alerting: accuracy drop, cost spike, Trino timeout
- [ ] Auto-refresh preprocessing on dbt deploy
- [ ] Slack bot integration
- [ ] Admin dashboard + load testing

---

## Git History

| Commit | Message |
|---|---|
| 4eaed6a | Phase 5.2: ThinkingTab, action bar, tables-used, Excel/filter, Documents/Glossary/Admin pages |
| 17129cc | Phase 5.1: Multi-turn sessions, help modal, search, loading progress |
| 0600663 | Phase 6: Prometheus metrics, data quality tooling, Grafana dashboard |
| b43c4f7 | Phase 5: Full UI redesign + open-source sanitization |
| 152f3f8 | Phase 3: semantic model 57 rules, gen_complex, model routing fallback, did-you-mean, confidence calibration |
| c5490e5 | Fix AMBIGUOUS routing + add fallback for edge cases |
| fce3568 | Rewrite eval harness for real infrastructure integration |
| 8a2bb7c | Add 200-question CDP test set + glossary embedding fixes |
| 4a3dcac | Update memory.md with Phase 3 details |
| 5c43753 | Phase 3: async search, caching, multi-turn, feedback, semantic model |
| c500deb | feat: preprocessing + embedding + bug fixes + E2E 6/6 |
| 8cf9e84 | Connect to real infrastructure: Azure OpenAI + Trino + pgvector |
| fd548cf | Phase 2 Week 5: Web service + React UI + Docker + K8s |
| 120748b | Phase 2 Week 4: Extended test set to 100 questions |
| 63fbe72 | Phase 1 Weeks 1-3: Core pipeline |

---

## Phase 3 Changes (commit 152f3f8)

### New/Modified Files
- **prompts/gen_complex.txt** — Domain-aware complex query generation prompt (4-step: Decompose → Plan → Write → Verify, 13-point quality checklist)
- **config/semantic_model.yaml** — Expanded from 6→18 tables, 12→57 business rules, 5→19 verified queries, 4→12 relationships
- **config/model_routing.yaml** — 5 light stages configured for gpt4o-mini with auto-fallback to gpt4o
- **src/raven/generation/sql_generator.py** — Wired gen_complex.txt as strategy for COMPLEX Candidate A
- **src/raven/pipeline.py** — "Did you mean?" suggestions for AMBIGUOUS queries (up to 5 from similar_queries + glossary), retrieval_quality dict passed to validator
- **src/raven/connectors/openai_client.py** — _unavailable_models tracking + 404 detection + auto-fallback in complete()
- **src/raven/validation/candidate_selector.py** — 7-factor confidence scoring (max 10pts): candidates, no_errors, cost_guard, entity_matches, glossary_matches, similar_query_sim, probe_evidence
- **preprocessing/ingest_documentation.py** — FIXED: was using text-embedding-3-small (1536-dim) via raw OpenAI client. Now uses project connectors (Azure text-embedding-3-large, 3072-dim) via OpenAIClient + PgVectorStore
- **tests/eval_ab_candidates.py** — A/B test script: 1 vs 3 candidates for COMPLEX queries

### pgvector Embeddings
- schema_embeddings: 2,399
- question_embeddings: 7,213
- glossary_embeddings: 598 (rebuilt from 57 rules, 19 queries, 18 tables, 12 relationships)
- doc_embeddings: 76 (re-embedded with 3072-dim text-embedding-3-large)

### Previous Phase 3 Changes (commit 5c43753)

### New Files
- **src/raven/cache.py** — QueryCache: in-memory LRU (500 entries, 1hr TTL), SHA256 key normalization
- **src/raven/conversation.py** — ConversationManager: follow-up detection heuristics + LLM-based question rewriting using query_log history

### Modified Files
- **src/raven/connectors/pgvector_store.py** — async_search() via asyncio.to_thread, query_log table (CRUD: log_query, update_feedback, get_query, get_conversation_history, get_pending_corrections), indexes on query_id/conversation_id/feedback
- **src/raven/pipeline.py** — Integrated cache (check→stages→store), conversation resolution for follow-ups, passes openai to FeedbackCollector
- **src/raven/feedback/collector.py** — Complete rewrite: persists to query_log, auto-embeds thumbs-up pairs into few-shot (question_embeddings), correction workflow
- **src/raven/retrieval/{fewshot,doc,glossary}_retriever.py** + **src/raven/schema/column_filter.py** — All switched to await pgvector.async_search() for true parallelism
- **config/semantic_model.yaml** — CDP model: 6 tables (gold_batches, gold_batch_rooms, gold_orders, gold_dbt_lecture_batch_info, gold_offline_assigned_batch, gold_payments), 11 business rules, 5 verified queries, 4 relationships, 72 synonyms
- **config/model_routing.yaml** — Added cost_tier annotations (light/heavy) for gpt4o-mini migration; 5 light stages identified (router, conversation_rewrite, ir_keyword_extract, out_chart, out_summary)
- **preprocessing/build_glossary.py** — Rewritten for new YAML format (synonyms, time_dimensions, metrics, term/sql_fragment, top-level relationships), uses project connectors (OpenAIClient + PgVectorStore)

### Performance Expectations
- pgvector parallelism: 3 concurrent searches ~5s vs ~15s sequential (3x speedup)
- Cache hit: ~0s (instant) for repeated questions within 1hr
- Cost optimization: when gpt4o-mini deployed, light stages save ~78% ($0.28 → ~$0.06/query)
- Glossary embeddings: 154 entries ready to embed (6 tables, 32 dims, 8 time dims, 15 metrics, 72 synonyms, 12 rules, 5 queries, 4 relationships)

---

## Phase 5 Changes (commit b43c4f7)

- Complete UI redesign with antd, Monaco Editor, Plotly, lucide-react, react-flow
- Sanitized all company-specific references from tracked files for open-source

## Phase 5.1 Changes (commit 17129cc)

- **web/ui/src/App.js** — Multi-turn conversation_id wiring to backend, session message persistence (localStorage save/load/delete/switch), "What can I ask?" help modal, session history search filter, 8-stage loading progress animation, session delete, version bumped to v0.4
- **web/ui/src/App.css** — Sidebar search styles, session flex layout with delete button, loading progress bar (3px bar with stage text), help modal overlay/styles, enhanced mobile-responsive CSS (tab wrapping, data table scroll, SQL read-only on mobile, candidate grid stack)
- **web/routes/__init__.py** — Added `verified: bool = False` to QueryResponse model (UI badge was already checking this field)

## Phase 5.2 Changes (commit 4eaed6a)

### New Files
- **web/ui/src/components/tabs/ThinkingTab.js** — 8-stage pipeline trace with collapsible details (router classification, context keywords/entities/glossary, selected tables as pills, probe evidence, SQL candidates/winner, validation errors/fixes, execution rows/chart), per-stage timing, skipped stage display for SIMPLE queries, raw debug JSON viewer
- **web/ui/src/components/pages/DocumentUpload.js** — Drag-drop file upload page (POST /api/admin/upload-doc), document list with chunk count + delete
- **web/ui/src/components/pages/GlossaryEditor.js** — Full CRUD glossary editor (search, add/edit form with term/definition/SQL fragment/synonyms/tables, delete), calls /api/admin/glossary endpoints, demo data fallback
- **web/ui/src/components/pages/AdminDashboard.js** — 3-tab admin dashboard (Overview: 6 stat cards + top questions, Costs: 3 stat cards, Failures: error list), calls /api/stats, demo data fallback

### Modified Files
- **web/ui/src/App.js** — Lazy imports for 3 page components, `activeTool` state for sidebar tool navigation (Documents/Glossary/Admin), sidebar tools section with 3 buttons (Admin restricted to engineer persona), Suspense-wrapped tool page rendering, conditional chat/input visibility, `'thinking'` added to analyst+engineer PERSONAS visibleTabs
- **web/ui/src/App.css** — ~200 lines: tables-used pills, action bar, thinking tab (stages/header/body/details/raw), sidebar tools, page panels, upload zone, doc list, glossary (toolbar/search/form/list), admin (tabs/stats grid/stat cards/subsections/list), shared buttons (btn-icon-sm, btn-primary-sm, btn-secondary-sm), empty state, mobile responsive additions
- **web/ui/src/components/ResponseCard.js** — ThinkingTab integration + action bar (Copy SQL, Download CSV, Share Link) between QueryRefinement and FeedbackBar, `copiedSQL` state with clipboard fallback
- **web/ui/src/components/tabs/SummaryTab.js** — TablesUsed sub-component displaying `debug.selected_tables` as pills with Database icon
- **web/ui/src/components/tabs/DataTab.js** — Per-column text filter dropdowns (antd filterDropdown), Excel export via XML Spreadsheet format (no library), triggerDownload helper
- **web/routes/__init__.py** — Glossary CRUD endpoints (GET/POST/PUT/DELETE /api/admin/glossary) with JSON file persistence at config/glossary_terms.json

### UI Spec Features Implemented (from raven-ui-specification.md)
- ✅ ThinkingTab (chain-of-thought pipeline trace)
- ✅ Action bar (Copy SQL, CSV download, Share)
- ✅ Tables used display in summary
- ✅ Excel download + column filtering
- ✅ Document upload page
- ✅ Glossary editor page
- ✅ Admin dashboard page
- ✅ Sidebar tools navigation

### UI Spec Features Deferred (future work)
- SSE streaming (real backend → frontend stage events)
- Phoenix iframe embedding + JWT auth
- Save to Metabase from SQL tab
- Voice input
- Embeddable npm package `@raven-sql/embed`

---

## Phase 6: Infrastructure + Data Quality

### Eval Baseline (30-question sample, Phase 3)
- Pass rate: 53.3% (16/30), Exec success: 93.3%
- SIMPLE: 63.2%, COMPLEX: 36.4%
- Avg cost: $1.31/query (all gpt4o, no mini available)
- Avg latency: 73.4s, P95: 108.7s

### Data Quality Assessment
- 99.6% tables (2,389/2,399) missing descriptions
- 99.8% columns (72,692/72,826) missing descriptions
- 100% content awareness entries missing stats (49,362 null values)
- Gold tables: 420 total, 410 empty descriptions (97.6%)
- Silver tables: 1,034 total, 1,034 empty descriptions (100%)

### New Files
- **src/raven/metrics.py** — Prometheus instrumentation: Histogram (query_latency, stage_latency, cost), Counter (queries_total, errors, cache hits/misses, tokens, feedback, confidence), Gauge (in_flight). Custom registry, stage_timer context manager, generate_metrics() exposition.
- **preprocessing/auto_describe.py** — GPT-4o-mini auto table/column description generator. Tier filtering (gold/silver/all), batch processing, dry-run mode, cost estimation (~$0.11 for 421 gold tables).
- **preprocessing/export_finetuning_data.py** — Export validated (question, SQL) pairs for RLVR fine-tuning from query_log + semantic_model. Formats: OpenAI JSONL, DPO, raw.
- **scripts/assess_data_quality.py** — One-shot data quality gap analysis script.
- **k8s/grafana-dashboard.json** — Grafana dashboard: Overview stats (queries, latency p50/p95, cost, cache rate), Latency (by difficulty, by stage), Throughput/Errors (query rate, error rate, stage errors), Cost/Tokens (per-query cost, token usage), Quality/Feedback (confidence pie, feedback rate, positive rate).

### Modified Files
- **src/raven/pipeline.py** — Instrumented with METRICS: query_started/completed, cache hit/miss, per-stage timing via _run_stage, stage error recording.
- **web/routes/__init__.py** — /api/metrics now returns proper prometheus_client exposition format. Feedback endpoint records METRICS.record_feedback().
- **preprocessing/build_content_awareness.py** — Added --trino-password, --trino-ssl-insecure, --from-env CLI flags for proper Trino authentication. Default port 443, catalog cdp.

### Phase 6 Remaining
- [ ] Run `auto_describe.py` (gold ~$0.11, silver ~$0.25)
- [ ] Run `build_content_awareness.py --from-env` (populate 49K null stats)
- [ ] Re-embed updated schema_catalog into pgvector
- [ ] Re-run 30-question eval to measure accuracy lift
