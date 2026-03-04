# Text-to-SQL Project — Conversation Memory

## Last Updated: March 4, 2026

---

## Project Overview

Building a production text-to-SQL system for Aamir's Trino-Iceberg CDP platform (1,200+ tables, bronze/silver/gold layers) at penpencil.co / pw.live (education analytics). System takes English questions → SQL + data + chart + NL summary.

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
| Embedding model | text-embedding-3-small (benchmark vs large in Phase 3) | No SQL-specific evidence for upgrade, start cheap |
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

| Phase | Timeline | Accuracy | Key Drivers |
|---|---|---|---|
| Phase 1 | Weeks 1-3 | 70-75% | Core pipeline + dbt + Metabase few-shot + Trino dialect |
| Phase 2 | Weeks 4-5 | 75-80% | PExA probes + error taxonomy + chart/summary |
| Phase 3 | Weeks 6-8 | 80-85% | Expanded glossary + feedback loop + more few-shot |
| Phase 4 | Weeks 9-12 | 82-88% | Production hardening + accumulated corrections |
| Future | 6+ months | 88-92% | RLVR fine-tune Qwen2.5-Coder-32B (needs GPU) |

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

- **LLM:** OpenAI API only (GPT-4o + GPT-4o-mini). No GPU.
- **Data privacy:** Schema/metadata → API OK. Row values → NEVER to API.
- **DB:** Trino-Iceberg, 1,200+ tables, 155 schemas, bronze/silver/gold
- **Vector DB:** pgvector (existing)
- **Infra:** Docker on K8s, 1-2 engineers, Claude Opus via Claude Code
- **Budget:** No hard cap, target < $3,000/month at full scale

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
