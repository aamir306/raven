<p align="center">
  <h1 align="center">RAVEN</h1>
  <p align="center"><strong>Retrieval-Augmented Validated Engine for Natural-language SQL</strong></p>
</p>

<p align="center">
  <a href="#architecture">Architecture</a> •
  <a href="#quickstart">Quickstart</a> •
  <a href="#features">Features</a> •
  <a href="#configuration">Configuration</a> •
  <a href="#contributing">Contributing</a> •
  <a href="docs/architecture.md">Full Docs</a>
</p>

---

RAVEN is a production-grade text-to-SQL system that converts natural language questions into optimized SQL queries for **Trino-Iceberg** data warehouses. It implements an 8-stage pipeline inspired by state-of-the-art research systems (CHESS, CHASE-SQL, PExA, SQL-of-Thought, QueryWeaver, TriSQL), built from scratch for real-world enterprise environments with 1,000+ tables.

## Why RAVEN?

Naive LLM-to-SQL approaches achieve **~6% accuracy** on enterprise-scale schemas (Spider 2.0 benchmark). RAVEN solves this with:

- **Intelligent routing** — Simple queries take a fast path (2-4s), complex queries get the full pipeline (8-18s)
- **Graph-based schema linking** — NetworkX traversal discovers bridge tables that vector search misses
- **Pre-generation probes** — Explores real data before writing SQL, preventing value format mismatches
- **Multi-candidate generation** — 3 diverse SQL candidates with pairwise selection for complex queries
- **Classified error repair** — 36-subtype error taxonomy with targeted fix strategies (not generic "fix this")
- **Business glossary** — Snowflake Cortex-style semantic model with dimensions, metrics, synonyms, verified queries

**Target: 82-88% execution accuracy** on enterprise Trino environments.

## Current Status

| Phase | Status | Description |
|---|---|---|
| Phase 1-3 | ✅ Complete | Core 8-stage pipeline, 230 tests, semantic model (57 rules), multi-turn, caching |
| Phase 5 | ✅ Complete | Production UI (antd, Monaco Editor, Plotly, react-flow) |
| Phase 6 | 🟡 In Progress | Prometheus metrics, Grafana dashboard, data quality tooling |
| Phase 4 | ⬜ Planned | SSO auth, rate limiting, Slack bot, admin dashboard |

**Eval baseline (30-question sample):** 53.3% pass rate, 93.3% execution success, $1.31/query avg cost.\
**Key bottleneck:** Schema descriptions are 99.6% empty — data quality population is the highest-leverage improvement.

## Architecture

```
User Question (English)
        │
        ▼
┌──────────────────────────┐
│  Stage 1: ROUTER         │  Classify: SIMPLE / COMPLEX / AMBIGUOUS
├──────────────────────────┤
│  Stage 2: RETRIEVAL      │  Keywords, LSH entities, few-shot, glossary, docs
├──────────────────────────┤
│  Stage 3: SCHEMA         │  4-step pruning + NetworkX graph bridge table discovery
├──────────────────────────┤
│  Stage 4: PROBES         │  Decompose → probe DB → collect evidence [COMPLEX only]
├──────────────────────────┤
│  Stage 5: GENERATION     │  1 or 3 diverse SQL candidates + Trino dialect rules
├──────────────────────────┤
│  Stage 6: VALIDATION     │  Pairwise selection + 36-type error taxonomy [COMPLEX only]
├──────────────────────────┤
│  Stage 7: EXECUTION      │  Run SQL + auto-chart + NL summary
├──────────────────────────┤
│  Stage 8: RESPONSE       │  SQL + data + chart + summary + feedback loop
└──────────────────────────┘
```

**Simple queries (70%):** Stages 1→2→3→5→7→8 | ~$0.02 | 2-4s  
**Complex queries (30%):** All 8 stages, 3 candidates | ~$0.10 | 8-18s

See [docs/architecture.md](docs/architecture.md) for the full design document.

## Features

| Feature | Description |
|---|---|
| **Difficulty Routing** | TriSQL-inspired classifier routes queries to fast or full pipeline |
| **LSH Entity Matching** | MinHash locality-sensitive hashing resolves fuzzy entity references locally (no API calls) |
| **Graph Schema Linking** | NetworkX on dbt lineage + Metabase JOIN patterns discovers bridge tables |
| **Content Awareness** | Column-level metadata: enums, NULL rates, format patterns, value ranges |
| **PExA Test Probes** | Decompose question → probe database → use evidence for SQL generation |
| **CHASE-SQL Multi-Gen** | 3 diverse generators (Divide-and-Conquer, Execution Plan CoT, Few-Shot) |
| **Error Taxonomy** | 13 categories, 36 sub-types with targeted repair directives |
| **Semantic Model** | Snowflake Cortex-style YAML: dimensions, metrics, synonyms, verified queries |
| **Auto Visualization** | Plotly charts auto-detected from query results |
| **Feedback Loop** | Thumbs up/down → auto-expands few-shot index |
| **Data Privacy** | Schema metadata goes to LLM; actual row data **never** leaves your infrastructure |

## Quickstart

### Prerequisites

- Python 3.11+
- Docker & Docker Compose
- Access to a Trino cluster
- OpenAI API key

### 1. Clone & Configure

```bash
git clone https://github.com/aamir306/raven.git
cd raven
cp .env.example .env
# Edit .env with your Trino, OpenAI, and PostgreSQL credentials
```

### 2. Start Services

```bash
docker-compose up -d
```

This starts the RAVEN API server and a pgvector database for embeddings.

### 3. Run Preprocessing

```bash
# Extract metadata from your dbt project and Metabase
python -m preprocessing.refresh_all
```

### 4. Ask Questions

```bash
curl -X POST http://localhost:8000/api/generate \
  -H "Content-Type: application/json" \
  -d '{"question": "How many active users yesterday?"}'
```

Or open `http://localhost:8000` for the web UI.

## Configuration

| File | Purpose |
|---|---|
| `.env` | Credentials (Trino, OpenAI, PostgreSQL) |
| `config/settings.yaml` | Pipeline parameters, thresholds, cache TTL |
| `config/model_routing.yaml` | LLM model selection per pipeline stage |
| `config/error_taxonomy.json` | 36-subtype SQL error classification |
| `config/trino_dialect_rules.txt` | 20 Trino-specific SQL rules |
| `config/cost_guards.yaml` | Query cost limits, partition requirements |
| `config/semantic_model.yaml` | Business glossary (dimensions, metrics, synonyms) |

## Monitoring

RAVEN exposes a Prometheus-compatible `/api/metrics` endpoint with:

- **Query latency** histograms (by difficulty, cached/uncached)
- **Stage latency** histograms (per pipeline stage)
- **Query cost** histograms (USD per query)
- **Counters** for cache hits/misses, errors by stage, token usage, feedback
- **Gauge** for in-flight queries

Import the provided Grafana dashboard from `k8s/grafana-dashboard.json`.

## Project Structure

```
raven/
├── config/                 # Configuration files
├── docs/                   # Architecture docs, build guide, decision log
├── preprocessing/          # Data ingestion scripts (dbt, Metabase, LSH, etc.)
│   ├── auto_describe.py    # GPT-4o-mini auto table/column descriptions
│   ├── export_finetuning_data.py  # RLVR fine-tuning data export
│   └── ...                 # 10 preprocessing scripts total
├── prompts/                # All 16 LLM prompt templates
├── scripts/                # Utility scripts (data quality assessment)
├── src/raven/              # Core pipeline
│   ├── pipeline.py         # Main orchestrator
│   ├── metrics.py          # Prometheus instrumentation
│   ├── router/             # Stage 1: Difficulty classification
│   ├── retrieval/          # Stage 2: Context retrieval (6 sub-modules)
│   ├── schema/             # Stage 3: Schema selection (4 sub-modules)
│   ├── probes/             # Stage 4: PExA test probes
│   ├── generation/         # Stage 5: SQL generation (3 generators)
│   ├── validation/         # Stage 6: Selection + error taxonomy
│   ├── output/             # Stage 7: Execution + charts + summary
│   ├── connectors/         # Trino, pgvector, OpenAI clients
│   ├── feedback/           # Rating and correction pipeline
│   └── safety/             # Query validation, data policy
├── web/                    # FastAPI API + React UI
├── tests/                  # 200-question test set + evaluation scripts
├── k8s/                    # Kubernetes manifests + Grafana dashboard
└── data/                   # Generated artifacts (gitignored)
```

## Research Foundations

RAVEN synthesizes techniques from multiple state-of-the-art systems:

| Source | Contribution to RAVEN |
|---|---|
| [CHESS](https://arxiv.org/abs/2405.16755) (Stanford, 71.1% BIRD) | 4-agent pipeline architecture, prompt patterns |
| [CHASE-SQL](https://arxiv.org/abs/2410.01943) (Apple, 73.0% BIRD) | Multi-candidate generation + pairwise selection |
| [PExA](https://arxiv.org/abs/2501.xxxxx) (Bloomberg) | Test-probe-before-generate pattern |
| [SQL-of-Thought](https://arxiv.org/abs/2502.xxxxx) (NeurIPS 2025) | Error taxonomy (36 sub-types) |
| [QueryWeaver](https://github.com/FalkorDB/QueryWeaver) | Graph-based schema traversal, Content Awareness |
| [TriSQL](https://www.nature.com/articles/xxxxx) (Nature 2026) | Difficulty-based routing |
| [Snowflake Cortex Analyst](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-analyst) | Semantic View YAML glossary format |

## Cost Model

| Query Type | Cost | Latency |
|---|---|---|
| Simple (70% of traffic) | $0.015–0.025 | 2–4s |
| Complex (30% of traffic) | $0.09–0.14 | 8–18s |
| Blended average | ~$0.04 | ~5s |
| Monthly (200 users × 5 queries/day) | ~$1,200 | — |

## Contributing

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

### Development Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

### Running Tests

```bash
pytest tests/
```

## License

This project is licensed under the Apache License 2.0 — see [LICENSE](LICENSE) for details.

---

<p align="center">
  <strong>RAVEN</strong> — Because your data warehouse deserves better than <code>SELECT * FROM guessing</code>
</p>
