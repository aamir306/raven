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
  <a href="docs/ai-handoff.md">AI Handoff</a>
</p>

---

RAVEN is an accuracy-first text-to-SQL system for **Trino-centered analytics stacks**. It is being built as a semantic, compiler-style backend for `Trino + dbt + Metabase + OpenMetadata` environments, with domain knowledge loaded from configurable semantic assets instead of hardcoded engine logic.

## AI / Engineering Handoff

If you are continuing implementation or evaluating the current architecture, start here:

- [docs/ai-handoff.md](docs/ai-handoff.md) — current implementation state, active backend path, next work
- [docs/accuracy-first-10-10-roadmap.md](docs/accuracy-first-10-10-roadmap.md) — target architecture and roadmap

Some older markdown files in the repo are historical context and may describe earlier architecture decisions.

## Current Direction

RAVEN is no longer being shaped as a generic "LLM chats with your schema" app.

The active direction is:

- **Semantic contracts first** — metrics, dimensions, relationships, and trusted assets should live in config or domain packs
- **Deterministic planning first** — the system should form a typed plan before free-form SQL generation
- **Trusted evidence first** — verified queries, Metabase assets, OpenMetadata signals, and semantic assets should outrank model guesswork
- **Abstain over guess** — if confidence is weak, the system should clarify or refuse instead of returning elegant wrong answers
- **OSS engine, configurable domain knowledge** — the Python engine should stay generic; business semantics should be externalized

## Current Status

As of `March 7, 2026`:

- overall technical roadmap: `~70%`
- accuracy-core architecture: `~92%`
- production/runtime hardening: `~43%`

Implemented now:

- configurable domain-pack / semantic-model loading
- semantic contract validation
- trusted query-family matching and safe query-family reuse
- deterministic value grounding
- deterministic join policy and linker
- deterministic schema seeding plus non-destructive pruning
- typed query plans
- deterministic Trino compilation for planned lanes
- plan-aware validation
- execution-grounded result sanity checks
- abstention when plan or result confidence is too weak

Still in progress:

- constrained fallback generation
- calibrated confidence model
- benchmark-as-release-gate
- runtime hardening: shared cache, Trino pooling, vector scaling, distributed-safe state

Current passing suites:

- focused accuracy-first suite: `55 passed`
- smoke suite: `137 passed`

## Architecture

```
User Question (English)
        │
        ▼
┌──────────────────────────┐
│  1. Normalize / Route    │  Understand question shape and scope
├──────────────────────────┤
│  2. Semantic Assets      │  Contracts, verified queries, Metabase, OM evidence
├──────────────────────────┤
│  3. Ground / Link        │  Resolve values, tables, joins, required columns
├──────────────────────────┤
│  4. Build Query Plan     │  Typed deterministic plan when possible
├──────────────────────────┤
│  5. Compile or Fallback  │  Deterministic SQL first, LLM fallback second
├──────────────────────────┤
│  6. Validate             │  Check plan consistency before execution
├──────────────────────────┤
│  7. Execute / Judge      │  Sanity-check results against plan
├──────────────────────────┤
│  8. Respond or Abstain   │  Return answer, ask clarification, or refuse
└──────────────────────────┘
```

Important note:

- the repo still contains older staged modules from the original LLM-heavier pipeline
- the active backend path is increasingly compiler-first and deterministic
- for the exact current path, read [docs/ai-handoff.md](docs/ai-handoff.md)

For the current implementation state, see [docs/ai-handoff.md](docs/ai-handoff.md). For the target architecture, see [docs/accuracy-first-10-10-roadmap.md](docs/accuracy-first-10-10-roadmap.md).

## Features

| Feature | Description |
|---|---|
| **Configurable Domain Packs** | Load semantic assets from `RAVEN_DOMAIN_PACK_PATH` or `RAVEN_SEMANTIC_MODEL_PATH` |
| **Semantic Contract Validation** | Reject malformed metric, dimension, and relationship definitions at startup |
| **Trusted Query Families** | Reuse verified SQL and Metabase-backed assets across safe metric, dimension, time, and filter changes |
| **Value Grounding** | Resolve entity values from semantic enums, content signals, and asset evidence |
| **Deterministic Join Policy** | Prefer approved join paths over model-discovered joins |
| **Typed Query Plans** | Build structured plans before SQL for supported intents |
| **Deterministic SQL Compilation** | Compile planned queries to Trino SQL via internal AST-style builders |
| **Plan-Aware Validation** | Reject SQL that drops required joins, filters, limits, ordering, or metric intent |
| **Execution Judge** | Reject results whose row shape or numeric behavior contradicts the plan |
| **Fallback Generation** | Still present for unresolved cases, but no longer the desired default path |

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
curl -X POST http://localhost:8000/api/query \
  -H "Content-Type: application/json" \
  -d '{"question": "How many active users yesterday?"}'
```

Or open `http://localhost:8000` for the web UI.

## Configuration

| File | Purpose |
|---|---|
| `.env` | Credentials and environment variables |
| `config/settings.yaml` | Pipeline parameters, thresholds, cache TTL |
| `config/model_routing.yaml` | LLM model selection per pipeline stage |
| `config/error_taxonomy.json` | 36-subtype SQL error classification |
| `config/trino_dialect_rules.txt` | 20 Trino-specific SQL rules |
| `config/cost_guards.yaml` | Query cost limits, partition requirements |
| `config/semantic_model.yaml` | Single-file semantic asset pack |
| `config/table_annotations.yaml` | Table-level warnings, notes, and annotations |
| `config/openmetadata.yaml` | OpenMetadata integration settings |
| `config/metabase_mcp.yaml` | Metabase integration settings |

Important environment variables:

- `RAVEN_DOMAIN_PACK_PATH`
  - preferred path for a split semantic/domain pack
- `RAVEN_SEMANTIC_MODEL_PATH`
  - fallback path for a single semantic model file

For open-source use, keep business semantics in these assets instead of in engine code.

## Monitoring

RAVEN exposes a Prometheus-compatible `/api/metrics` endpoint with:

- **Query latency** histograms (by difficulty, cached/uncached)
- **Stage latency** histograms (per pipeline stage)
- **Query cost** histograms (USD per query)
- **Counters** for cache hits/misses, errors by stage, token usage, feedback
- **Gauge** for in-flight queries

Import the provided Grafana dashboard from `k8s/grafana-dashboard.json`.

Current note:

- the metrics/bootstrap path still needs cleanup before this should be treated as production-hardening complete

## Project Structure

```
raven/
├── config/                 # Configuration files
├── docs/                   # Handoff, roadmap, build notes
├── preprocessing/          # Data ingestion scripts (dbt, Metabase, LSH, etc.)
│   ├── auto_describe.py    # GPT-4o-mini auto table/column descriptions
│   ├── export_finetuning_data.py  # RLVR fine-tuning data export
│   └── ...                 # 10 preprocessing scripts total
├── prompts/                # All 16 LLM prompt templates
├── scripts/                # Utility scripts (data quality assessment)
├── src/raven/              # Core pipeline
│   ├── pipeline.py         # Main orchestrator
│   ├── semantic_assets.py  # Semantic asset access layer
│   ├── metrics.py          # Prometheus instrumentation
│   ├── contracts/          # Semantic contract loading and validation
│   ├── router/             # Stage 1: Difficulty classification
│   ├── retrieval/          # Stage 2: Context retrieval (6 sub-modules)
│   ├── schema/             # Stage 3: Schema selection (4 sub-modules)
│   ├── grounding/          # Value grounding and entity resolution
│   ├── query_families/     # Trusted query matching and compilation
│   ├── planning/           # Typed query plans and deterministic planner
│   ├── sql/                # Deterministic AST-style SQL compilation
│   ├── probes/             # Probe-based evidence gathering
│   ├── generation/         # Fallback generation and revision
│   ├── validation/         # Plan validation, selection, execution judge
│   ├── output/             # Execution + charts + summary
│   ├── connectors/         # Trino, pgvector, OpenAI clients
│   ├── feedback/           # Rating and correction pipeline
│   └── safety/             # Query validation, data policy
├── web/                    # FastAPI API + React UI
├── tests/                  # 200-question test set + evaluation scripts
├── k8s/                    # Kubernetes manifests + Grafana dashboard
└── data/                   # Generated artifacts (gitignored)
```

## Research Foundations

RAVEN borrows ideas from multiple systems, but the current implementation is increasingly opinionated around semantic contracts, deterministic planning, and trusted query reuse.

| Source | Contribution to RAVEN |
|---|---|
| [CHESS](https://arxiv.org/abs/2405.16755) (Stanford, 71.1% BIRD) | 4-agent pipeline architecture, prompt patterns |
| [CHASE-SQL](https://arxiv.org/abs/2410.01943) (Apple, 73.0% BIRD) | Multi-candidate generation + pairwise selection |
| PExA-style probe-first patterns | Probe-before-generate reasoning |
| SQL-of-Thought-style taxonomy | Structured error and correction strategy |
| [QueryWeaver](https://github.com/FalkorDB/QueryWeaver) | Graph-based schema traversal, Content Awareness |
| TriSQL-style routing | Difficulty-based routing concepts |
| [Snowflake Cortex Analyst](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-analyst) | Semantic View YAML glossary format |
| Wren / MinusX / Vanna-style patterns | Semantic assets, trusted query memory, evidence-first retrieval |

## Notes For Contributors

- Start with [docs/ai-handoff.md](docs/ai-handoff.md).
- Do not hardcode company-specific semantics into Python logic.
- Put business logic into semantic/domain-pack assets.
- Prefer deterministic planning over expanding the fallback generator.
- Update the handoff and roadmap docs when the architecture or progress changes materially.

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
