<p align="center">
  <h1 align="center">RAVEN</h1>
  <p align="center"><strong>Accuracy-first text-to-SQL for Trino analytics stacks</strong></p>
</p>

RAVEN is an open-source backend for turning natural-language analytics questions into SQL for Trino-based warehouses.

The project is being built around:

- semantic contracts
- trusted query reuse
- value grounding
- deterministic planning
- plan-aware validation
- abstention when confidence is weak

It is meant for teams that already have analytics assets such as dbt metadata, saved BI queries, and business metric definitions.

## Status

RAVEN is **active, usable, and not finished**.

Current state:

- accuracy-core architecture is substantially built
- deterministic and trusted-query paths are implemented
- runtime hardening and benchmark-gated releases are still incomplete
- APIs and internal modules may still change

If you want the current engineering state, read:

- [docs/ai-handoff.md](docs/ai-handoff.md)
- [docs/accuracy-first-10-10-roadmap.md](docs/accuracy-first-10-10-roadmap.md)

## What RAVEN Is

RAVEN is:

- an accuracy-first text-to-SQL system for `Trino + dbt + Metabase + OpenMetadata` style environments
- a configurable engine that should load domain knowledge from semantic assets, not hardcoded Python rules
- best suited for internal analytics questions where correctness matters more than UI polish

RAVEN is not:

- a zero-config generic SQL chatbot
- a polished end-user BI product
- a guarantee of good results without curated metadata

## Research Lineage

The architecture is intentionally research-informed. That is a real differentiator and should be explicit.

RAVEN is not just "LLM + schema text". The current design pulls from work showing that enterprise text-to-SQL improves when schema linking, semantic structure, trusted examples, and execution-aware validation are treated as first-class concerns.

| Source | What RAVEN takes from it |
|---|---|
| [CHESS](https://arxiv.org/abs/2405.16755) | staged retrieval and schema-selection ideas for enterprise databases |
| [CHASE-SQL](https://arxiv.org/abs/2410.01943) | multi-candidate generation and comparative selection ideas |
| QueryWeaver | graph-oriented schema reasoning and bridge-table thinking |
| PExA-style probe-first work | evidence gathering before or around SQL generation |
| SQL-of-Thought-style validation | structured validation and repair thinking instead of generic retries |
| [RAT-SQL](https://aclanthology.org/2020.acl-main.677/) | schema linking as a first-order accuracy problem |
| [PICARD](https://aclanthology.org/2021.emnlp-main.779/) | constrained generation as the right direction for SQL safety |
| [BIRD](https://bird-bench.github.io/) and [Spider 2.0](https://spider2-sql.github.io/) | realism: large schemas, messy values, ambiguity, and enterprise complexity |
| [Snowflake Cortex Analyst](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-analyst) | semantic-model-first product direction |
| Wren / MinusX / Vanna style systems | semantic assets, trusted query memory, and evidence-backed reuse |

What that means in practice:

- semantic contracts matter more than prompt cleverness
- trusted internal queries matter more than generic few-shot examples
- value grounding matters as much as SQL generation
- deterministic planning should cover as much of the question space as possible
- the model should be the fallback for the unresolved tail, not the default authority

## What You Actually Need For Good Results

If you want strong accuracy, these inputs matter more than prompt tweaks:

1. A readable warehouse model
   - dbt models, decent naming, clear join paths
2. A semantic asset pack
   - metrics, dimensions, synonyms, approved relationships, verified query examples
3. Trusted analytics artifacts
   - saved BI questions, dashboards, curated SQL, or reviewed question-to-SQL pairs
4. Metadata quality
   - table descriptions, column descriptions, table annotations, ownership, lineage
5. Read-only Trino access
   - RAVEN assumes it can inspect and execute safely against Trino

Without those, the system can still run, but accuracy will plateau.

## Current Capabilities

Implemented now:

- configurable semantic model / domain-pack loading
- semantic contract validation
- trusted query-family matching and safe query reuse
- value grounding from semantic assets and metadata evidence
- deterministic join policy and linker
- deterministic schema seeding and non-destructive pruning
- typed query plans
- deterministic Trino compilation for planned lanes
- plan-aware SQL validation
- execution-grounded result sanity checks
- abstention when validation or result shape contradicts the plan

Current passing suites:

- focused accuracy-first suite: `55 passed`
- smoke suite: `137 passed`

## Current Limitations

Still in progress:

- constrained fallback generation
- calibrated confidence scoring
- benchmark-as-release-gate
- shared cache / rate limiting / distributed-safe runtime state
- Trino session reuse and broader runtime hardening
- fuller SQL compiler coverage for the unresolved tail

Practical implication:

- RAVEN is strongest when the question fits trusted-query or deterministic-plan lanes
- the unresolved tail still depends on fallback generation more than it should

## Architecture Summary

The active backend path is moving toward:

1. Normalize and route the question
2. Load semantic and trusted assets
3. Ground values and resolve likely tables / joins
4. Build a typed query plan when possible
5. Compile deterministic SQL first
6. Fall back to generation only when necessary
7. Validate SQL against the plan
8. Execute and sanity-check results
9. Return an answer, clarification, or abstention

For the exact current path, use [docs/ai-handoff.md](docs/ai-handoff.md) as the source of truth.

## Why This Matters

Most text-to-SQL systems fail for predictable reasons:

- wrong table or join selection
- wrong metric interpretation
- wrong entity or value resolution
- syntactically valid SQL that does not answer the business question

RAVEN's architecture is built to reduce those specific failure modes, not just to make generated SQL look more fluent.

## Quickstart

### Prerequisites

- Python 3.11+
- Docker and Docker Compose
- access to a Trino cluster
- OpenAI API key

### 1. Clone and configure

```bash
git clone https://github.com/aamir306/raven.git
cd raven
cp .env.example .env
```

Then edit `.env`.

### 2. Start services

```bash
docker-compose up -d
```

### 3. Refresh metadata and artifacts

```bash
python -m preprocessing.refresh_all
```

### 4. Query the API

```bash
curl -X POST http://localhost:8000/api/query \
  -H "Content-Type: application/json" \
  -d '{"question": "How many active users yesterday?"}'
```

Or open `http://localhost:8000` for the web UI.

## Configuration

Key environment variables:

- `RAVEN_DOMAIN_PACK_PATH`
  - preferred path for a split semantic/domain pack
- `RAVEN_SEMANTIC_MODEL_PATH`
  - fallback path for a single semantic model file

Important config files:

| File | Purpose |
|---|---|
| `.env` | credentials and environment variables |
| `config/settings.yaml` | pipeline and runtime settings |
| `config/model_routing.yaml` | model selection by stage |
| `config/error_taxonomy.json` | SQL error taxonomy |
| `config/trino_dialect_rules.txt` | Trino-specific SQL rules |
| `config/cost_guards.yaml` | query safety / cost constraints |
| `config/semantic_model.yaml` | semantic asset pack |
| `config/table_annotations.yaml` | table-level notes and warnings |
| `config/openmetadata.yaml` | OpenMetadata integration settings |
| `config/metabase_mcp.yaml` | Metabase integration settings |

## Repository Layout

```text
raven/
├── config/           # configuration and semantic assets
├── docs/             # handoff, roadmap, build notes
├── preprocessing/    # metadata extraction and indexing
├── prompts/          # prompt templates
├── src/raven/
│   ├── pipeline.py
│   ├── semantic_assets.py
│   ├── contracts/
│   ├── retrieval/
│   ├── schema/
│   ├── grounding/
│   ├── query_families/
│   ├── planning/
│   ├── sql/
│   ├── generation/
│   ├── validation/
│   ├── output/
│   ├── connectors/
│   └── feedback/
├── tests/
├── web/
└── k8s/
```

## Development

### Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

### Run tests

```bash
pytest tests/
```

For accuracy-core work, start with the suites listed in [docs/ai-handoff.md](docs/ai-handoff.md).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

If you materially change architecture or progress state, also update:

- [docs/ai-handoff.md](docs/ai-handoff.md)
- [docs/accuracy-first-10-10-roadmap.md](docs/accuracy-first-10-10-roadmap.md)

## License

This project is licensed under the Apache License 2.0. See [LICENSE](LICENSE).
