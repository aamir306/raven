# RAVEN AI Handoff

Last updated: `March 7, 2026`

## Purpose

This is the canonical handoff document for any AI or engineer continuing work on RAVEN.

Use this file first.
Then use [docs/accuracy-first-10-10-roadmap.md](./accuracy-first-10-10-roadmap.md) for the full strategic roadmap.

## Source Of Truth

Use the docs in this order:

1. `docs/ai-handoff.md`
   - current implementation state
   - active architecture
   - next recommended work
2. `docs/accuracy-first-10-10-roadmap.md`
   - long-form target architecture
   - workstreams, phases, exit criteria
3. `README.md`
   - public-facing project overview
4. `prompts/gpt54_codebase_eval.md`
   - external audit prompt, not the implementation source of truth
5. `memory.md`
   - historical research and decision archive
6. `docs/build-guide.md`
   - historical build sequence, partly stale

## Read This Before Coding

If you are continuing implementation:

1. Read this file fully.
2. Read [docs/accuracy-first-10-10-roadmap.md](./accuracy-first-10-10-roadmap.md).
3. Follow the active backend path listed below.
4. Treat older markdown files as context, not authority.

If two docs conflict:

- `docs/ai-handoff.md` wins for current implementation state
- `docs/accuracy-first-10-10-roadmap.md` wins for target architecture and roadmap
- older docs are archive unless explicitly refreshed

## Current Goal

RAVEN is no longer being shaped as a generic LLM-to-SQL app.

The active goal is:

- best-in-class text-to-SQL correctness for a configured analytics domain pack
- deterministic planning first
- semantic contracts, trusted query families, Metabase evidence, OpenMetadata scoring, and value grounding before fallback generation
- abstain instead of returning elegant wrong answers

## Hard Rules

- Do not hardcode company-specific business logic into the engine.
- Keep domain knowledge in config or domain-pack assets.
- OpenMetadata and Metabase are evidence, not semantic authority.
- Semantic contracts and reviewed trusted queries are the authority.
- Prefer deterministic planning over free-form SQL generation.
- If confidence is weak, clarify or abstain.
- Accuracy beats UI, convenience, and broad connector coverage.

## OSS Position

RAVEN should remain open-source and generic at the engine level.

- core engine logic must stay domain-agnostic
- business semantics must be configurable
- target-domain accuracy should come from contracts, instructions, verified queries, and metadata assets
- do not "improve accuracy" by silently baking one company's vocabulary into core Python logic

## Progress Snapshot

As of `March 7, 2026`:

- overall roadmap: `~70%`
- accuracy-core architecture: `~92%`
- production/runtime hardening: `~43%`

Current passing suites:

- focused accuracy-first suite: `55 passed`
- smoke suite: `137 passed`

## What Is Already Implemented

### Semantic / Contract Layer

- configurable semantic model / domain-pack loading
- semantic contract validation with startup warnings and errors
- semantic keyword routing derived from semantic assets
- generic engine direction with externalized domain knowledge

Key files:

- `src/raven/semantic_assets.py`
- `src/raven/contracts/registry.py`
- `src/raven/contracts/models.py`
- `src/raven/contracts/validator.py`

### Trusted Query Path

- exact trusted query lane
- query-family matching using verified queries and Metabase evidence
- trusted query reuse across:
  - filter value changes
  - time-window changes
  - time-grain changes
  - top/bottom changes
  - same-table metric swaps
  - same-table dimension swaps
  - join-aware dimension swaps
  - grouped categorical comparisons

Key files:

- `src/raven/query_families/matcher.py`
- `src/raven/query_families/compiler.py`

### Grounding / Linking

- value grounding from semantic enums, business rules, and content-awareness values
- deterministic join policy
- deterministic linker
- schema selector seeded with semantic and trusted-query evidence
- non-destructive column pruning that restores required metric/dimension/join columns

Key files:

- `src/raven/grounding/value_resolver.py`
- `src/raven/schema/join_policy.py`
- `src/raven/schema/deterministic_linker.py`
- `src/raven/schema/schema_selector.py`
- `src/raven/schema/column_pruner.py`

### Planning / Compilation

- typed query plans
- deterministic planner coverage for:
  - KPI
  - grouped aggregate
  - top-k
  - share / contribution
  - filter percentage
  - percentage breakdown by category
  - period growth
  - grouped period growth
  - categorical comparison / breakdown aggregate
  - categorical comparison / breakdown count
- narrow internal AST-style SQL compilation for deterministic plans

Key files:

- `src/raven/planning/query_plan.py`
- `src/raven/planning/deterministic_planner.py`
- `src/raven/sql/ast_builder.py`
- `src/raven/sql/trino_compiler.py`

### Validation / Abstention

- plan-aware SQL validation
- hard vs soft plan-violation gating in candidate selection
- rejection of structurally invalid candidates before pairwise comparison
- pipeline-level abstention when validation rejects all candidates
- execution-grounded result sanity checks after SQL execution
- pipeline-level abstention when returned data shape contradicts the plan

Key files:

- `src/raven/validation/query_plan_validator.py`
- `src/raven/validation/candidate_selector.py`
- `src/raven/validation/execution_judge.py`
- `src/raven/pipeline.py`

## Active Runtime Path

This is the real backend path to understand first:

1. `src/raven/pipeline.py`
2. `src/raven/semantic_assets.py`
3. `src/raven/retrieval/information_retriever.py`
4. `src/raven/schema/schema_selector.py`
5. `src/raven/planning/deterministic_planner.py`
6. `src/raven/generation/candidate_generator.py`
7. `src/raven/validation/candidate_selector.py`
8. `src/raven/validation/execution_judge.py`
9. `src/raven/output/renderer.py`

Important note:

- some legacy modules still exist in the repo
- not all of them are the real production path anymore
- do not assume `sql_generator.py`, `selector.py`, or older retrieval modules are the active architecture just because they exist

## Module Status

Use these labels mentally while working:

- `active`: part of the intended accuracy-first path
- `transitional`: still used, but should be narrowed or replaced
- `legacy`: should not lead architecture decisions

Current rough status:

- `active`
  - `src/raven/pipeline.py`
  - `src/raven/semantic_assets.py`
  - `src/raven/contracts/`
  - `src/raven/query_families/`
  - `src/raven/grounding/value_resolver.py`
  - `src/raven/schema/schema_selector.py`
  - `src/raven/schema/column_pruner.py`
  - `src/raven/schema/deterministic_linker.py`
  - `src/raven/schema/join_policy.py`
  - `src/raven/planning/`
  - `src/raven/sql/`
  - `src/raven/validation/query_plan_validator.py`
  - `src/raven/validation/candidate_selector.py`
  - `src/raven/validation/execution_judge.py`
- `transitional`
  - `src/raven/generation/candidate_generator.py`
  - `src/raven/generation/revision_loop.py`
  - `src/raven/schema/column_filter.py`
  - `src/raven/schema/table_selector.py`
- `legacy`
  - `src/raven/retrieval/context_retriever.py`
  - `src/raven/schema/selector.py`
  - `src/raven/generation/sql_generator.py`
  - `src/raven/validation/selection_agent.py`
  - `src/raven/validation/error_taxonomy_checker.py`

## What Is Still Missing

### Accuracy-Core Gaps

- instruction assets are not yet first-class compiled policy objects
- value indexes and richer ambiguity policy are missing
- constrained fallback generation is not implemented
- confidence modeling is still heuristic and incomplete
- benchmark-first release gating is not implemented
- provenance exists, but it is still too lightweight

### Runtime / Production Gaps

- Redis/shared cache and rate limiting are not in place
- Trino session reuse / pooling is not in place
- ANN/vector retrieval redesign is not in place
- distributed-safe focus/upload state is still not cleaned up
- the `prometheus_client` dependency/bootstrap mismatch is still unresolved

### Known Legacy / Drift Areas

- `src/raven/schema/column_filter.py`
- `src/raven/schema/table_selector.py`
- `src/raven/retrieval/context_retriever.py`
- `src/raven/schema/selector.py`
- `src/raven/generation/sql_generator.py`
- `src/raven/validation/selection_agent.py`
- `src/raven/validation/error_taxonomy_checker.py`

These files are not the right place to lead the architecture unless the task is explicitly legacy cleanup.

## Recommended Next Work

Priority order:

1. Add calibrated confidence scoring that combines:
   - plan consistency
   - cost guard
   - execution sanity
   - retrieval evidence strength
   - ambiguity
2. Integrate the real `CostGuard` into selection and abstention logic.
3. Constrain the fallback generation path:
   - structured output first
   - grammar / AST checks
   - fewer candidates
4. Build the benchmark runner and make benchmark delta the release gate.
5. Add value indexes and clarification behavior for ambiguous entity matches.
6. Replace the narrow SQL compiler with a fuller AST/compiler path.
7. Harden runtime:
   - shared cache
   - shared rate limit
   - Trino session reuse
   - vector indexing

## Known Sharp Edges

- The Prometheus/bootstrap dependency mismatch is still unresolved in the supported Docker path.
- Runtime/distributed correctness still lags behind accuracy-core progress.
- Some public docs still describe the earlier 8-stage LLM-heavy architecture more strongly than the current compiler-first direction.
- The benchmark is still not the release gate, so claims of improvement are still architecture-led more than benchmark-led.

## Testing Commands

Use these commands first when continuing accuracy-core work:

```bash
python -m pytest -q tests/test_execution_judge.py tests/test_candidate_selector.py tests/test_accuracy_path.py tests/test_query_plan_validator.py tests/test_sql_compiler.py tests/test_query_families.py tests/test_semantic_contracts.py
python -m pytest -q tests/test_basic.py tests/test_week3.py
```

## Documentation Maintenance Rules

When continuing implementation:

- update `docs/accuracy-first-10-10-roadmap.md` if progress or phase status changes materially
- update this handoff doc when the active architecture or next-step priorities change
- do not add more overlapping strategy docs unless absolutely necessary
- if a markdown file becomes historical, mark it clearly as archive / historical context
- when you make a material implementation change, update the progress percentages in both this file and the roadmap

## Historical Files

- `memory.md`
  - useful for research history
  - contains stale milestones and earlier project assumptions
- `docs/build-guide.md`
  - useful only as historical sequencing context
  - not the current implementation source of truth
- `prompts/gpt54_codebase_eval.md`
  - useful for external audit
  - should be read together with this file and the roadmap
