# RAVEN Accuracy-First 10/10 Technical Roadmap

Read [docs/ai-handoff.md](./ai-handoff.md) first for the current implementation state and active backend path.

## Purpose

This document is the implementation roadmap to make RAVEN `10/10` on technical grounds for its accuracy-first target use case:

- Primary goal: best-in-class **text-to-SQL correctness** on a configurable Trino/dbt/Metabase/OpenMetadata domain pack
- Secondary goals: performance, architecture, code quality, scalability, then security/governance
- Explicit non-goal for now: polished UI
- Explicit non-goal forever: becoming a generic "ask any database anything" product

This roadmap assumes:

- architecture changes are allowed
- backward compatibility is optional
- legacy modules can be removed
- the team is willing to reject "smart-looking" answers if they are not trustworthy
- the team is willing to optimize for domain-pack quality rather than broad connector coverage

This file is the strategic roadmap.
The canonical handoff/state file is [docs/ai-handoff.md](./ai-handoff.md).

## Executive Position

The current system has the right broad idea, but it is still too close to an LLM orchestration app and too far from a domain analytics compiler.

That distinction matters.

If the product wants the best answer quality for a target analytics domain, the system should not primarily ask:

- "What SQL should the model write?"

It should ask:

- "Can this question be mapped to a trusted metric, entity, join path, and query family before any free-form SQL generation happens?"

The staged architecture is **not** the main problem.
The main problem is that too many accuracy-critical decisions still happen through prompt interpretation instead of deterministic contracts.

## Explicit Product Scope

RAVEN is **not** trying to be:

- DataGrip with chat
- TablePlus with an LLM plugin
- a generic SQL chat app
- a cross-database toy that works "okay" on many schemas

RAVEN **is** trying to be:

- the most accurate analytics question-answering engine for a configured Trino + dbt + Metabase + OpenMetadata domain pack
- a semantic compiler that knows the active metrics, entities, join paths, and trusted dashboards better than general-purpose tools do
- a system that can say "I do not know" or ask a clarification question instead of guessing

## Current Baseline

Current subjective score:

- Architecture: `6/10`
- Performance: `5/10`
- Code Quality: `5/10`
- Scalability: `4/10`
- Security: `5/10`

Current strengths:

- The repo already has a multi-stage query path instead of a naive prompt-to-SQL loop
- The repo already has useful domain assets: `config/semantic_model.yaml`, dbt metadata, Metabase SQL, OpenMetadata lineage, focus documents, and a feedback-loop skeleton
- The repo already has evaluation code: `tests/eval_accuracy.py`, `tests/eval_ab_candidates.py`
- The repo now has a real semantic asset layer in `src/raven/semantic_assets.py`
- The repo now has configurable domain-pack loading plus semantic contract validation in `src/raven/contracts/`
- The repo now has a trusted query-family engine in `src/raven/query_families/`
- The repo now has a first-class value-grounding subsystem in `src/raven/grounding/value_resolver.py`
- The repo now has deterministic join policy and linker components in `src/raven/schema/join_policy.py` and `src/raven/schema/deterministic_linker.py`
- The repo now has a typed deterministic planner and plan-aware validator in `src/raven/planning/query_plan.py`, `src/raven/planning/deterministic_planner.py`, and `src/raven/validation/query_plan_validator.py`

Current blockers:

- The system is still too LLM-first and too weakly typed in the hot path
- Business semantics are still under-specified relative to how much the model is expected to infer
- Schema linking is only partially deterministic; `column_filter.py` and `table_selector.py` still remain too LLM-heavy
- Value grounding exists now, but value indexes, ambiguity policy, and clarification behavior are still incomplete
- The benchmark is still too execution-biased and too weakly tied to judged business correctness
- The SQL compiler is still narrow and not yet a full `sqlglot`-backed compiler
- The fallback generation path is still too powerful for a `10/10` accuracy-first system
- There is still architecture drift between active modules and legacy modules

## Current Implementation Snapshot

This section reflects the codebase after the accuracy-first work already implemented in this repository.

As of `March 7, 2026`:

Rough completion:

- overall `10/10` roadmap: `~70%`
- accuracy-core architecture: `~92%`
- production/runtime hardening: `~43%`

What is implemented:

- configurable semantic/domain-pack loading via `RAVEN_DOMAIN_PACK_PATH` and `RAVEN_SEMANTIC_MODEL_PATH`
- semantic contract validation with startup-time errors and warnings
- semantic keyword routing derived from loaded semantic assets instead of engine hardcoding
- deterministic value grounding from semantic enums, business rules, content-awareness signals, and Metabase filters
- deterministic join policy and deterministic linker
- deterministic schema seeding and non-destructive pruning in `schema_selector.py` and `column_pruner.py`
- typed query plans with plan-aware validation
- narrow typed SQL AST building and deterministic Trino compilation in `src/raven/sql/`
- trusted exact-match query lane
- trusted query-family lane backed by verified queries and Metabase cards
- conservative query-family compilation for:
  - top/bottom changes
  - time-window changes
  - time-grain changes
  - grounded filter-value changes
  - safe same-table dimension swaps
  - safe join-aware dimension swaps
  - safe same-table metric swaps
  - grouped and categorical comparison reuse
- deterministic planner coverage for:
  - KPI
  - grouped aggregate
  - top-k
  - share / contribution
  - filter percentage
  - categorical percentage breakdown
  - period growth
  - grouped period growth
  - categorical comparison / breakdown aggregate
- hard vs soft plan gating in `candidate_selector.py`
- execution-grounded result sanity checks in `execution_judge.py`
- pipeline-level abstention before render when validation or result sanity fails

What is only partially implemented:

- semantic instructions as durable compiled policy assets
- deterministic schema selection end-to-end
- confidence modeling
- benchmark-first release gating
- distributed runtime correctness

What is not implemented yet:

- full `sqlglot`-backed SQL AST compiler
- constrained fallback generation
- calibrated confidence model that combines plan, cost, and result sanity
- Redis/shared cache and rate limit
- pooled Trino sessions
- ANN/vector retrieval redesign
- benchmark runner as a hard release gate

Current validation snapshot:

- focused accuracy-first suite currently passes: `55 passed`
- broader smoke suite currently passes: `137 passed`
- current passing suites cover query families, semantic contracts, query-plan validation, candidate selection, execution judging, accuracy-path slices, and smoke coverage

## Research Inputs That Should Change The Design

This roadmap is based on the current codebase plus the following primary sources.

### Vendor patterns that matter

- Snowflake Cortex Analyst documentation:
  - semantic models are explicit assets
  - verified queries are a first-class optimization path
  - implication for RAVEN: `config/semantic_model.yaml` should become an executable contract, not prompt context
  - source: <https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-analyst>
  - source: <https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-analyst/verified-query-repository>
  - source: <https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-analyst/semantic-model-spec>

- Databricks Genie / AI-BI documentation:
  - trusted assets, instructions, SQL examples, and sampled values materially improve responses
  - implication for RAVEN: value dictionaries and query examples must become explicit assets, not incidental prompt fragments
  - source: <https://docs.databricks.com/gcp/ai-bi>
  - source: <https://docs.databricks.com/gcp/en/genie/index.html>
  - source: <https://docs.databricks.com/gcp/en/genie/set-up-space>

- Vanna documentation:
  - correct question-SQL pairs are disproportionately valuable
  - implication for RAVEN: the feedback loop must store and retrieve verified internal examples aggressively
  - source: <https://vanna.ai/docs/train/>
  - source: <https://vanna.ai/docs/training-advice/>

### Research patterns that matter

- Spider 2.0 benchmark:
  - enterprise text-to-SQL is harder than classic benchmarks because workflows, ambiguity, and realism matter
  - implication for RAVEN: a nice Spider-style prompt pipeline is not enough for enterprise correctness
  - source: <https://spider2-sql.github.io/>

- BIRD benchmark:
  - realistic text-to-SQL systems must handle large schemas, dirty values, and external knowledge
  - implication for RAVEN: value grounding and business knowledge are mandatory
  - source: <https://bird-bench.github.io/>

- CHESS:
  - staged retrieval plus schema selection is directionally right for enterprise databases
  - implication for RAVEN: keep the staged idea, but strengthen the deterministic core inside those stages
  - source: <https://arxiv.org/abs/2405.16755>

- RAT-SQL:
  - schema linking quality is a first-order driver of SQL quality
  - implication for RAVEN: deterministic schema linking deserves its own subsystem, not just a prompt
  - source: <https://aclanthology.org/2020.acl-main.677/>

- PICARD:
  - constrained decoding reduces invalid SQL materially
  - implication for RAVEN: the fallback generator should not emit unconstrained final SQL strings
  - source: <https://aclanthology.org/2021.emnlp-main.779/>

- FLEX:
  - execution accuracy alone produces false positives and false negatives
  - implication for RAVEN: the benchmark must score judged answer correctness, not just "query ran"
  - source: <https://aclanthology.org/2025.naacl-long.228/>

- Enhancing Text-to-SQL Parsing through Question Rewriting and Execution-Guided Refinement:
  - rewrite plus execution feedback improves accuracy
  - implication for RAVEN: clarification and execution-grounded repair are good, but only after plan formation
  - source: <https://aclanthology.org/2024.findings-acl.324/>

- DAIL-SQL / execution-feedback style work:
  - better demonstrations and decomposition improve model performance
  - implication for RAVEN: few-shot context should come from query families and judged internal examples, not random nearest neighbors
  - source: <https://arxiv.org/abs/2308.15363>

- STaR-SQL / ExCoT-DPO / execution-feedback fine-tuning style work:
  - fine-tuning can help, but only after good trajectories and execution signals exist
  - implication for RAVEN: model specialization is phase 2 or 3, not phase 0
  - source: <https://arxiv.org/abs/2502.15449>
  - source: <https://arxiv.org/abs/2505.12520>

## Competitive Capabilities To Steal

This section makes the competitive posture explicit.

### Wren AI

Steal:

- executable semantic model
- explicit instructions as durable assets
- question-SQL pairs as first-class knowledge

Implication for RAVEN:

- `config/semantic_model.yaml` must split into validated semantic contracts plus instruction assets
- metric definitions, business caveats, and join rules must exist as machine-usable contracts
- query families must become a first-class module, not an incidental few-shot trick

Sources:

- <https://docs.getwren.ai/cp/getting_started/playbook>
- <https://docs.getwren.ai/oss/engine/concept/what_is_mdl>
- <https://docs.getwren.ai/oss/guide/knowledge/question-sql-pairs>
- <https://docs.getwren.ai/cp/guide/knowledge/instructions>

### MinusX

Steal:

- Metabase-native asset retrieval
- dashboard/card/snippet reuse
- source-backed context rather than generic retrieval

Implication for RAVEN:

- Metabase cards, dashboards, snippets, and saved questions should become trusted evidence and query-family seeds
- a response should know which Metabase assets influenced the plan
- the planner should prefer a known trusted dashboard/card path over open-ended SQL generation

Sources:

- <https://docs.minusx.ai/en/articles/11813453-explorer-agent>
- <https://docs.minusx.ai/en/articles/11777797-connect-metabase>

### QueryWeaver

Steal:

- graph-first schema reasoning
- relationship-centric traversal instead of flat table ranking

Implication for RAVEN:

- dbt lineage plus OpenMetadata lineage should feed an explicit join-policy graph
- schema linking should operate over approved graph paths, not just tables and columns

Source:

- <https://github.com/FalkorDB/QueryWeaver>

### Vanna

Steal:

- verified question-SQL memory
- training-data-first product mindset

Implication for RAVEN:

- accepted production queries must flow into query families, not just vector memory
- benchmarked, reviewed examples should outweigh model cleverness

Sources:

- <https://vanna.ai/docs/train/>
- <https://vanna.ai/docs/training-advice/>

### SQLAI.ai

Steal:

- workspace-level rules and controlled generation boundaries

Implication for RAVEN:

- instruction assets should encode business-specific do/don't rules
- generated SQL must be constrained by those rules before execution

Source:

- <https://www.sqlai.ai/generators/text-to-sql-query-generator/>

### DBHub

Steal:

- tool-style, progressive metadata retrieval instead of dumping too much schema at once

Implication for RAVEN:

- retrieval should progressively fetch only the metadata needed for the current plan
- token-efficient evidence retrieval should replace broad prompt stuffing

Source:

- <https://docs.dbhub.ai/>

### DataGrip And TablePlus

Steal:

- tight schema awareness
- fast operator feedback loops

Do not copy:

- IDE/plugin posture as the product strategy

Sources:

- <https://www.jetbrains.com/help/ai-assistant/use-ai-with-databases.html>
- <https://docs.tableplus.com/llm-plugin>

### SQL Chat

Steal:

- almost nothing for the accuracy core besides lightweight conversational UX

Do not copy:

- generic chat-over-schema product shape

Source:

- <https://github.com/sqlchat/sqlchat>

## What The Repo Still Lacks Relative To The Best Tools

These are the missing capabilities that matter most for accuracy.

- semantic contracts are present, but they are not yet the primary executable authority
- business instructions are not first-class assets
- Metabase dashboards/cards/snippets are not yet harvested as trusted query families
- value grounding is not yet a dedicated subsystem with ambiguity handling
- join reasoning is not yet governed by an explicit approved-graph policy
- most common intents still do not compile through deterministic plans
- answer provenance is still too weak; the system should say which contract, query family, dashboard, or lineage path it used
- OpenMetadata is integrated as metadata enrichment, but not yet elevated into the deterministic scoring core

## Brutal Assessment Of The Current Architecture

The current architecture is **promising**, but several parts are actively hostile to correctness.

### What is sound

- `src/raven/pipeline.py` as a single orchestrator
- a staged query path instead of single-shot prompting
- use of semantic metadata, lineage, and prior queries
- validation as a distinct stage instead of "first SQL wins"

### What is sabotaging accuracy

- `src/raven/schema/column_filter.py` and `src/raven/schema/table_selector.py` still make too many early decisions via LLM prompts
- `src/raven/generation/candidate_generator.py` is still too close to "infer SQL from context" for common metric questions that should be compiled
- `src/raven/generation/revision_loop.py` can drift semantics while trying to fix SQL
- `src/raven/probes/probe_runner.py` and probe logic are not yet a formal grounding layer with deterministic contracts
- `src/raven/validation/candidate_selector.py` now has hard/soft plan gating, but it still lacks real cost-guard integration and a calibrated confidence model
- `src/raven/cache.py` still reflects an optimization-first mindset instead of a correctness-first one
- `tests/eval_accuracy.py` still overweights execution success relative to business correctness

### The architectural conclusion

Do **not** throw away the staged pipeline.

Do throw away the idea that the LLM should remain the default decider for:

- table selection
- join-path choice
- metric interpretation
- grouping grain
- value resolution
- final SQL string construction for common intents

## Non-Negotiable Design Rules

These are the rules for a domain-best text-to-SQL system.

1. Business logic lives in semantic contracts, not prompts.
2. Verified query families outrank free-form generation.
3. Common intents compile to a typed query plan and then to SQL AST.
4. LLMs may help with intent extraction and edge cases, but they should not invent core business semantics.
5. Value grounding is mandatory for production answers.
6. Clarification is better than an elegant wrong answer.
7. Benchmark improvement is the release gate.
8. No optimization is acceptable if it increases wrong-answer rate.
9. Side features do not belong on the critical path.
10. Metabase SQL and OpenMetadata lineage are evidence, not authority.
11. dbt semantic contracts and judged internal queries are the authority.
12. The hot path stays as a modular monolith until the compiler core is stable.
13. Metabase dashboards, cards, and snippets must become trusted evidence assets, not just optional focus context.
14. OpenMetadata must inform deterministic scoring for lineage, quality, ownership, and domains.
15. The product will optimize for the active domain pack, not for generic database coverage.

## North-Star Product Definition

RAVEN should become a **domain analytics compiler with an LLM fallback**, not an LLM app with retrieval hints.

The target stack is:

- semantic contracts for metrics, dimensions, joins, and policies
- instruction assets for business caveats and query rules
- value-grounding services for entity resolution
- trusted query families mined from verified SQL and Metabase assets
- deterministic planning and SQL AST compilation for common intents
- constrained fallback generation only for the unresolved tail

### Target query lanes

1. Exact trusted query lane
   - exact or near-exact match to a verified query family
   - deterministic slot filling
   - deterministic SQL AST build

2. Planned query lane
   - question maps to a known intent type and canonical metric
   - deterministic schema linker resolves most of the plan
   - constrained generation only fills the unresolved fragments

3. Ambiguous query lane
   - system asks a clarification question before running SQL

4. Open-ended fallback lane
   - only used when semantic family, planner, and deterministic linker cannot fully cover the question
   - still constrained by AST rules, join policy, cost guard, and execution-grounded judging

### Target query flow

1. Normalize question
2. Resolve conversation context
3. Run lightweight rewrite if necessary
4. Classify domain and intent
5. Resolve semantic contracts and instruction assets
6. Resolve candidate query families from verified examples and Metabase assets
7. Resolve metric, dimensions, and entities with value grounding
8. Run deterministic schema linker and join policy
9. Build typed query plan
10. Compile SQL AST directly where possible
11. Use constrained generation only for unresolved pieces
12. Validate by plan, AST, join policy, execution evidence, and source provenance
13. Return answer only if confidence is above threshold
14. Otherwise clarify or refuse

## Success Metrics

These metrics define whether the system is actually improving.

### Primary accuracy metrics

- judged answer correctness on internal benchmark: `>= 90%`
- execution accuracy on internal benchmark: `>= 95%`
- wrong-answer rate on top 100 business-critical intents: `<= 2%`
- canonical metric correctness for revenue/enrollment/engagement intents: `>= 97%`
- join-path correctness on benchmarked multi-table queries: `>= 95%`
- value-resolution correctness on benchmarked entity-heavy queries: `>= 95%`

### Confidence metrics

- high-confidence wrong-answer rate: `<= 1%`
- useful clarification rate on ambiguous questions: `>= 80%`
- abstention quality:
  - when the system refuses or clarifies, it should be correct to do so at least `80%` of the time

### Runtime metrics

- p95 latency:
  - exact trusted lane: `< 2.5s`
  - planned lane: `< 5s`
  - fallback lane: `< 8s`
- average embedding calls per request reduced from current baseline by at least `30%`
- cost per correct answer reduced materially from current baseline

### Reliability metrics

- zero wrong cached answers across conversation/focus/user boundaries
- zero live-path import/runtime breakages
- no correctness-critical feature depends on local process memory or local files
- 100% coverage of benchmarked business-critical paths by automated tests
- `>= 80%` of business-critical answers cite trusted internal evidence sources in debug/provenance output

## What 10/10 Means

### Architecture 10/10

- one canonical backend path
- business semantics are encoded explicitly
- typed intermediate representations exist between stages
- query families, semantic contracts, join policy, and value grounding are first-class modules
- no production behavior depends on legacy duplicate modules
- trusted internal assets are first-class citizens: contracts, instructions, dashboards, cards, snippets, verified queries

### Performance 10/10

- trusted queries avoid unnecessary LLM calls
- embeddings are reused per request
- vector search remains fast at scale
- Trino concurrency is bounded and predictable
- summaries/charts/follow-ups are fully off the critical path

### Code Quality 10/10

- accuracy-critical modules are small, typed, and individually testable
- no giant implicit dictionaries flowing through the hot path
- planner, linker, compiler, validator, and confidence layers have explicit contracts
- benchmark and regression reports are standard outputs of development
- provenance is explicit on every production answer path

### Scalability 10/10

- correctness does not depend on process-local state
- retrieval and value lookup scale with domain size
- the system behaves predictably with concurrent requests
- offline indexing and evaluation can grow independently of the API

### Security 10/10

- read-only execution remains enforced
- auth and audit are present
- file/upload paths are safe
- schema visibility can become role-aware later without redesigning the accuracy core

## What To Remove, Demote, Or Rewrite

This is the part that should be explicit.

### Remove or demote from the accuracy-critical path

- free-form table selection as the first source of truth
- free-form join-path discovery when an explicit join policy can exist
- 3-candidate open-ended generation for simple KPI and grouped aggregate questions
- prompt-heavy column pruning before metric and entity resolution
- generic nearest-neighbor few-shot retrieval with no query-family or evidence contract
- non-essential follow-up suggestion generation on the hot path
- any cache lookup that happens before full request scope is understood

### Rewrite, not just tune

- `src/raven/schema/column_filter.py`
  - rewrite into a deterministic scorer with optional LLM tie-breaking

- `src/raven/schema/table_selector.py`
  - stop using this as the primary selector; it should become a fallback disambiguator after deterministic ranking

- `src/raven/schema/column_pruner.py`
  - make it plan-aware and metric-aware, not just context-aware

- `src/raven/probes/probe_runner.py`
  - convert into a deliberate value/profile probing layer with well-defined inputs and outputs

- `src/raven/retrieval/fewshot_retriever.py`
  - stop treating this as the main learning mechanism; fold it into query-family and evidence retrieval

- `src/raven/validation/candidate_selector.py`
  - replace with a plan-aware execution judge and confidence scorer

- `src/raven/generation/revision_loop.py`
  - restrict it to syntactic and dialect repair unless explicit evidence supports a semantic repair

### Delete or quarantine as legacy

- `src/raven/retrieval/context_retriever.py`
- `src/raven/schema/selector.py`
- `src/raven/generation/sql_generator.py`
- `src/raven/validation/selection_agent.py`
- `src/raven/validation/error_taxonomy_checker.py`

These modules create false optionality and confuse what the real system is.

## Target Accuracy Architecture

### 1. Semantic Contract Registry

### Goal

Turn `config/semantic_model.yaml` into an executable domain contract.

### Why

Every leading enterprise system that performs well in a domain leans on explicit semantic assets. The current repo has the seed of this, but it is underused.

### Actions

- split `config/semantic_model.yaml` into domain-scoped contracts if it grows large:
  - `config/semantic/contracts/revenue.yaml`
  - `config/semantic/contracts/enrollment.yaml`
  - `config/semantic/contracts/engagement.yaml`
- define for each metric:
  - owner table(s)
  - grain
  - time column
  - default filters
  - additive/semi-additive behavior
  - disallowed joins
  - synonyms
  - examples
- define for each dimension:
  - canonical column
  - alias list
  - allowed source tables
  - normalization rules
- define approved join edges with multiplicity and risk annotations

### New modules

- `src/raven/contracts/registry.py`
- `src/raven/contracts/models.py`
- `src/raven/contracts/validator.py`
- `src/raven/contracts/ownership.py`
- `src/raven/contracts/policies.py`

### 2. Semantic Instruction Layer

### Goal

Turn business instructions into durable assets rather than prompt text.

### Why

Wren AI and SQLAI-style systems are strong where they make instructions durable. RAVEN needs that, but the instructions should feed the planner, linker, compiler, and validator rather than simply being pasted into prompts.

### Actions

- create domain-scoped instruction files:
  - revenue rules
  - enrollment rules
  - center and geography rules
  - exclusions and caveats
- encode instruction types:
  - preferred metric interpretation
  - forbidden joins
  - required default filters
  - preferred source tables
  - known gotchas and caveats
- compile instructions into planner and validator policies, not just prompts

### New modules

- `src/raven/contracts/instructions.py`
- `src/raven/contracts/instruction_compiler.py`

### 3. Trusted Query Family Engine

### Goal

Make top business questions compile through reusable query families instead of raw generation.

### Why

Vendor practice and Vanna-style systems both show the importance of verified examples. The difference here is that RAVEN should go beyond retrieval and compile families with slots.

### Actions

- group verified queries into families:
  - metric lookup
  - grouped aggregate
  - time series
  - comparison
  - top-k
  - cohort/funnel/retention
- support slots:
  - metric
  - dimension
  - date range
  - grain
  - filters
  - top-k
  - ordering
- rank families with:
  - lexical score
  - embedding score
  - metric synonym score
  - entity overlap
  - prior acceptance rate
- ingest seeds from:
  - thumbs-up verified queries
  - benchmark gold queries
  - trusted Metabase questions/cards/snippets
  - semantic model examples

### New modules

- `src/raven/query_families/registry.py`
- `src/raven/query_families/matcher.py`
- `src/raven/query_families/compiler.py`
- `src/raven/query_families/provenance.py`

### 4. Metabase Asset Registry And Evidence Layer

### Goal

Make Metabase assets a first-class source of trusted analytics evidence.

### Why

MinusX is strong because it sits close to how business users already consume analytics. RAVEN should not treat Metabase as an optional side integration. It should mine dashboards, cards, questions, and snippets into reusable evidence for planning and query-family matching.

### Actions

- ingest Metabase assets:
  - dashboard metadata
  - cards and saved questions
  - SQL snippets
  - models where available
- classify assets by domain and metric coverage
- use those assets to:
  - seed query families
  - propose preferred source tables
  - attach answer provenance
- score assets higher when they are widely used or explicitly trusted

### New modules

- `src/raven/metabase/assets.py`
- `src/raven/metabase/evidence_store.py`
- `src/raven/metabase/query_family_sync.py`

### 5. Intent And Slot Extraction

### Goal

Map a question to a typed intent and structured slots before SQL.

### Why

The model should produce a contract like `metric=revenue, grain=day, group_by=center` rather than inventing SQL directly.

### Actions

- define intent classes:
  - KPI
  - grouped aggregate
  - time series
  - ranked list
  - comparison
  - contribution/share
  - cohort/funnel/retention
  - detail lookup
- extract slots into typed models
- separate:
  - domain intent
  - metric intent
  - entity references
  - time intent

### New modules

- `src/raven/planning/query_intent.py`
- `src/raven/planning/slot_extractor.py`
- `src/raven/planning/time_normalizer.py`

### 6. Value Grounding And Entity Resolution

### Goal

Make value resolution a first-class subsystem.

### Why

Many enterprise text-to-SQL failures are not schema failures. They are entity/value failures:

- center names
- batch names
- exam aliases
- year/phase aliases
- region aliases
- program names

This is also where dirty production data hurts text-to-SQL systems most.

### Actions

- build value dictionaries for high-value dimensions
- normalize aliases:
  - `fy24` -> financial year 2024
  - `jee mains` -> canonical exam code
  - `kota center` -> canonical center identifier
- create searchable value indexes for categorical columns
- use OpenMetadata profiles and Metabase filter metadata where available to bootstrap candidate values
- maintain ambiguity policy:
  - one confident match -> continue
  - multiple plausible matches -> ask clarification
  - no strong match -> refuse or ask

### New modules

- `src/raven/grounding/value_resolver.py`
- `src/raven/grounding/value_index.py`
- `src/raven/grounding/entity_normalizer.py`
- `src/raven/grounding/ambiguity_policy.py`

### 7. Deterministic Schema Linker

### Goal

Resolve tables and columns primarily through deterministic scoring.

### Why

RAT-SQL and later work reinforce that schema linking is a first-order problem. RAVEN should not ask the LLM to invent a join graph when the active domain pack already encodes the core analytical joins.

### Actions

- score tables and columns from:
  - metric ownership
  - dimension ownership
  - query-family prior
  - Metabase asset evidence
  - verified query history
  - focus scope
  - dbt lineage
  - OpenMetadata lineage
  - OpenMetadata ownership and quality signals
- encode trust penalties:
  - bronze/raw tables
  - deprecated assets
  - low-quality data sources
- choose from approved join edges only unless explicitly configured otherwise

### New modules

- `src/raven/schema/deterministic_linker.py`
- `src/raven/schema/join_policy.py`
- `src/raven/schema/source_of_truth.py`

### 8. Typed Query Plan

### Goal

Represent the intended query in an explicit intermediate representation before SQL generation.

### Why

Without a typed plan, it is too hard to validate whether a candidate SQL statement actually matches the business intent.

### Example plan shape

```json
{
  "intent": "TIME_SERIES",
  "metric": "net_revenue",
  "dimensions": ["center"],
  "grain": "day",
  "date_range": {"kind": "last_n_days", "value": 30},
  "filters": [{"field": "program", "op": "=", "value": "jee"}],
  "source_tables": ["analytics.revenue_facts", "analytics.centers_dim"],
  "join_path": ["revenue_facts.center_id = centers_dim.center_id"],
  "path_type": "TRUSTED_FAMILY"
}
```

### Actions

- define strict models
- validate planner output before SQL generation
- attach provenance:
  - exact match
  - family match
  - deterministic plan
  - fallback generation

### New modules

- `src/raven/planning/query_plan.py`
- `src/raven/planning/provenance.py`
- `src/raven/planning/evidence_bundle.py`

### 9. SQL AST Builders

### Goal

Generate SQL through AST construction whenever possible.

### Why

For common intents, the system should not ask an LLM to produce final SQL text. It should build SQL from the typed plan.

### Actions

- adopt an AST builder and formatter for Trino-compatible SQL
- compile:
  - KPI
  - grouped aggregate
  - time series
  - top-k
  - comparison
from the query plan directly
- keep SQL generation string-free until the final render step

### New modules

- `src/raven/sql/ast_builder.py`
- `src/raven/sql/trino_compiler.py`
- `src/raven/sql/render.py`

### Implementation note

Use a mature SQL AST library rather than building your own parser. `sqlglot` is a strong fit for AST generation and Trino dialect handling:

- source: <https://github.com/tobymao/sqlglot>

### 10. Constrained Fallback Generation

### Goal

Make open-ended SQL generation safer for the cases that remain.

### Why

PICARD-style constraints and AST validation reduce invalid SQL. More importantly, a constrained fallback narrows the semantic surface area the model can get wrong.

### Actions

- require structured model output:
  - typed plan JSON first
  - SQL fragments or AST patch second
- grammar-check all output
- restrict revision loop:
  - syntax repair allowed
  - dialect repair allowed
  - semantic repair only with explicit evidence
- reduce candidate count for planned queries
- keep multi-candidate generation only for genuinely open-ended cases

### New modules

- `src/raven/generation/constrained_sql.py`
- `src/raven/generation/sql_patcher.py`

### 11. Execution-Grounded Judge And Confidence Model

### Goal

Reject plausible wrong SQL aggressively.

### Why

The current pipeline still leans too much on "the SQL ran" as a success signal. That is not enough.

### Actions

- validate candidate against the query plan:
  - required tables present
  - disallowed tables absent
  - required filters present
  - expected grouping grain respected
  - join policy respected
- use execution evidence:
  - EXPLAIN cost thresholds
  - row-count sanity
  - dimension cardinality sanity
  - known metric range sanity
  - null-rate sanity for expected fields
- compute final confidence from:
  - path type
  - family match score
  - entity ambiguity
  - plan consistency
  - execution sanity
  - evidence strength from contracts, Metabase assets, and verified examples

### New modules

- `src/raven/validation/query_plan_validator.py`
- `src/raven/validation/execution_judge.py`
- `src/raven/validation/confidence_model.py`

### 12. Benchmark And Learning Loop

### Goal

Make judged correctness the central product-development loop.

### Why

FLEX shows execution accuracy is insufficient. Vendor docs also imply that systems improve by feeding back judged assets and examples.

### Actions

- store benchmark cases with:
  - question
  - canonical intent
  - expected metric
  - expected entities
  - allowed tables
  - expected evidence type
  - expected SQL family or answer properties
  - human correctness label
- version benchmark outputs
- feed accepted production queries back into query families and examples
- never auto-promote production queries without review for business-critical families

### New modules

- `src/raven/eval/benchmark_runner.py`
- `src/raven/eval/reporting.py`
- `src/raven/learning/query_acceptance.py`

### 13. OpenMetadata Usage Model

### Goal

Use OpenMetadata deeply, but in the right role.

### Why

OpenMetadata should materially improve deterministic scoring, lineage, quality awareness, and domain scoping. It should not be mistaken for the sole semantic authority for business metrics.

### OpenMetadata should be authoritative for

- lineage graph evidence
- ownership and domain metadata
- table and column profiles
- data quality warnings
- glossary enrichment
- domain-based scoping

### OpenMetadata should not be authoritative for

- canonical metric definitions
- final join policy for business-critical metrics
- trusted query-family ranking by itself

### Actions

- fold OM lineage, ownership, tags, and quality into deterministic scoring
- use OM profiles to improve value grounding and ambiguity handling
- use OM domains as default scope hints, not final semantic truth
- keep dbt contracts and reviewed query families above OM for business semantics

### 14. Distributed Runtime Layer

### Goal

Make the runtime operationally correct without distorting the accuracy architecture.

### Why

Operational instability leaks into accuracy. Broken caches, per-process state, and inconsistent retrieval all produce wrong answers.

### Actions

- shared cache and rate limit
- pooled Trino sessions
- ANN vector retrieval
- shared persistence for focus docs and uploads
- no correctness-critical state in process memory

## Keep / Replace / Delete

### Keep, but narrow responsibilities

- `src/raven/pipeline.py`
  - orchestrator only; business logic should move out

- `src/raven/retrieval/information_retriever.py`
  - retrieval helper only; it should not own semantics

- `src/raven/semantic_assets.py`
  - evolve into registry-backed semantic retrieval

- `src/raven/connectors/openmetadata_mcp.py`
  - keep as metadata evidence provider, not semantic authority

- `src/raven/connectors/metabase_client.py`
  - keep as asset ingestion bridge, not just focus-mode plumbing

- `src/raven/focus.py`
  - keep as scope/context input, not as a source of truth for metric semantics

### Replace or heavily redesign

- `src/raven/schema/column_filter.py`
- `src/raven/schema/table_selector.py`
- `src/raven/schema/column_pruner.py`
- `src/raven/probes/probe_runner.py`
- `src/raven/validation/candidate_selector.py`
- `src/raven/cache.py`
- `src/raven/connectors/trino_connector.py`
- `src/raven/connectors/pgvector_store.py`
- `tests/eval_accuracy.py`
- `web/routes/__init__.py`
  - Metabase and focus logic should not stay buried in route glue forever

### Restrict to fallback-only roles

- `src/raven/generation/candidate_generator.py`
- `src/raven/generation/revision_loop.py`

These should no longer be the default path for common analytics questions.

### Delete or quarantine as legacy

- `src/raven/retrieval/context_retriever.py`
- `src/raven/schema/selector.py`
- `src/raven/generation/sql_generator.py`
- `src/raven/validation/selection_agent.py`
- `src/raven/validation/error_taxonomy_checker.py`

## Workstreams

### Current Workstream Status

- Workstream A: `Low progress`
  - tests improved for new accuracy-path slices, but benchmark-first release gating is not implemented yet
- Workstream B: `Partial`
  - contract registry, contract models, and validation are implemented
  - instruction assets as compiled policy objects are not implemented yet
- Workstream C: `Strong partial`
  - exact trusted-query path is implemented
  - trusted query families work for exact, near-template, Metabase-backed, value-grounded, dimension-swapped, join-swapped, metric-swapped, and grouped comparison cases
  - provenance is still lightweight and Metabase asset sync is still incomplete
- Workstream D: `Strong partial`
  - value grounding exists
  - value indexes, richer ambiguity policy, and explicit clarification UX are still missing
- Workstream E: `Strong partial`
  - deterministic linker and join policy exist
  - upstream schema narrowing still relies too much on LLM stages, but schema selector seeding and pruning are now materially more deterministic
- Workstream F: `Strong partial`
  - typed query plans and deterministic planner exist
  - a narrow AST-style compiler exists, but not a full `sqlglot`-backed compiler
- Workstream G: `Low progress`
  - fallback is bypassed more often, but fallback itself is not yet constrained enough
- Workstream H: `Strong partial`
  - plan-aware validation exists
  - execution-grounded judging and abstention now exist
  - real cost-guard integration and calibrated confidence modeling are not complete
- Workstream I: `Low progress`
  - correctness-oriented work happened before runtime hardening; distributed/runtime fixes are still mostly pending
- Workstream J: `Partial`
  - typed subsystems have improved the hot path, but legacy path cleanup and CI are still incomplete
- Workstream K: `Not started`
- Workstream L: `Not started`

### Workstream A: Benchmark-First Development

Priority: `Critical`

### Goal

Make benchmark delta the release gate for backend work.

### Current files to evolve

- `tests/eval_accuracy.py`
- `tests/eval_ab_candidates.py`

### Required actions

- add judged answer correctness
- add wrong-answer severity labels
- add intent and query-family tags
- add expected entities and expected metric fields
- add business-critical benchmark:
  - `tests/test_set_business_critical.json`
- add benchmark reporting that outputs:
  - answer correctness
  - execution accuracy
  - table recall
  - join-path correctness
  - value-resolution correctness
  - latency
  - cost
  - confidence calibration

### Exit criteria

- no change ships without benchmark delta
- business-critical benchmark is the first slide in every backend review

### Workstream B: Semantic Contracts And Instructions

Priority: `Critical`

### Goal

Make semantic contracts and instruction assets executable and validated.

### Current files to evolve

- `config/semantic_model.yaml`
- `src/raven/semantic_assets.py`

### Required actions

- split semantic model by domain if needed
- add metric and dimension invariants
- add instruction assets and policy compilation
- add a semantic contract validator
- fail startup if contracts are invalid

### Exit criteria

- every benchmarked critical metric resolves through an explicit contract

### Workstream C: Trusted Query Families And Metabase Evidence

Priority: `Critical`

### Goal

Default top questions to family compilation instead of free-form generation, using verified queries and trusted Metabase assets.

### Current files to evolve

- `src/raven/semantic_assets.py`
- `src/raven/pipeline.py`
- `src/raven/connectors/metabase_client.py`
- `src/raven/focus.py`

### Required actions

- implement exact match, near match, and paraphrase match
- compile family templates with slots
- attach family provenance to response metadata
- sync trusted Metabase cards/questions/snippets into family seeds
- score evidence strength and keep it in debug output

### Exit criteria

- top 50 business questions execute without fallback generation

### Workstream D: Value Grounding

Priority: `Critical`

### Goal

Resolve values and entities correctly before SQL.

### Current files to evolve

- `src/raven/probes/probe_runner.py`
- `src/raven/retrieval/information_retriever.py`
- `src/raven/connectors/openmetadata_mcp.py`
- `src/raven/connectors/metabase_client.py`

### Required actions

- add normalized dictionaries for top business entities
- add lookup indexes for high-value dimensions
- add ambiguity policy and clarification prompts
- use OpenMetadata profiles and Metabase filter metadata as bootstrap signals

### Exit criteria

- value-heavy benchmark subset reaches `>= 95%` value-resolution correctness

### Workstream E: Deterministic Linker And Join Policy

Priority: `Critical`

### Goal

Make schema linking mostly deterministic.

### Current files to evolve

- `src/raven/schema/schema_selector.py`
- `src/raven/schema/graph_path_finder.py`

### Required actions

- rank tables by contract ownership and history
- encode approved joins explicitly
- penalize unsafe sources
- fold OpenMetadata lineage, ownership, and quality signals into deterministic scoring
- fold Metabase asset evidence into table and metric priors
- use LLM tie-breaking only after deterministic narrowing

### Exit criteria

- join-path correctness on benchmarked multi-table queries reaches `>= 95%`

### Workstream F: Query Planner And SQL AST Builders

Priority: `Critical`

### Goal

Make planned queries compile from a typed IR.

### Current files to evolve

- `src/raven/pipeline.py`
- `src/raven/generation/candidate_generator.py`
- `src/raven/semantic_assets.py`

### Required actions

- define intent models and query plan models
- compile supported intents to AST
- render Trino SQL from AST
- bypass fallback generation for supported plan shapes
- carry provenance and evidence bundles through the plan

### Exit criteria

- most simple and medium questions are answered through the trusted or planned lanes

### Workstream G: Constrained Fallback Generation

Priority: `High`

### Goal

Reduce failure rate in the remaining open-ended cases.

### Current files to evolve

- `src/raven/generation/candidate_generator.py`
- `src/raven/generation/revision_loop.py`

### Required actions

- require structured output
- AST-check before acceptance
- use family-aware and plan-aware examples only
- cut unnecessary candidate generation

### Exit criteria

- invalid SQL rate and semantic-drift repairs both decline materially

### Workstream H: Execution Judge And Confidence

Priority: `Critical`

### Goal

Make wrong answers rare by refusing aggressively when evidence is weak.

### Current files to evolve

- `src/raven/validation/candidate_selector.py`
- `src/raven/validation/cost_guard.py`
- `src/raven/output/query_executor.py`

### Required actions

- replace stubbed cost logic with the real guard
- add plan-consistency checks
- add execution sanity checks
- compute confidence from multiple signals instead of string labels only

### Exit criteria

- high-confidence wrong-answer rate drops below `1%`

### Workstream I: Runtime And Performance

Priority: `High`

### Goal

Remove runtime behavior that can corrupt or destabilize correctness.

### Current files to evolve

- `src/raven/connectors/trino_connector.py`
- `src/raven/connectors/pgvector_store.py`
- `src/raven/cache.py`
- `web/middleware/__init__.py`

### Required actions

- Redis cache and rate limit
- safe cache key semantics
- Trino session reuse and bounded concurrency
- ANN vector indexing
- request-scoped embedding reuse
- move summaries, charting, and suggestions off-path

### Exit criteria

- performance improves without any measured drop in judged correctness

### Workstream J: Codebase Cleanup

Priority: `High`

### Goal

Remove architecture drift and make the hot path auditable.

### Required actions

- quarantine legacy modules
- shrink `PipelineContext`
- replace loose dict-heavy contracts with typed models
- move semantic, Metabase, and OpenMetadata logic into explicit subsystems instead of route/orchestrator sprawl
- split giant route/UI files when they slow iteration on backend changes
- add CI for:
  - tests
  - lint
  - import smoke tests
  - benchmark smoke subset

### Exit criteria

- a new engineer can identify the real backend path in under 30 minutes

### Workstream K: Model Specialization

Priority: `Medium`

### Goal

Use fine-tuning only after the data and contracts are strong enough to justify it.

### Why

Fine-tuning before semantic contracts, value grounding, and a strict benchmark usually just teaches the model to imitate current mistakes more efficiently.

### Required actions

- collect verified question -> plan -> SQL -> outcome traces
- build paraphrases for top query families
- fine-tune or adapt:
  - intent classifier
  - slot extractor
  - fallback generator
not necessarily one model for all tasks
- use execution feedback and preference signals, not raw logs alone

### Exit criteria

- model specialization improves benchmarked correctness beyond the deterministic baseline

### Workstream L: Security Second Wave

Priority: `Medium`

### Goal

Raise security and governance after the accuracy core is stable.

### Required actions

- sanitize upload/delete paths
- require auth outside local development
- add audit logging
- prepare role-aware schema visibility later

### Exit criteria

- no trivial exposure remains while preserving the accuracy architecture

## What Not To Spend Time On Yet

These are tempting but lower ROI right now.

- rebuilding the UI
- generic multi-database feature expansion
- microservices decomposition of the hot path
- adding more candidate count to brute-force accuracy
- prompt-engineering-only improvements without semantic contracts
- auto-learning from production thumbs-up without review
- security or governance features that substantially delay the correctness program

## Phase Plan

### Phase 0: Remove Accuracy-Hostile Behavior

Duration: `1 week`

Status: `Partial`

### Must do

- fix bootstrap/runtime correctness breaks
- fix cache key correctness
- fix broken feedback and follow-up paths
- freeze legacy modules
- move non-essential side effects off-path

### Target outcome

The current pipeline becomes stable enough to benchmark honestly.

### Phase 1: Contracts, Benchmark, And Trusted Families

Duration: `2-4 weeks`

Status: `Strong partial`

### Must do

- benchmark redesign
- semantic contract registry
- semantic instruction registry
- trusted exact-match path
- trusted family matching for top business intents
- Metabase asset ingestion for trusted families

### Target outcome

Top business questions stop depending on generic generation by default.

### Phase 2: Value Grounding And Deterministic Linker

Duration: `3-5 weeks`

Status: `Strong partial`

### Must do

- value dictionaries and ambiguity handling
- deterministic linker
- join policy
- source-of-truth scoring
- OpenMetadata and Metabase evidence folded into deterministic ranking

### Target outcome

The system stops guessing entities and joins for common business questions.

### Phase 3: Planner, AST Builders, And Confidence Model

Duration: `3-5 weeks`

Status: `Strong partial`

### Must do

- typed query plan
- AST builders for common intents
- execution judge
- confidence model

### Target outcome

Most simple and medium queries become compiled queries, not generated queries.

### Phase 4: Constrained Fallback And Runtime Hardening

Duration: `2-4 weeks`

Status: `Partial`

### Must do

- constrained fallback generation
- bounded runtime concurrency
- shared cache/rate-limit/store
- ANN retrieval
- CI and cleanup

### Target outcome

The fallback path is safe enough and the runtime is production-correct.

### Phase 5: Model Specialization

Duration: `2-4 weeks`

Status: `Not started`

### Must do

- fine-tune targeted components only if benchmark data says it is worth it

### Target outcome

The model layer improves the strong architecture instead of compensating for a weak one.

## Concrete File-Level Plan

### Implemented So Far

- `src/raven/contracts/registry.py`
- `src/raven/contracts/models.py`
- `src/raven/contracts/validator.py`
- `src/raven/query_families/matcher.py`
- `src/raven/query_families/compiler.py`
- `src/raven/grounding/value_resolver.py`
- `src/raven/schema/deterministic_linker.py`
- `src/raven/schema/join_policy.py`
- `src/raven/schema/schema_selector.py`
- `src/raven/schema/column_pruner.py`
- `src/raven/planning/query_plan.py`
- `src/raven/planning/deterministic_planner.py`
- `src/raven/sql/ast_builder.py`
- `src/raven/sql/trino_compiler.py`
- `src/raven/validation/query_plan_validator.py`
- `src/raven/validation/candidate_selector.py`
- `src/raven/validation/execution_judge.py`
- `tests/test_semantic_contracts.py`
- `tests/test_query_families.py`
- `tests/test_query_plan_validator.py`
- `tests/test_candidate_selector.py`
- `tests/test_execution_judge.py`
- `tests/test_accuracy_path.py`

These files do not complete the roadmap, but they are now real implementation, not just planned additions.

### Immediate changes

- `src/raven/pipeline.py`
  - reduce to orchestration
  - move business logic into dedicated subsystems
  - ensure path provenance is explicit

- `src/raven/semantic_assets.py`
  - turn into a thin semantic asset access layer over validated contracts, instructions, query families, and evidence assets

- `src/raven/schema/schema_selector.py`
  - wrap deterministic linker and fallback disambiguation rather than owning selection logic directly

- `src/raven/generation/candidate_generator.py`
  - fallback-only generation
  - accept query plan and unresolved fragments instead of full free-form context

- `src/raven/connectors/metabase_client.py`
  - promote from integration helper to trusted asset ingestion source

- `src/raven/connectors/openmetadata_mcp.py`
  - promote from optional enrichment to deterministic scoring input

- `src/raven/generation/revision_loop.py`
  - syntax/dialect repair only by default

- `src/raven/validation/candidate_selector.py`
  - keep hard/soft plan gating, but add calibrated confidence and real cost-guard integration

- `tests/eval_accuracy.py`
  - judged correctness first
  - execution accuracy second

### New files to add next

- `src/raven/contracts/instructions.py`
- `src/raven/contracts/instruction_compiler.py`
- `src/raven/query_families/registry.py`
- `src/raven/query_families/provenance.py`
- `src/raven/metabase/assets.py`
- `src/raven/metabase/evidence_store.py`
- `src/raven/metabase/query_family_sync.py`
- `src/raven/grounding/value_index.py`
- `src/raven/grounding/ambiguity_policy.py`
- `src/raven/schema/source_of_truth.py`
- `src/raven/planning/query_intent.py`
- `src/raven/planning/slot_extractor.py`
- `src/raven/generation/constrained_sql.py`
- `src/raven/validation/confidence_model.py`
- `src/raven/eval/benchmark_runner.py`
- `tests/test_instruction_registry.py`
- `tests/test_metabase_evidence.py`
- `tests/test_value_resolver.py`
- `tests/test_deterministic_linker.py`
- `tests/test_query_plan.py`

## Benchmark Policy

Every major workstream must be measured on:

- `tests/test_set_200.json`
- `tests/test_set_business_critical.json`
- a manually judged top-25 stakeholder set
- a value-heavy ambiguity subset
- a join-heavy multi-table subset
- a Metabase-backed evidence subset

Every run must output:

- judged answer correctness
- execution accuracy
- SQL validity
- table recall
- join-path correctness
- value-resolution correctness
- evidence provenance coverage
- latency
- cost
- confidence calibration
- failure categories

Execution accuracy is necessary but insufficient.
Judged correctness is the real release gate.

## Product Rules

These rules should not be violated.

- Never prefer a fluent answer over a correct answer
- Never guess a metric definition
- Never guess an entity value when ambiguity is material
- Never allow a generated query to outrank a trusted family match without evidence
- Never ignore a stronger trusted Metabase/dashboard/query-family path in favor of generic generation
- Never let free-form SQL bypass join policy
- Never accept cache shortcuts that cross correctness boundaries
- Never improve latency by increasing wrong-answer rate
- Never auto-learn business-critical queries without review

## Done Criteria For 10/10

The project is `10/10` only when all of the following are true:

- judged answer correctness on business-critical benchmark `>= 90%`
- execution accuracy on the same benchmark `>= 95%`
- wrong-answer rate on top 100 questions `<= 2%`
- high-confidence wrong-answer rate `<= 1%`
- trusted or planned path coverage `>= 75%`
- value-resolution correctness `>= 95%`
- join-path correctness `>= 95%`
- evidence provenance coverage `>= 80%` on business-critical answers
- p95 latency within target for all lanes
- no correctness-critical dead paths remain
- no duplicate production modules remain
- benchmark smoke runs are enforced in CI
- runtime state is distributed-safe
- baseline security issues are fixed

Until then, the system may be strong, but it is not `10/10`.

## Final Engineering Direction

The final product should look like this:

- deterministic where business logic is known
- semantic-contract-driven by default
- instruction-driven where business caveats matter
- Metabase-evidence-aware by default
- value-grounded before SQL
- plan-first before generation
- AST-built where possible
- execution-judged where necessary
- benchmark-optimized, not demo-optimized

That is the path to beating generic text-to-SQL tools in this domain.
