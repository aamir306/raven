# OpenMetadata Requirements For Maximum RAVEN Accuracy

## Purpose

This document is the contract for the **OpenMetadata-side implementation** needed to maximize RAVEN accuracy.

It is written for:

- engineers or AI agents working in the OpenMetadata codebase
- teams exposing OpenMetadata MCP tools for RAVEN
- anyone curating metadata so RAVEN can answer analytics questions correctly

This is not a generic metadata wishlist.
It is the practical set of OpenMetadata capabilities that materially improve RAVEN's text-to-SQL accuracy.

## The Core Principle

From RAVEN's point of view:

- OpenMetadata is a **high-value evidence layer**
- OpenMetadata is **not** the sole semantic authority

RAVEN should use OpenMetadata for:

- search
- lineage
- profiles
- domains
- glossary enrichment
- ownership
- quality warnings
- write-back

RAVEN should **not** rely on OpenMetadata alone for:

- canonical metric definitions
- final business-logic truth
- approved query-family ranking by itself

Those should still come from semantic/domain-pack assets and reviewed trusted queries.

## Where RAVEN Uses OpenMetadata Today

These files are the current consumers:

- [openmetadata_mcp.py](/Users/aamir/trino-text-to-sql/src/raven/connectors/openmetadata_mcp.py)
- [information_retriever.py](/Users/aamir/trino-text-to-sql/src/raven/retrieval/information_retriever.py)
- [schema_selector.py](/Users/aamir/trino-text-to-sql/src/raven/schema/schema_selector.py)
- [deterministic_linker.py](/Users/aamir/trino-text-to-sql/src/raven/schema/deterministic_linker.py)
- [deterministic_planner.py](/Users/aamir/trino-text-to-sql/src/raven/planning/deterministic_planner.py)
- [focus.py](/Users/aamir/trino-text-to-sql/src/raven/focus.py)
- [pipeline.py](/Users/aamir/trino-text-to-sql/src/raven/pipeline.py)
- [openmetadata.yaml](/Users/aamir/trino-text-to-sql/config/openmetadata.yaml)

Current uses:

- semantic search for candidate tables
- glossary search for semantic matches
- lineage and column lineage for bridge-table and join-path discovery
- column profiles for content awareness and value grounding
- quality warnings as table-ranking and confidence signals
- domain-based focus/scoping
- write-back of glossary terms, lineage, test cases, and knowledge articles

## Priority Order

If the OpenMetadata team can only do a few things, do them in this order:

1. Strong table and column metadata quality
2. Reliable semantic search with relevance scores
3. Reliable table lineage and column lineage
4. Column profiles with safe sample values
5. Glossary terms with synonyms and SQL-oriented hints
6. Domain and ownership metadata
7. Data quality result summaries
8. Write-back surfaces for verified knowledge

## Minimum Required OpenMetadata Surface

These are the things that should exist and be usable from the OpenMetadata side.

### 1. Table Entity Contract

For each table, RAVEN needs:

- fully qualified name
- short name
- human-readable description
- owner
- domain
- tags / classifications
- updated timestamp
- table quality summary
- column list

Recommended response fields:

```json
{
  "fullyQualifiedName": "service.db.schema.table",
  "name": "table",
  "description": "Business-friendly description",
  "owner": {"name": "finance-data"},
  "domain": {"name": "Revenue"},
  "tags": [{"tagFQN": "Tier.Gold"}],
  "updatedAt": "2026-03-07T12:00:00Z",
  "testSuite": {"failed": 0},
  "columns": []
}
```

### 2. Column Entity Contract

For each column, RAVEN needs:

- name
- data type
- description
- constraint or key hint
- ordinal position
- tags
- profile payload

Recommended response fields:

```json
{
  "name": "center_id",
  "dataType": "BIGINT",
  "description": "Center identifier",
  "constraint": "PRIMARY_KEY",
  "ordinalPosition": 1,
  "tags": [{"tagFQN": "PII.None"}],
  "profile": {}
}
```

### 3. Semantic Search

RAVEN needs a table-oriented semantic search that returns:

- entity payload
- relevance score

Required behavior:

- search should work well on business phrases, not only exact table names
- score must be stable enough to use in ranking
- descriptions, tags, glossary links, and usage signals should influence ranking

Current RAVEN entry point:

- `semantic_search(query, entity_type="table", limit=20)`

### 4. Lineage And Column Lineage

RAVEN needs:

- table lineage graph
- direct column lineage between table pairs

Required behavior:

- lineage should include enough upstream/downstream depth for bridge discovery
- column lineage should expose `fromColumns` and `toColumns`
- lineage should be queryable fast enough for request-time use

Current RAVEN entry points:

- `get_lineage(entity_type, fqn, upstream_depth, downstream_depth)`
- `get_column_lineage(from_fqn, to_fqn)`

### 5. Glossary

RAVEN needs a glossary that is useful for query understanding, not only documentation.

Each glossary term should support:

- name
- description
- synonyms
- related terms
- optional SQL-oriented hint

Recommended response fields:

```json
{
  "name": "net revenue",
  "fullyQualifiedName": "Glossary.Revenue.net_revenue",
  "description": "Revenue after refunds",
  "synonyms": ["nr", "realized revenue"],
  "relatedTerms": [],
  "customProperties": {
    "sql_fragment": "SUM(net_revenue_amount)"
  }
}
```

Current RAVEN entry points:

- `search_glossary(query, limit=10)`
- `get_glossary_terms(glossary=None)`

### 6. Column Profiles

Profiles are one of the biggest OpenMetadata-side accuracy levers.

RAVEN needs:

- distinct count
- null proportion
- safe sample values
- data type
- min / max where available

Recommended response fields:

```json
{
  "columnProfile": [
    {
      "name": "status",
      "dataType": "STRING",
      "distinctCount": 4,
      "nullProportion": 0.0,
      "sampleValues": ["active", "inactive", "draft"]
    }
  ]
}
```

Why this matters:

- value grounding
- ambiguity handling
- filter normalization
- better candidate ranking

### 7. Data Quality Status

RAVEN uses quality data as a ranking and confidence signal.

OpenMetadata should expose:

- failing test count per table
- test result summaries
- root cause payload where available

Current RAVEN entry points:

- `get_test_case_results(table_fqn)`
- `get_rca(test_case_id)`

## High-Value Metadata To Add

These are not all mandatory on day one, but they materially improve accuracy.

### Table-Level Custom Properties

Recommended custom properties:

- `raven_role`
  - `fact`, `dimension`, `bridge`, `aggregate`, `staging`
- `raven_grain`
  - `order`, `user_day`, `center_day`, etc.
- `raven_entity_keys`
  - primary business keys
- `raven_time_columns`
  - list of time columns appropriate for analytics
- `raven_default_filters`
  - JSON or text rule describing default filters
- `raven_preferred_dimensions`
  - likely group-by columns
- `raven_deprecated_reason`
  - if the table should be avoided

### Column-Level Custom Properties

Recommended custom properties:

- `raven_semantic_role`
  - `metric_owner`, `dimension_key`, `label`, `time_dimension`, `status`, `amount`
- `raven_synonyms`
  - business aliases
- `raven_enum_values`
  - curated categorical values
- `raven_value_aliases`
  - aliases for values such as `fy24`, `north zone`, `new admission`
- `raven_is_groupable`
  - whether the column is safe for grouping
- `raven_is_filterable`
  - whether the column is safe for filters

### Relationship Metadata

OpenMetadata should ideally support richer join hints beyond raw lineage.

Recommended fields:

- relationship type
  - `one_to_many`, `many_to_one`, `many_to_many`
- join keys
- join confidence
- approved / discouraged relationship flag
- business description of the join

This is especially valuable when lineage exists but does not fully encode business-safe joins.

## MCP / Tool Contract Expected By RAVEN

These are the OpenMetadata-side tools RAVEN already expects or is designed to consume:

- `semantic_search`
- `search_entities_with_query`
- `get_table_by_fqn`
- `get_table_columns_by_fqn`
- `get_table_profile`
- `get_sample_data`
- `get_list_of_tables`
- `get_lineage`
- `get_column_lineage`
- `create_lineage`
- `search_glossary`
- `get_glossary_terms`
- `create_glossary_term`
- `get_tags`
- `get_classifications`
- `get_test_definitions`
- `create_test_case`
- `get_test_case_results`
- `get_rca`
- `get_domains`
- `get_teams`
- `get_users`
- `get_databases`
- `get_database_schemas`
- `create_knowledge_article`

If the OpenMetadata team is implementing MCP support, these tools should be stable, typed, and permission-aware.

## Accuracy-Critical Behaviors On The OpenMetadata Side

### Search Quality

OpenMetadata search should rank by more than string similarity.

Useful ranking inputs:

- description quality
- glossary linkage
- usage frequency
- domain match
- ownership relevance
- freshness
- quality status
- tags such as `gold`, `deprecated`, `pii`

### Lineage Quality

Lineage should not just exist; it should be trustworthy.

Needed properties:

- accurate table FQNs
- accurate column mappings
- stable directionality
- no missing key edges for high-value fact and dimension tables

### Profile Safety

Sample values are very useful, but must be safe.

Recommended behavior:

- suppress or redact values for PII-tagged columns
- keep safe sample values for categorical business columns
- prefer top-k distinct categorical values over random row dumps

### Domain Coverage

Domains should map to real business scopes.

RAVEN uses domains for focus mode and scoping.
That works only if domains are:

- populated
- not too broad
- aligned with how analytics questions are actually asked

## What OpenMetadata Should Write Back

To make the integration a learning loop instead of a one-way lookup, OpenMetadata should support write-back for:

### 1. Verified Query Knowledge

When a RAVEN query is reviewed and trusted, OpenMetadata should be able to store it as a knowledge article or similar reviewed asset.

Expected payload:

- title
- content
- tags
- provenance

### 2. New Business Terms

When RAVEN discovers a strong new business synonym or concept, OpenMetadata should allow:

- glossary-term creation
- synonym registration
- optional SQL hint or custom properties

### 3. New Relationships

When a join path is reviewed and accepted, OpenMetadata should allow:

- lineage edge creation
- optional business description

### 4. Probe-Derived Quality Tests

When RAVEN finds suspicious value issues during probing, OpenMetadata should allow:

- creating a test case
- attaching the discovered logic and description

## Things That Matter Most For Accuracy

If the OpenMetadata team asks "what gives the biggest return?", the answer is:

1. Good table and column descriptions
2. Good glossary synonyms
3. Reliable column lineage
4. Safe categorical sample values
5. Domain ownership and quality signals
6. Deprecation and freshness signals
7. Reviewed knowledge assets for trusted queries

## Things OpenMetadata Should Not Assume

OpenMetadata should not assume:

- it alone defines business truth
- lineage alone implies a safe analytical join
- raw sample rows are always safe to expose
- tags and descriptions are enough without glossary synonyms
- a table being searchable means it is the right table to answer a metric question

## Recommended Acceptance Criteria For The OpenMetadata Side

OpenMetadata support for RAVEN is in good shape only when:

- top relevant tables appear in semantic search reliably
- top relevant glossary terms are discoverable by business phrasing
- column lineage is available for high-value fact/dimension joins
- profile coverage is high for top business tables
- quality status is queryable at request time
- domain data is populated for major business areas
- write-back calls succeed for glossary, lineage, test cases, and knowledge articles

## Suggested OpenMetadata Backlog, In Order

1. Improve table and column description coverage
2. Add or enrich glossary synonyms and related terms
3. Ensure semantic search ranks business-relevant assets well
4. Ensure column lineage works for major analytical joins
5. Expose safe profile stats and categorical sample values
6. Add domain, ownership, freshness, and deprecation metadata consistently
7. Add Raven-oriented custom properties for semantic roles and defaults
8. Support robust write-back for reviewed knowledge

## Final Rule

If the OpenMetadata-side work does not improve:

- search relevance
- join-path correctness
- value grounding
- or confidence calibration

then it is not helping RAVEN accuracy enough.

The OpenMetadata side should optimize for those four outcomes first.
