"""
OpenMetadata MCP Client
========================
Client for OpenMetadata's built-in MCP server (v1.12+).
Uses Personal Access Token for authentication.

Provides:
- Semantic search (replaces pgvector schema_embeddings)
- Lineage graph (replaces NetworkX dbt graph)
- Glossary terms (supplements RAVEN's semantic_model.yaml)
- Column profiles (replaces content_awareness.json)
- Data quality checks (NEW: warn on failing tests)
- Write-back: test cases, lineage, glossary terms, knowledge articles
- Domain-based focus (NEW: zero-setup scoping)
"""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass, field
from itertools import combinations
from typing import Any

import aiohttp

logger = logging.getLogger(__name__)


# ── Configuration ─────────────────────────────────────────────────────

@dataclass
class OpenMetadataConfig:
    """Configuration for OpenMetadata MCP integration."""
    url: str = ""                          # https://openmetadata.pw.live
    pat: str = ""                          # Personal Access Token

    # Feature flags — granular control over what RAVEN uses from OM
    semantic_search: bool = True           # Use OM's vector search
    lineage_graph: bool = True             # Use OM's lineage
    glossary: bool = True                  # Use OM's glossary
    column_profiles: bool = True           # Use OM's column profiles
    quality_checks: bool = True            # Show quality warnings
    auto_create_test_cases: bool = True    # Auto-create DQ tests from probes
    write_back_lineage: bool = True        # Push discovered relationships
    write_back_glossary: bool = True       # Push new business terms
    domain_focus: bool = True              # Enable domain-based Focus Mode

    # Fallback paths (used when OM is down)
    fallback_enabled: bool = True
    fallback_schema_catalog: str = "data/schema_catalog.json"
    fallback_content_awareness: str = "data/content_awareness.json"
    fallback_table_graph: str = "data/table_graph.gpickle"

    @classmethod
    def from_env(cls) -> OpenMetadataConfig:
        """Load config from environment variables."""
        return cls(
            url=os.getenv("OPENMETADATA_URL", ""),
            pat=os.getenv("OPENMETADATA_PAT", ""),
        )

    @classmethod
    def from_yaml(cls, path: str) -> OpenMetadataConfig:
        """Load config from YAML file with env variable interpolation."""
        import yaml
        if not os.path.exists(path):
            return cls.from_env()
        with open(path) as f:
            raw = yaml.safe_load(f) or {}
        om = raw.get("openmetadata", {})
        features = om.get("features", {})
        fallback = om.get("fallback", {})

        def resolve(val: str) -> str:
            if isinstance(val, str) and val.startswith("${") and val.endswith("}"):
                return os.getenv(val[2:-1], "")
            return val or ""

        return cls(
            url=resolve(om.get("url", "")),
            pat=resolve(om.get("pat", "")),
            semantic_search=features.get("semantic_search", True),
            lineage_graph=features.get("lineage_graph", True),
            glossary=features.get("glossary", True),
            column_profiles=features.get("column_profiles", True),
            quality_checks=features.get("quality_checks", True),
            auto_create_test_cases=features.get("auto_create_test_cases", True),
            write_back_lineage=features.get("write_back_lineage", True),
            write_back_glossary=features.get("write_back_glossary", True),
            domain_focus=features.get("domain_focus", True),
            fallback_enabled=fallback.get("enabled", True),
            fallback_schema_catalog=fallback.get("schema_catalog_path", "data/schema_catalog.json"),
            fallback_content_awareness=fallback.get("content_awareness_path", "data/content_awareness.json"),
            fallback_table_graph=fallback.get("table_graph_path", "data/table_graph.gpickle"),
        )

    @property
    def is_configured(self) -> bool:
        return bool(self.url and self.pat)


# ── MCP Tool Response Normalization ─────────────────────────────────

@dataclass
class OMTableResult:
    """Normalized table from OpenMetadata."""
    fqn: str
    name: str = ""
    description: str = ""
    owner: str = ""
    domain: str = ""
    tags: list[str] = field(default_factory=list)
    columns: list[dict] = field(default_factory=list)
    quality_status: str = "UNKNOWN"
    failing_tests: int = 0
    last_updated: str = ""
    score: float = 0.0  # search relevance

    @classmethod
    def from_om_response(cls, data: dict, score: float = 0.0) -> OMTableResult:
        """Parse OpenMetadata entity response into normalized format."""
        tags = [t.get("tagFQN", "") for t in data.get("tags", [])]
        columns = []
        for col in data.get("columns", []):
            col_tags = [t.get("tagFQN", "") for t in col.get("tags", [])]
            columns.append({
                "name": col.get("name", ""),
                "data_type": col.get("dataType", ""),
                "description": col.get("description", ""),
                "tags": col_tags,
                "is_pii": any("PII" in t for t in col_tags),
                "constraint": col.get("constraint", ""),
                "ordinal_position": col.get("ordinalPosition"),
                "profile": col.get("profile", {}),
            })
        quality = data.get("testSuite", {})
        return cls(
            fqn=data.get("fullyQualifiedName", ""),
            name=data.get("name", ""),
            description=data.get("description", ""),
            owner=data.get("owner", {}).get("name", "unknown") if data.get("owner") else "unknown",
            domain=data.get("domain", {}).get("name", "") if data.get("domain") else "",
            tags=tags,
            columns=columns,
            quality_status="PASS" if quality.get("failed", 0) == 0 else "FAIL",
            failing_tests=quality.get("failed", 0),
            last_updated=data.get("updatedAt", ""),
            score=score,
        )


@dataclass
class OMGlossaryTerm:
    """Normalized glossary term from OpenMetadata."""
    name: str
    fqn: str = ""
    description: str = ""
    synonyms: list[str] = field(default_factory=list)
    related_assets: list[str] = field(default_factory=list)
    sql_fragment: str = ""
    score: float = 0.0

    @classmethod
    def from_om_response(cls, data: dict, score: float = 0.0) -> OMGlossaryTerm:
        return cls(
            name=data.get("name", ""),
            fqn=data.get("fullyQualifiedName", ""),
            description=data.get("description", ""),
            synonyms=data.get("synonyms", []),
            related_assets=[
                a.get("fullyQualifiedName", "")
                for a in data.get("relatedTerms", [])
            ],
            sql_fragment=data.get("customProperties", {}).get("sql_fragment", ""),
            score=score,
        )


@dataclass
class OMLineageEdge:
    """A lineage edge between two entities."""
    from_entity: str
    to_entity: str
    from_columns: list[str] = field(default_factory=list)
    to_columns: list[str] = field(default_factory=list)
    description: str = ""

    @classmethod
    def from_om_response(cls, edge: dict) -> OMLineageEdge:
        col_lineage = edge.get("columnLineage", [])
        from_cols, to_cols = [], []
        for cl in col_lineage:
            from_cols.extend(cl.get("fromColumns", []))
            to_cols.extend(cl.get("toColumns", []))
        return cls(
            from_entity=edge.get("fromEntity", {}).get("fqn", ""),
            to_entity=edge.get("toEntity", {}).get("fqn", ""),
            from_columns=from_cols,
            to_columns=to_cols,
            description=edge.get("description", ""),
        )


# ── Main Client ──────────────────────────────────────────────────────

class OpenMetadataMCPClient:
    """
    Client for OpenMetadata's built-in MCP server.
    Wraps MCP tool calls with typed convenience methods.
    Falls back gracefully when OM is unavailable.
    """

    def __init__(self, config: OpenMetadataConfig | None = None):
        self.config = config or OpenMetadataConfig.from_env()
        self.mcp_url = f"{self.config.url.rstrip('/')}/api/v1/mcp" if self.config.url else ""
        self._available: bool | None = None  # Cached availability check
        self._session: aiohttp.ClientSession | None = None

    @property
    def is_configured(self) -> bool:
        return self.config.is_configured

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create an aiohttp session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers={
                    "Authorization": f"Bearer {self.config.pat}",
                    "Content-Type": "application/json",
                },
                timeout=aiohttp.ClientTimeout(total=30),
            )
        return self._session

    async def close(self):
        """Close the HTTP session."""
        if self._session and not self._session.closed:
            await self._session.close()

    async def check_availability(self) -> bool:
        """Test if OpenMetadata MCP server is reachable."""
        if not self.is_configured:
            self._available = False
            return False
        try:
            session = await self._get_session()
            async with session.get(
                f"{self.config.url.rstrip('/')}/api/v1/system/version"
            ) as resp:
                self._available = resp.status == 200
                if self._available:
                    data = await resp.json()
                    logger.info("OpenMetadata connected: version %s", data.get("version", "?"))
                return self._available
        except Exception as exc:
            logger.warning("OpenMetadata unavailable: %s", exc)
            self._available = False
            return False

    async def is_available(self) -> bool:
        """Check cached availability, re-check if unknown."""
        if self._available is None:
            return await self.check_availability()
        return self._available

    # ── Generic MCP Tool Call ─────────────────────────────────────

    async def call_tool(self, tool_name: str, arguments: dict) -> dict | list | None:
        """Call an MCP tool on the OpenMetadata server."""
        if not self.is_configured:
            logger.debug("OpenMetadata not configured, skipping %s", tool_name)
            return None
        try:
            session = await self._get_session()
            async with session.post(
                f"{self.mcp_url}/tools/{tool_name}",
                json=arguments,
            ) as resp:
                if resp.status == 200:
                    return await resp.json()
                else:
                    text = await resp.text()
                    logger.warning("OM MCP %s returned %d: %s", tool_name, resp.status, text[:200])
                    return None
        except asyncio.TimeoutError:
            logger.warning("OM MCP %s timed out", tool_name)
            self._available = False
            return None
        except Exception as exc:
            logger.warning("OM MCP %s failed: %s", tool_name, exc)
            self._available = False
            return None

    # ── Discovery & Search ────────────────────────────────────────

    async def semantic_search(self, query: str, entity_type: str = "table",
                              limit: int = 20) -> list[OMTableResult]:
        """Vector similarity search using OpenMetadata's embeddings.
        Replaces pgvector schema_embeddings search."""
        if not self.config.semantic_search:
            return []
        result = await self.call_tool("semantic_search", {
            "query": query, "entity_type": entity_type, "limit": limit
        })
        if not result:
            return []
        hits = result if isinstance(result, list) else result.get("hits", result.get("data", []))
        return [
            OMTableResult.from_om_response(h.get("entity", h), score=h.get("score", 0.0))
            for h in hits
        ]

    async def search_entities(self, query: str, entity_type: str = "table",
                              filters: dict | None = None, limit: int = 50) -> list[dict]:
        """Full-text search across OpenMetadata entities."""
        args: dict[str, Any] = {
            "query": query, "entity_type": entity_type, "limit": limit
        }
        if filters:
            args["filters"] = filters
        result = await self.call_tool("search_entities_with_query", args)
        if not result:
            return []
        return result if isinstance(result, list) else result.get("data", [])

    async def get_table_by_fqn(self, fqn: str) -> OMTableResult | None:
        """Get full table details by fully qualified name."""
        result = await self.call_tool("get_table_by_fqn", {"fqn": fqn})
        if not result:
            return None
        return OMTableResult.from_om_response(result)

    async def get_table_columns(self, fqn: str) -> list[dict]:
        """Get column details for a table."""
        result = await self.call_tool("get_table_columns_by_fqn", {"fqn": fqn})
        if not result:
            return []
        return result if isinstance(result, list) else result.get("columns", [])

    async def get_table_profile(self, fqn: str) -> dict | None:
        """Get column-level profiling data (distinct count, null%, min/max).
        Replaces content_awareness.json lookup."""
        if not self.config.column_profiles:
            return None
        result = await self.call_tool("get_table_profile", {"fqn": fqn})
        return result

    async def get_sample_data(self, fqn: str) -> dict | None:
        """Get sample rows from a table."""
        return await self.call_tool("get_sample_data", {"fqn": fqn})

    async def get_list_of_tables(self, limit: int = 100,
                                 offset: int = 0) -> list[dict]:
        """Paginated table list with basic info."""
        result = await self.call_tool("get_list_of_tables", {
            "limit": limit, "offset": offset
        })
        if not result:
            return []
        return result if isinstance(result, list) else result.get("data", [])

    # ── Lineage ───────────────────────────────────────────────────

    async def get_lineage(self, entity_type: str, fqn: str,
                          upstream_depth: int = 3,
                          downstream_depth: int = 3) -> dict:
        """Get lineage graph for an entity. Replaces NetworkX dbt graph."""
        if not self.config.lineage_graph:
            return {"nodes": [], "edges": []}
        result = await self.call_tool("get_lineage", {
            "entity_type": entity_type, "fqn": fqn,
            "upstream_depth": upstream_depth,
            "downstream_depth": downstream_depth,
        })
        return result or {"nodes": [], "edges": []}

    async def get_column_lineage(self, from_fqn: str,
                                 to_fqn: str) -> list[dict]:
        """Get column-level lineage between two tables."""
        if not self.config.lineage_graph:
            return []
        result = await self.call_tool("get_column_lineage", {
            "from_fqn": from_fqn, "to_fqn": to_fqn
        })
        if not result:
            return []
        return result if isinstance(result, list) else result.get("columnLineage", [])

    async def create_lineage(self, from_entity: str, to_entity: str,
                             description: str = "") -> dict | None:
        """Create a new lineage edge. Write-back from RAVEN discoveries."""
        if not self.config.write_back_lineage:
            return None
        return await self.call_tool("create_lineage", {
            "from_entity": from_entity,
            "to_entity": to_entity,
            "description": description,
        })

    # ── Glossary & Classification ─────────────────────────────────

    async def search_glossary(self, query: str, limit: int = 10) -> list[OMGlossaryTerm]:
        """Search glossary terms. Supplements RAVEN's semantic_model.yaml."""
        if not self.config.glossary:
            return []
        result = await self.call_tool("search_glossary", {
            "query": query, "limit": limit
        })
        if not result:
            return []
        items = result if isinstance(result, list) else result.get("data", [])
        return [
            OMGlossaryTerm.from_om_response(item, score=item.get("score", 0.0))
            for item in items
        ]

    async def get_glossary_terms(self, glossary: str | None = None) -> list[OMGlossaryTerm]:
        """Retrieve all terms from a glossary."""
        if not self.config.glossary:
            return []
        args = {}
        if glossary:
            args["glossary"] = glossary
        result = await self.call_tool("get_glossary_terms", args)
        if not result:
            return []
        items = result if isinstance(result, list) else result.get("data", [])
        return [OMGlossaryTerm.from_om_response(item) for item in items]

    async def create_glossary_term(self, glossary: str, name: str,
                                   description: str, synonyms: list[str] | None = None,
                                   related_terms: list[str] | None = None,
                                   sql_fragment: str = "") -> dict | None:
        """Push a new business term to OpenMetadata glossary."""
        if not self.config.write_back_glossary:
            return None
        args: dict[str, Any] = {
            "glossary": glossary,
            "name": name,
            "description": description,
        }
        if synonyms:
            args["synonyms"] = synonyms
        if related_terms:
            args["related_terms"] = related_terms
        if sql_fragment:
            args["custom_properties"] = {"sql_fragment": sql_fragment}
        return await self.call_tool("create_glossary_term", args)

    async def get_tags(self) -> list[dict]:
        """Retrieve classification tags."""
        result = await self.call_tool("get_tags", {})
        if not result:
            return []
        return result if isinstance(result, list) else result.get("data", [])

    async def get_classifications(self) -> list[dict]:
        """Get classification hierarchy."""
        result = await self.call_tool("get_classifications", {})
        if not result:
            return []
        return result if isinstance(result, list) else result.get("data", [])

    # ── Data Quality ──────────────────────────────────────────────

    async def get_test_definitions(self) -> list[dict]:
        """List available data quality test types."""
        result = await self.call_tool("get_test_definitions", {})
        if not result:
            return []
        return result if isinstance(result, list) else result.get("data", [])

    async def create_test_case(self, table_fqn: str, column: str,
                               test_definition: str, parameters: dict,
                               description: str = "") -> dict | None:
        """Create a data quality test case. Auto-generated from probe discoveries."""
        if not self.config.auto_create_test_cases:
            return None
        return await self.call_tool("create_test_case", {
            "table_fqn": table_fqn,
            "column": column,
            "test_definition": test_definition,
            "parameters": parameters,
            "description": description,
        })

    async def get_test_case_results(self, table_fqn: str) -> list[dict]:
        """Get data quality test results for a table."""
        if not self.config.quality_checks:
            return []
        result = await self.call_tool("get_test_case_results", {
            "table_fqn": table_fqn
        })
        if not result:
            return []
        return result if isinstance(result, list) else result.get("data", [])

    async def get_rca(self, test_case_id: str) -> dict | None:
        """Root Cause Analysis for a failing test."""
        return await self.call_tool("get_rca", {"test_case_id": test_case_id})

    # ── Domains & Teams ───────────────────────────────────────────

    async def get_domains(self) -> list[dict]:
        """List data domains. Used for domain-based Focus Mode."""
        if not self.config.domain_focus:
            return []
        result = await self.call_tool("get_domains", {})
        if not result:
            return []
        return result if isinstance(result, list) else result.get("data", [])

    async def get_teams(self) -> list[dict]:
        """List teams and members."""
        result = await self.call_tool("get_teams", {})
        if not result:
            return []
        return result if isinstance(result, list) else result.get("data", [])

    async def get_users(self) -> list[dict]:
        """List users."""
        result = await self.call_tool("get_users", {})
        if not result:
            return []
        return result if isinstance(result, list) else result.get("data", [])

    # ── Schema & Database ─────────────────────────────────────────

    async def get_databases(self) -> list[dict]:
        """List databases."""
        result = await self.call_tool("get_databases", {})
        if not result:
            return []
        return result if isinstance(result, list) else result.get("data", [])

    async def get_database_schemas(self, database_fqn: str) -> list[dict]:
        """List schemas in a database."""
        result = await self.call_tool("get_database_schemas", {
            "database_fqn": database_fqn
        })
        if not result:
            return []
        return result if isinstance(result, list) else result.get("data", [])

    # ── Knowledge Center ──────────────────────────────────────────

    async def create_knowledge_article(self, title: str, content: str,
                                       tags: list[str] | None = None) -> dict | None:
        """Push a verified query to OpenMetadata Knowledge Center."""
        args: dict[str, Any] = {"title": title, "content": content}
        if tags:
            args["tags"] = tags
        return await self.call_tool("create_knowledge_article", args)

    # ── High-Level Composite Methods ──────────────────────────────

    async def find_bridge_tables(self, candidate_tables: list[str]) -> set[str]:
        """Use lineage to discover bridge/junction tables between candidates.
        Replaces NetworkX graph traversal."""
        if not self.config.lineage_graph or not await self.is_available():
            return set()

        full_table_set = set(candidate_tables)

        # Get lineage for each candidate (parallel)
        lineage_results = await asyncio.gather(*[
            self.get_lineage("table", fqn, upstream_depth=2, downstream_depth=2)
            for fqn in candidate_tables
        ], return_exceptions=True)

        for lineage in lineage_results:
            if isinstance(lineage, Exception) or not lineage:
                continue
            for edge in lineage.get("edges", []):
                from_fqn = edge.get("fromEntity", {}).get("fqn", "")
                to_fqn = edge.get("toEntity", {}).get("fqn", "")
                if from_fqn in candidate_tables or to_fqn in candidate_tables:
                    if from_fqn:
                        full_table_set.add(from_fqn)
                    if to_fqn:
                        full_table_set.add(to_fqn)

        # Check column-level lineage between pairs (parallel, limited)
        pairs = list(combinations(candidate_tables, 2))[:10]  # limit combos
        if pairs:
            col_results = await asyncio.gather(*[
                self.get_column_lineage(t1, t2) for t1, t2 in pairs
            ], return_exceptions=True)
            for col_lineage in col_results:
                if isinstance(col_lineage, Exception) or not col_lineage:
                    continue
                for mapping in col_lineage:
                    ft = mapping.get("from_table", "")
                    tt = mapping.get("to_table", "")
                    if ft:
                        full_table_set.add(ft)
                    if tt:
                        full_table_set.add(tt)

        return full_table_set

    async def get_quality_warnings(self, table_fqns: list[str]) -> list[dict]:
        """Check quality status for a list of tables.
        Returns warnings for tables with failing tests."""
        if not self.config.quality_checks or not await self.is_available():
            return []

        results = await asyncio.gather(*[
            self.get_test_case_results(fqn) for fqn in table_fqns
        ], return_exceptions=True)

        warnings = []
        for fqn, result in zip(table_fqns, results):
            if isinstance(result, Exception) or not result:
                continue
            failing = [r for r in result if r.get("status") == "Failed"]
            if failing:
                warnings.append({
                    "table": fqn,
                    "failing_tests": len(failing),
                    "details": [
                        {
                            "test": r.get("testDefinition", {}).get("name", ""),
                            "column": r.get("entityLink", ""),
                            "message": r.get("message", ""),
                        }
                        for r in failing[:3]  # top 3 failures
                    ],
                })
        return warnings

    async def get_tables_with_profiles(self, fqns: list[str]) -> list[dict]:
        """Get table profiles for multiple tables in parallel.
        Replaces content_awareness.json lookups."""
        if not self.config.column_profiles or not await self.is_available():
            return []

        results = await asyncio.gather(*[
            self.get_table_profile(fqn) for fqn in fqns[:15]  # limit
        ], return_exceptions=True)

        profiles = []
        for fqn, result in zip(fqns, results):
            if isinstance(result, Exception) or not result:
                continue
            profiles.append({"table": fqn, "profile": result})
        return profiles

    async def probe_and_report(self, table_fqn: str,
                               probe_results: list[dict],
                               question: str = "") -> list[dict]:
        """Analyze probe results and auto-create DQ test cases in OpenMetadata.
        Returns list of created test case IDs."""
        if not self.config.auto_create_test_cases or not await self.is_available():
            return []

        created = []
        for probe in probe_results:
            if not probe.get("success"):
                continue

            result_text = probe.get("result", "")
            probe_type = probe.get("type", "")

            # Detect high NULL rate
            if "null" in result_text.lower() and "%" in result_text:
                # Try to extract column and percentage
                tc = await self.create_test_case(
                    table_fqn=table_fqn,
                    column=probe.get("column", ""),
                    test_definition="columnValuesToNotBeNull",
                    parameters={"threshold": 20},
                    description=f"RAVEN discovered high NULL rate while answering: '{question}'"
                )
                if tc:
                    created.append(tc)

            # Detect unexpected enum values
            if probe_type == "distinct_values" and probe.get("unexpected_values"):
                tc = await self.create_test_case(
                    table_fqn=table_fqn,
                    column=probe.get("column", ""),
                    test_definition="columnValuesToBeInSet",
                    parameters={"allowedValues": probe.get("expected_values", [])},
                    description=f"RAVEN found unexpected values: {probe.get('unexpected_values')}"
                )
                if tc:
                    created.append(tc)

        return created

    async def on_thumbs_up(self, question: str, sql: str,
                           tables: list[str]) -> dict | None:
        """Push verified query to OpenMetadata Knowledge Center."""
        return await self.create_knowledge_article(
            title=f"RAVEN Verified: {question}",
            content=f"Question: {question}\nSQL: {sql}\nTables: {', '.join(tables)}",
            tags=["raven-verified", "text-to-sql"],
        )

    async def on_relationship_discovered(self, from_table: str, to_table: str,
                                         join_column: str) -> dict | None:
        """Push discovered table relationship as lineage edge."""
        return await self.create_lineage(
            from_entity=from_table,
            to_entity=to_table,
            description=f"Discovered by RAVEN from query pattern. JOIN on {join_column}",
        )

    async def focus_from_domain(self, domain_name: str) -> dict:
        """Build Focus context from an OpenMetadata domain.
        Zero-setup Focus Mode for mapped domains."""
        if not self.config.domain_focus or not await self.is_available():
            return {"tables": [], "glossary_terms": []}

        # Search for tables in this domain
        tables = await self.search_entities(
            query=f"domain:{domain_name}",
            entity_type="table",
            limit=100,
        )

        # Search for glossary terms tagged to this domain
        glossary = await self.search_glossary(query=domain_name, limit=20)

        return {
            "domain": domain_name,
            "tables": [t.get("fullyQualifiedName", "") for t in tables],
            "glossary_terms": [
                {"term": g.name, "definition": g.description, "sql_fragment": g.sql_fragment}
                for g in glossary
            ],
            "table_count": len(tables),
        }
