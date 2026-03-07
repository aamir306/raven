"""
Accuracy-first semantic asset retrieval for RAVEN.

Loads the semantic model YAML and exposes deterministic retrieval helpers for:
- verified query matching
- table hinting from synonyms / metrics / dimensions
- business-rule / instruction matching
- Metabase evidence extraction from focus context

This gives the pipeline a trusted asset path before it falls back to free-form
LLM generation.
"""

from __future__ import annotations

from dataclasses import dataclass
import logging
import os
from pathlib import Path
import re
from typing import Any

from .contracts import (
    ContractRegistry,
    SemanticContractValidator,
)
from .query_families import QueryFamilyMatcher

logger = logging.getLogger(__name__)

_TOKEN_RE = re.compile(r"[a-z0-9_]+")
_TABLE_RE = re.compile(r"(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_.]*)", re.IGNORECASE)
_STOPWORDS = {
    "a", "an", "and", "are", "as", "at", "be", "by", "for", "from", "get",
    "give", "how", "i", "in", "is", "it", "list", "me", "of", "on", "or",
    "show", "that", "the", "this", "to", "total", "what", "with",
}


@dataclass(frozen=True)
class VerifiedQueryAsset:
    question: str
    sql: str
    notes: str
    category: str
    tables_used: tuple[str, ...]
    source: str
    tokens: frozenset[str]
    metadata: dict[str, Any]


@dataclass(frozen=True)
class RuleAsset:
    term: str
    definition: str
    sql_fragment: str
    synonyms: tuple[str, ...]
    categories: tuple[str, ...]
    rule_type: str
    tokens: frozenset[str]


@dataclass(frozen=True)
class TableAsset:
    name: str
    description: str
    synonyms: tuple[str, ...]
    metrics: tuple[dict[str, Any], ...]
    dimensions: tuple[dict[str, Any], ...]
    time_dimensions: tuple[dict[str, Any], ...]
    tokens: frozenset[str]


@dataclass(frozen=True)
class RelationshipAsset:
    left_table: str
    right_table: str
    left_column: str
    right_column: str
    cast_required: bool = False
    cast_type: str = ""
    notes: str = ""
    source: str = "semantic_model"


_GENERIC_ANALYTICS_KEYWORDS = {
    "analytics",
    "average",
    "breakdown",
    "count",
    "daily",
    "dimension",
    "group",
    "growth",
    "kpi",
    "max",
    "metric",
    "min",
    "monthly",
    "quarterly",
    "rank",
    "report",
    "sum",
    "table",
    "top",
    "total",
    "trend",
    "weekly",
    "yearly",
}


def _default_model_path() -> Path:
    domain_pack = os.getenv("RAVEN_DOMAIN_PACK_PATH")
    if domain_pack:
        return Path(domain_pack).expanduser()
    configured = os.getenv("RAVEN_SEMANTIC_MODEL_PATH")
    if configured:
        return Path(configured).expanduser()
    root = Path(__file__).resolve().parents[2]
    pack_dir = root / "config" / "semantic"
    if pack_dir.exists():
        return pack_dir
    primary = root / "config" / "semantic_model.yaml"
    if primary.exists():
        return primary
    return root / "config" / "semantic_model.example.yaml"


def _normalize(text: str) -> str:
    return " ".join(text.lower().split())


def _tokenize(text: str) -> set[str]:
    return {
        token
        for token in _TOKEN_RE.findall(text.lower())
        if len(token) > 1 and token not in _STOPWORDS
    }


def _extract_tables(sql: str) -> tuple[str, ...]:
    tables = list(dict.fromkeys(_TABLE_RE.findall(sql or "")))
    return tuple(tables)


def _score_overlap(query_tokens: set[str], candidate_tokens: set[str]) -> float:
    if not query_tokens or not candidate_tokens:
        return 0.0
    overlap = query_tokens & candidate_tokens
    if not overlap:
        return 0.0
    return len(overlap) / len(query_tokens)


def _infer_rule_type(sql_fragment: str) -> str:
    fragment = (sql_fragment or "").strip().lower()
    if not fragment:
        return "definition"
    if "order by" in fragment and "limit" in fragment:
        return "ranking"
    if any(op in fragment for op in (" = ", " ='", ">=", "<=", " !=", " in ", " like ")):
        return "filter"
    if re.search(r"(?:^|[^a-z])(sum|avg|count|min|max|approx_percentile)\s*\(", fragment):
        return "metric_formula"
    if any(token in fragment for token in ("date_trunc(", "extract(", "date(")):
        return "grain"
    return "sql_hint"


class SemanticModelStore:
    """Deterministic access layer over the semantic model."""

    def __init__(self, path: str | Path | None = None):
        self.path = Path(path) if path else _default_model_path()
        self._model: dict[str, Any] = {}
        self._tables: list[TableAsset] = []
        self._verified_queries: list[VerifiedQueryAsset] = []
        self._rules: list[RuleAsset] = []
        self._relationships: list[RelationshipAsset] = []
        self._table_alias_lookup: dict[str, str] = {}
        self._validator = SemanticContractValidator()
        self._query_family_matcher = QueryFamilyMatcher()
        self.reload()

    @property
    def table_assets(self) -> tuple[TableAsset, ...]:
        return tuple(self._tables)

    @property
    def rule_assets(self) -> tuple[RuleAsset, ...]:
        return tuple(self._rules)

    @property
    def relationship_assets(self) -> tuple[RelationshipAsset, ...]:
        return tuple(self._relationships)

    def get_table_asset(self, table_name: str) -> TableAsset | None:
        resolved = self.resolve_table_name(table_name)
        for asset in self._tables:
            if asset.name == resolved:
                return asset
        return None

    @staticmethod
    def table_aliases(table_name: str) -> tuple[str, ...]:
        raw = str(table_name or "").strip()
        if not raw:
            return tuple()

        parts = raw.split(".")
        aliases = [raw]
        if len(parts) >= 2:
            aliases.append(".".join(parts[-2:]))
        aliases.append(parts[-1])
        return tuple(dict.fromkeys(alias for alias in aliases if alias))

    def resolve_table_name(
        self,
        table_name: str,
        candidates: set[str] | None = None,
    ) -> str:
        aliases = self.table_aliases(table_name)
        if candidates:
            candidate_lookup: dict[str, str] = {}
            for candidate in candidates:
                for alias in self.table_aliases(candidate):
                    candidate_lookup.setdefault(alias, candidate)
            for alias in aliases:
                if alias in candidate_lookup:
                    return candidate_lookup[alias]

        for alias in aliases:
            if alias in self._table_alias_lookup:
                return self._table_alias_lookup[alias]
        return aliases[0] if aliases else str(table_name)

    def data_keywords(self) -> set[str]:
        keywords = set(_GENERIC_ANALYTICS_KEYWORDS)
        for table in self._tables:
            keywords.update(_tokenize(table.name))
            keywords.update(_tokenize(table.description))
            for synonym in table.synonyms:
                keywords.update(_tokenize(synonym))
            for metric in table.metrics:
                keywords.update(_tokenize(metric.get("name", "")))
                keywords.update(_tokenize(metric.get("description", "")))
            for dimension in (*table.dimensions, *table.time_dimensions):
                keywords.update(_tokenize(dimension.get("name", "")))
                keywords.update(_tokenize(dimension.get("description", "")))

        for rule in self._rules:
            keywords.update(_tokenize(rule.term))
            keywords.update(_tokenize(rule.definition))
            for synonym in rule.synonyms:
                keywords.update(_tokenize(synonym))

        return {
            keyword
            for keyword in keywords
            if len(keyword) >= 3 and keyword not in _STOPWORDS
        }

    def keyword_pattern(self) -> re.Pattern[str]:
        escaped = [re.escape(keyword) for keyword in sorted(self.data_keywords(), key=len, reverse=True)]
        if not escaped:
            return re.compile(r"$^")
        return re.compile(r"\b(" + "|".join(escaped) + r")\b", re.IGNORECASE)

    def reload(self) -> None:
        if not self.path.exists():
            logger.warning("Semantic model not found at %s", self.path)
            self._model = {}
            self._tables = []
            self._verified_queries = []
            self._rules = []
            self._relationships = []
            self._table_alias_lookup = {}
            return

        bundle = ContractRegistry(self.path).load()
        report = self._validator.validate(bundle)
        for warning in report.warnings:
            logger.warning("Semantic contract warning: %s", warning)
        report.raise_for_errors()
        self._model = bundle.to_dict()

        self._tables = []
        self._table_alias_lookup = {}
        for raw in self._model.get("tables", []):
            name = raw.get("name", "")
            desc = raw.get("description", "")
            synonyms = tuple(raw.get("synonyms", []) or [])
            dimensions = tuple(raw.get("dimensions", []) or [])
            time_dimensions = tuple(raw.get("time_dimensions", []) or [])
            metrics = tuple(raw.get("metrics", []) or [])

            token_parts: list[str] = [name, desc, " ".join(synonyms)]
            token_parts.extend(d.get("name", "") for d in dimensions)
            token_parts.extend(d.get("description", "") for d in dimensions)
            token_parts.extend(d.get("name", "") for d in time_dimensions)
            token_parts.extend(d.get("description", "") for d in time_dimensions)
            token_parts.extend(m.get("name", "") for m in metrics)
            token_parts.extend(m.get("description", "") for m in metrics)

            self._tables.append(
                TableAsset(
                    name=name,
                    description=desc,
                    synonyms=synonyms,
                    metrics=metrics,
                    dimensions=dimensions,
                    time_dimensions=time_dimensions,
                    tokens=frozenset(_tokenize(" ".join(token_parts))),
                )
            )
            for alias in self.table_aliases(name):
                self._table_alias_lookup.setdefault(alias, name)

        self._rules = []
        for raw in self._model.get("business_rules", []):
            term = str(raw.get("term", ""))
            definition = str(raw.get("definition", ""))
            sql_fragment = str(raw.get("sql_fragment", ""))
            synonyms = tuple(raw.get("synonyms", []) or [])
            categories = tuple(raw.get("category", []) or [])
            self._rules.append(
                RuleAsset(
                    term=term,
                    definition=definition,
                    sql_fragment=sql_fragment,
                    synonyms=synonyms,
                    categories=categories,
                    rule_type=_infer_rule_type(sql_fragment),
                    tokens=frozenset(
                        _tokenize(
                            " ".join(
                                [term, definition, sql_fragment, " ".join(synonyms), " ".join(categories)]
                            )
                        )
                    ),
                )
            )

        self._verified_queries = []
        for raw in self._model.get("verified_queries", []):
            question = raw.get("question", "")
            sql = raw.get("sql", "")
            notes = raw.get("notes", "")
            category = str(raw.get("category", ""))
            self._verified_queries.append(
                VerifiedQueryAsset(
                    question=question,
                    sql=sql,
                    notes=notes,
                    category=category,
                    tables_used=_extract_tables(sql),
                    source="semantic_model",
                    tokens=frozenset(
                        _tokenize(
                            " ".join(
                                [
                                    question,
                                    notes,
                                    category,
                                    " ".join(_extract_tables(sql)),
                                ]
                            )
                        )
                    ),
                    metadata={},
                )
            )

        self._relationships = []
        for raw in self._model.get("relationships", []):
            join_columns = raw.get("join_columns", {}) or {}
            left_table = str(raw.get("left_table", ""))
            right_table = str(raw.get("right_table", ""))
            left_column = str(join_columns.get("left", ""))
            right_column = str(join_columns.get("right", ""))
            if not (left_table and right_table and left_column and right_column):
                continue
            self._relationships.append(
                RelationshipAsset(
                    left_table=left_table,
                    right_table=right_table,
                    left_column=left_column,
                    right_column=right_column,
                    cast_required=bool(raw.get("cast_required", False)),
                    cast_type=str(raw.get("cast_type", "")),
                    notes=str(raw.get("notes", "")),
                )
            )

        logger.info(
            "Semantic assets loaded: %d tables, %d verified queries, %d rules, %d relationships",
            len(self._tables),
            len(self._verified_queries),
            len(self._rules),
            len(self._relationships),
        )

    def retrieve(self, question: str, focus: Any | None = None) -> dict[str, Any]:
        """Return ranked semantic hints for the question."""
        query_norm = _normalize(question)
        query_tokens = _tokenize(question)

        focus_tables = list(getattr(focus, "tables", []) or [])
        focus_verified = list(getattr(focus, "verified_queries", []) or [])
        focus_rules = list(getattr(focus, "business_rules", []) or [])
        focus_cards = list(getattr(focus, "dashboard_cards", []) or [])
        focus_filters = list(getattr(focus, "dashboard_filters", []) or [])
        focus_type = str(getattr(focus, "type", "") or "")
        focus_name = str(getattr(focus, "name", "") or "")

        metabase_evidence = self._metabase_evidence(
            question=question,
            query_tokens=query_tokens,
            cards=focus_cards,
            filters=focus_filters,
            focus_name=focus_name,
        )
        ranked_queries = self._search_verified_queries(
            query_norm=query_norm,
            query_tokens=query_tokens,
            focus_tables=focus_tables,
            focus_verified=focus_verified,
            focus_type=focus_type,
            focus_name=focus_name,
        )
        ranked_tables = self._search_tables(
            query_norm=query_norm,
            query_tokens=query_tokens,
            focus_tables=focus_tables,
        )
        glossary_matches = self._search_glossary(
            query_tokens=query_tokens,
            ranked_tables=ranked_tables,
        )
        instruction_matches = self._search_rules(query_norm=query_norm, query_tokens=query_tokens)
        doc_snippets = self._focus_rule_snippets(focus_rules)
        doc_snippets.extend(self._instruction_snippets(instruction_matches))
        doc_snippets.extend(self._metabase_snippets(metabase_evidence))

        trusted_match = next(
            (q for q in ranked_queries if q.get("exact_match")),
            None,
        )
        family_match = self.match_query_family(
            question=question,
            verified_queries=ranked_queries,
            metabase_evidence=metabase_evidence,
            glossary_matches=glossary_matches,
        )

        preferred_tables: list[str] = []
        preferred_tables.extend(focus_tables)
        if trusted_match:
            preferred_tables.extend(trusted_match.get("tables_used", []))
        elif family_match:
            preferred_tables.extend(family_match.get("tables_used", []))
        for item in ranked_queries[:3]:
            preferred_tables.extend(item.get("tables_used", []))
        for item in ranked_tables[:8]:
            preferred_tables.append(item["table"])
        for item in metabase_evidence[:5]:
            preferred_tables.extend(item.get("tables", []))

        return {
            "trusted_query": trusted_match,
            "query_family_match": family_match,
            "verified_queries": ranked_queries[:5],
            "preferred_tables": list(dict.fromkeys(t for t in preferred_tables if t)),
            "glossary_matches": glossary_matches[:10],
            "instruction_matches": instruction_matches[:10],
            "metabase_evidence": metabase_evidence[:6],
            "doc_snippets": doc_snippets[:10],
        }

    def match_query_family(
        self,
        *,
        question: str,
        verified_queries: list[dict[str, Any]],
        metabase_evidence: list[dict[str, Any]],
        resolved_filters: list[dict[str, Any]] | None = None,
        glossary_matches: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any] | None:
        requested_metric = self._requested_metric_context(
            question=question,
            glossary_matches=glossary_matches or [],
            candidate_assets=[*verified_queries, *metabase_evidence],
        )
        requested_dimension = self._requested_dimension_context(
            question=question,
            glossary_matches=glossary_matches or [],
            candidate_assets=[*verified_queries, *metabase_evidence],
        )
        question_metric_phrases = (
            list(requested_metric.get("phrases", []))
            if requested_metric
            else []
        )
        question_dimension_phrases = (
            list(requested_dimension.get("phrases", []))
            if requested_dimension
            else []
        )
        enriched_verified = [
            self._enrich_family_asset_dimension(
                self._enrich_family_asset_metric(item, requested_metric),
                requested_dimension,
            )
            for item in verified_queries
        ]
        enriched_metabase = [
            self._enrich_family_asset_dimension(
                self._enrich_family_asset_metric(item, requested_metric),
                requested_dimension,
            )
            for item in metabase_evidence
        ]
        return self._query_family_matcher.match(
            question=question,
            verified_queries=enriched_verified,
            metabase_evidence=enriched_metabase,
            resolved_filters=resolved_filters,
            question_dimension_phrases=question_dimension_phrases,
            question_metric_phrases=question_metric_phrases,
        )

    def _requested_metric_context(
        self,
        *,
        question: str,
        glossary_matches: list[dict[str, Any]],
        candidate_assets: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        candidate_tables = {
            table
            for asset in candidate_assets
            for table in (asset.get("tables_used", []) or asset.get("tables", []) or [])
            if table
        }
        question_lower = str(question or "").lower()
        ranked: list[tuple[float, dict[str, Any]]] = []
        for item in glossary_matches:
            if item.get("kind") != "metric":
                continue
            table = str(item.get("table", ""))
            metric_name = str(item.get("field_name", ""))
            if not metric_name:
                continue
            if candidate_tables and table and table not in candidate_tables:
                continue
            metric_def = self._table_metric(table, metric_name)
            phrases = self._metric_phrases(
                term=item.get("term", metric_name),
                definition=item.get("definition", ""),
                synonyms=metric_def.get("synonyms", []) if metric_def else [],
            )
            score = float(item.get("similarity", 0.0))
            if any(phrase and phrase in question_lower for phrase in phrases):
                score += 0.15
            ranked.append(
                (
                    score,
                    {
                        "table": table,
                        "name": metric_name,
                        "sql": str(item.get("sql_fragment", "")),
                        "phrases": phrases,
                    },
                )
            )

        if not ranked:
            return None
        ranked.sort(key=lambda item: item[0], reverse=True)
        best_score, best = ranked[0]
        if best_score < 0.30:
            return None
        return best

    def _requested_dimension_context(
        self,
        *,
        question: str,
        glossary_matches: list[dict[str, Any]],
        candidate_assets: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        candidate_tables = {
            table
            for asset in candidate_assets
            for table in (asset.get("tables_used", []) or asset.get("tables", []) or [])
            if table
        }
        question_lower = str(question or "").lower()
        ranked: list[tuple[float, dict[str, Any]]] = []
        for item in glossary_matches:
            if item.get("kind") != "dimension":
                continue
            table = str(item.get("table", ""))
            column = str(item.get("field_name", ""))
            if not column:
                continue
            if (
                candidate_tables
                and table
                and table not in candidate_tables
                and not self._table_joinable_to_any(table, candidate_tables)
            ):
                continue
            dimension_def = self._table_dimension(table, column)
            phrases = self._dimension_phrases(
                term=item.get("term", column),
                definition=item.get("definition", ""),
                synonyms=dimension_def.get("synonyms", []) if dimension_def else [],
            )
            score = float(item.get("similarity", 0.0))
            if any(phrase and phrase in question_lower for phrase in phrases):
                score += 0.15
            ranked.append(
                (
                    score,
                    {
                        "table": table,
                        "column": column,
                        "phrases": phrases,
                    },
                )
            )

        if not ranked:
            return None
        ranked.sort(key=lambda item: item[0], reverse=True)
        best_score, best = ranked[0]
        if best_score < 0.30:
            return None
        return best

    def _enrich_family_asset_metric(
        self,
        asset: dict[str, Any],
        requested_metric: dict[str, Any] | None,
    ) -> dict[str, Any]:
        if not requested_metric:
            return dict(asset)

        sql = str(asset.get("sql", ""))
        tables = list(asset.get("tables_used", []) or asset.get("tables", []) or [])
        candidate = self._asset_metric_context(
            question=str(asset.get("question", "")),
            sql=sql,
            tables=tables,
            requested_metric=requested_metric,
        )
        if not candidate:
            return dict(asset)

        enriched = dict(asset)
        enriched["metric_question_phrases"] = list(candidate.get("phrases", []))

        if (
            candidate.get("table") == requested_metric.get("table")
            and candidate.get("name") != requested_metric.get("name")
        ):
            enriched["metric_replacements"] = [
                {
                    "old_metric_sql": candidate.get("sql", ""),
                    "new_metric_sql": requested_metric.get("sql", ""),
                    "old_metric_name": candidate.get("name", ""),
                    "new_metric_name": requested_metric.get("name", ""),
                }
            ]

        return enriched

    def _table_joinable_to_any(self, table: str, candidate_tables: set[str]) -> bool:
        if not table or not candidate_tables:
            return False
        resolved = self.resolve_table_name(table)
        candidate_resolved = {self.resolve_table_name(item) for item in candidate_tables if item}
        for relationship in self._relationships:
            left = self.resolve_table_name(relationship.left_table)
            right = self.resolve_table_name(relationship.right_table)
            if resolved == left and right in candidate_resolved:
                return True
            if resolved == right and left in candidate_resolved:
                return True
        return False

    def _enrich_family_asset_dimension(
        self,
        asset: dict[str, Any],
        requested_dimension: dict[str, Any] | None,
    ) -> dict[str, Any]:
        if not requested_dimension:
            return dict(asset)

        sql = str(asset.get("sql", ""))
        tables = list(asset.get("tables_used", []) or asset.get("tables", []) or [])
        candidate = self._asset_dimension_context(
            question=str(asset.get("question", "")),
            sql=sql,
            tables=tables,
            requested_dimension=requested_dimension,
        )
        if not candidate:
            return dict(asset)

        enriched = dict(asset)
        enriched["dimension_question_phrases"] = list(candidate.get("phrases", []))

        if (
            candidate.get("table") == requested_dimension.get("table")
            and candidate.get("column") != requested_dimension.get("column")
        ):
            old_refs = list(candidate.get("present_refs", []))
            if old_refs:
                ref_map: dict[str, str] = {}
                for old_ref in old_refs:
                    if "." in old_ref:
                        prefix = old_ref.rsplit(".", 1)[0]
                        ref_map[old_ref] = f"{prefix}.{requested_dimension['column']}"
                    else:
                        ref_map[old_ref] = str(requested_dimension["column"])
                if ref_map:
                    enriched["dimension_replacements"] = [{"ref_map": ref_map}]
        elif candidate.get("table") != requested_dimension.get("table"):
            join_swap = self._join_swap_context(
                root_table=tables[0] if tables else "",
                current_dimension=candidate,
                requested_dimension=requested_dimension,
                available_tables=tables,
            )
            if join_swap:
                enriched["join_replacements"] = [
                    {
                        "old_join_sql": join_swap["old_join_sql"],
                        "new_join_sql": join_swap["new_join_sql"],
                    }
                ]
                ref_map = {
                    old_ref: new_ref
                    for old_ref, new_ref in zip(
                        candidate.get("present_refs", []),
                        join_swap.get("new_dimension_refs", []),
                    )
                    if old_ref and new_ref
                }
                if ref_map:
                    enriched["dimension_replacements"] = [{"ref_map": ref_map}]

        return enriched

    def _asset_dimension_context(
        self,
        *,
        question: str,
        sql: str,
        tables: list[str],
        requested_dimension: dict[str, Any],
    ) -> dict[str, Any] | None:
        sql_lower = str(sql or "").lower()
        question_lower = str(question or "").lower()
        ranked: list[tuple[float, dict[str, Any]]] = []

        for table in tables:
            asset = self.get_table_asset(table)
            if not asset:
                continue
            for dimension in asset.dimensions:
                column = str(dimension.get("name", ""))
                if not column:
                    continue
                aliases = self._column_aliases(table, column)
                present_refs = [
                    alias
                    for alias in aliases
                    if re.search(rf"\b{re.escape(alias.lower())}\b", sql_lower)
                ]
                if not present_refs:
                    continue
                if (
                    table == requested_dimension.get("table")
                    and column != requested_dimension.get("column")
                    and self._sql_uses_column_as_filter(sql, aliases)
                ):
                    continue

                phrases = self._dimension_phrases(
                    term=dimension.get("name", ""),
                    definition=dimension.get("description", ""),
                    synonyms=dimension.get("synonyms", []) or [],
                )
                score = 0.10 * len(present_refs)
                if any(phrase and phrase in question_lower for phrase in phrases):
                    score += 0.30
                if table == requested_dimension.get("table"):
                    score += 0.10
                ranked.append(
                    (
                        score,
                        {
                            "table": table,
                            "column": column,
                            "phrases": phrases,
                            "present_refs": present_refs,
                        },
                    )
                )

        if not ranked:
            return None
        ranked.sort(key=lambda item: item[0], reverse=True)
        best_score, best = ranked[0]
        if best_score < 0.20:
            return None
        return best

    def _asset_metric_context(
        self,
        *,
        question: str,
        sql: str,
        tables: list[str],
        requested_metric: dict[str, Any],
    ) -> dict[str, Any] | None:
        sql_lower = str(sql or "").lower()
        question_lower = str(question or "").lower()
        ranked: list[tuple[float, dict[str, Any]]] = []

        for table in tables:
            asset = self.get_table_asset(table)
            if not asset:
                continue
            for metric in asset.metrics:
                metric_name = str(metric.get("name", ""))
                metric_sql = str(metric.get("sql", ""))
                if not (metric_name and metric_sql):
                    continue
                if metric_sql.lower() not in sql_lower:
                    continue

                phrases = self._metric_phrases(
                    term=metric_name,
                    definition=metric.get("description", ""),
                    synonyms=metric.get("synonyms", []) or [],
                )
                score = 0.30
                if f"as {metric_name.lower()}" in sql_lower:
                    score += 0.20
                if any(phrase and phrase in question_lower for phrase in phrases):
                    score += 0.25
                if table == requested_metric.get("table"):
                    score += 0.10
                ranked.append(
                    (
                        score,
                        {
                            "table": table,
                            "name": metric_name,
                            "sql": metric_sql,
                            "phrases": phrases,
                        },
                    )
                )

        if not ranked:
            return None
        ranked.sort(key=lambda item: item[0], reverse=True)
        best_score, best = ranked[0]
        if best_score < 0.35:
            return None
        return best

    def _table_dimension(self, table: str, column: str) -> dict[str, Any] | None:
        asset = self.get_table_asset(table)
        if not asset:
            return None
        for dimension in asset.dimensions:
            if str(dimension.get("name", "")) == str(column):
                return dimension
        return None

    def _table_metric(self, table: str, metric_name: str) -> dict[str, Any] | None:
        asset = self.get_table_asset(table)
        if not asset:
            return None
        for metric in asset.metrics:
            if str(metric.get("name", "")) == str(metric_name):
                return metric
        return None

    @staticmethod
    def _metric_phrases(
        *,
        term: str,
        definition: str,
        synonyms: list[str] | tuple[str, ...],
    ) -> list[str]:
        phrases = {
            str(term or "").strip().lower(),
            str(term or "").strip().lower().replace("_", " "),
            str(definition or "").strip().lower(),
        }
        for synonym in synonyms or []:
            phrases.add(str(synonym).strip().lower())
        return sorted(
            {" ".join(phrase.split()) for phrase in phrases if " ".join(phrase.split())},
            key=len,
            reverse=True,
        )

    @staticmethod
    def _dimension_phrases(
        *,
        term: str,
        definition: str,
        synonyms: list[str] | tuple[str, ...],
    ) -> list[str]:
        phrases = {
            str(term or "").strip().lower(),
            str(term or "").strip().lower().replace("_", " "),
            str(definition or "").strip().lower(),
        }

        raw_term = str(term or "").strip().lower()
        if raw_term.endswith("_name"):
            phrases.add(raw_term[:-5])
        if raw_term.endswith("_id"):
            phrases.add(raw_term[:-3])

        for synonym in synonyms or []:
            phrases.add(str(synonym).strip().lower())

        expanded: set[str] = set()
        for phrase in phrases:
            normalized = " ".join(phrase.split())
            if not normalized:
                continue
            expanded.add(normalized)
            if " " not in normalized and not normalized.endswith("s"):
                expanded.add(f"{normalized}s")

        return sorted(expanded, key=len, reverse=True)

    def _column_aliases(self, table: str, column: str) -> list[str]:
        resolved_table = self.resolve_table_name(table)
        aliases: list[str] = []
        for table_alias in self.table_aliases(resolved_table):
            aliases.append(f"{table_alias}.{column}")
        aliases.append(column)
        return list(dict.fromkeys(alias for alias in aliases if alias))

    @staticmethod
    def _sql_uses_column_as_filter(sql: str, aliases: list[str]) -> bool:
        sql_lower = str(sql or "").lower()
        for alias in aliases:
            if re.search(rf"\b{re.escape(alias.lower())}\b\s*=", sql_lower):
                return True
        return False

    def _join_swap_context(
        self,
        *,
        root_table: str,
        current_dimension: dict[str, Any],
        requested_dimension: dict[str, Any],
        available_tables: list[str],
    ) -> dict[str, Any] | None:
        current_table = str(current_dimension.get("table", ""))
        requested_table = str(requested_dimension.get("table", ""))
        if not root_table or not current_table or not requested_table:
            return None
        if current_table == root_table or requested_table == root_table:
            return None

        candidates = set(available_tables) | {requested_table}
        old_edge = self._direct_relationship_edge(root_table, current_table, candidates=candidates)
        new_edge = self._direct_relationship_edge(root_table, requested_table, candidates=candidates)
        if not old_edge or not new_edge:
            return None

        old_join_sql = f"JOIN {old_edge['right_table']} ON {old_edge['condition_sql']}"
        new_join_sql = f"JOIN {new_edge['right_table']} ON {new_edge['condition_sql']}"
        if old_join_sql == new_join_sql:
            return None

        requested_refs = self._column_aliases(
            requested_table,
            str(requested_dimension.get("column", "")),
        )
        current_refs = list(current_dimension.get("present_refs", []))
        new_dimension_refs: list[str] = []
        for old_ref in current_refs:
            if "." in old_ref:
                new_dimension_refs.append(f"{new_edge['right_table']}.{requested_dimension['column']}")
            else:
                new_dimension_refs.append(str(requested_dimension["column"]))

        return {
            "old_join_sql": old_join_sql,
            "new_join_sql": new_join_sql,
            "new_dimension_refs": new_dimension_refs or requested_refs[: len(current_refs)],
        }

    def _direct_relationship_edge(
        self,
        left_table: str,
        right_table: str,
        *,
        candidates: set[str] | None = None,
    ) -> dict[str, str] | None:
        left_resolved = self.resolve_table_name(left_table, candidates=candidates)
        right_resolved = self.resolve_table_name(right_table, candidates=candidates)

        for relationship in self._relationships:
            rel_left = self.resolve_table_name(relationship.left_table, candidates=candidates)
            rel_right = self.resolve_table_name(relationship.right_table, candidates=candidates)
            if rel_left == left_resolved and rel_right == right_resolved:
                return {
                    "left_table": rel_left,
                    "right_table": rel_right,
                    "condition_sql": self._relationship_condition_sql(
                        left_table=rel_left,
                        left_column=relationship.left_column,
                        right_table=rel_right,
                        right_column=relationship.right_column,
                        cast_required=relationship.cast_required,
                        cast_type=relationship.cast_type,
                    ),
                }
            if rel_left == right_resolved and rel_right == left_resolved:
                return {
                    "left_table": left_resolved,
                    "right_table": right_resolved,
                    "condition_sql": self._relationship_condition_sql(
                        left_table=left_resolved,
                        left_column=relationship.right_column,
                        right_table=right_resolved,
                        right_column=relationship.left_column,
                        cast_required=relationship.cast_required,
                        cast_type=relationship.cast_type,
                    ),
                }
        return None

    @staticmethod
    def _relationship_condition_sql(
        *,
        left_table: str,
        left_column: str,
        right_table: str,
        right_column: str,
        cast_required: bool,
        cast_type: str,
    ) -> str:
        left_expr = f"{left_table}.{left_column}"
        right_expr = f"{right_table}.{right_column}"
        if cast_required and cast_type:
            left_expr = f"TRY_CAST({left_expr} AS {cast_type})"
            right_expr = f"TRY_CAST({right_expr} AS {cast_type})"
        return f"{left_expr} = {right_expr}"

    def _search_verified_queries(
        self,
        *,
        query_norm: str,
        query_tokens: set[str],
        focus_tables: list[str],
        focus_verified: list[dict[str, Any]],
        focus_type: str,
        focus_name: str,
    ) -> list[dict[str, Any]]:
        assets = list(self._verified_queries)
        focus_source = "metabase" if focus_type in {"dashboard", "question", "collection"} else "focus"
        for raw in focus_verified:
            question = raw.get("question", "")
            sql = raw.get("sql", "")
            metadata = {}
            if focus_source == "metabase":
                metadata = {
                    "focus_name": focus_name,
                    "card_id": raw.get("card_id"),
                    "display": raw.get("display"),
                }
            assets.append(
                VerifiedQueryAsset(
                    question=question,
                    sql=sql,
                    notes=str(raw.get("notes", focus_name if focus_name else "")),
                    category=str(raw.get("category", "")),
                    tables_used=_extract_tables(sql),
                    source=focus_source,
                    tokens=frozenset(
                        _tokenize(
                            " ".join(
                                [
                                    question,
                                    str(raw.get("notes", "")),
                                    str(raw.get("category", "")),
                                    " ".join(_extract_tables(sql)),
                                ]
                            )
                        )
                    ),
                    metadata=metadata,
                )
            )

        ranked: list[dict[str, Any]] = []
        for asset in assets:
            exact_match = _normalize(asset.question) == query_norm
            score = 1.0 if exact_match else _score_overlap(query_tokens, set(asset.tokens))
            if focus_tables and set(asset.tables_used) & set(focus_tables):
                score += 0.15
            if asset.source == "metabase":
                score += 0.10
            if score <= 0:
                continue
            ranked.append(
                {
                    "question": asset.question,
                    "sql": asset.sql,
                    "tables_used": list(asset.tables_used),
                    "similarity": min(score, 1.0),
                    "source": asset.source,
                    "notes": asset.notes,
                    "category": asset.category,
                    "exact_match": exact_match,
                    "metadata": asset.metadata,
                }
            )

        ranked.sort(
            key=lambda item: (
                item.get("exact_match", False),
                item.get("source") == "metabase",
                item.get("similarity", 0.0),
                len(item.get("tables_used", [])) > 0,
            ),
            reverse=True,
        )

        deduped: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        for item in ranked:
            key = (_normalize(item["question"]), _normalize(item["sql"]))
            if key in seen:
                continue
            seen.add(key)
            deduped.append(item)
        return deduped

    def _search_tables(
        self,
        *,
        query_norm: str,
        query_tokens: set[str],
        focus_tables: list[str],
    ) -> list[dict[str, Any]]:
        ranked: list[dict[str, Any]] = []
        for asset in self._tables:
            score = _score_overlap(query_tokens, set(asset.tokens))
            if asset.name in focus_tables:
                score += 0.25
            short_name = asset.name.rsplit(".", 1)[-1].lower()
            if short_name and short_name in query_norm:
                score += 0.2
            if score <= 0:
                continue
            ranked.append(
                {
                    "table": asset.name,
                    "description": asset.description,
                    "similarity": min(score, 1.0),
                    "source": "semantic_model",
                }
            )

        ranked.sort(key=lambda item: item["similarity"], reverse=True)
        deduped: list[dict[str, Any]] = []
        seen: set[str] = set()
        for item in ranked:
            if item["table"] in seen:
                continue
            seen.add(item["table"])
            deduped.append(item)
        return deduped

    def _search_glossary(
        self,
        *,
        query_tokens: set[str],
        ranked_tables: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        top_tables = {item["table"] for item in ranked_tables[:5]}
        results: list[dict[str, Any]] = []
        for asset in self._tables:
            table_bonus = 0.15 if asset.name in top_tables else 0.0

            for metric in asset.metrics:
                term = metric.get("name", "")
                definition = metric.get("description", "")
                sql_fragment = metric.get("sql", "")
                score = _score_overlap(
                    query_tokens,
                    _tokenize(" ".join([term, definition, asset.name, " ".join(asset.synonyms)])),
                ) + table_bonus
                if score > 0:
                    results.append(
                        {
                            "term": term,
                            "definition": definition,
                            "sql_fragment": sql_fragment,
                            "synonyms": list(asset.synonyms),
                            "similarity": min(score, 1.0),
                            "source": "semantic_model",
                            "table": asset.name,
                            "kind": "metric",
                            "field_name": term,
                        }
                    )

            for dimension in asset.dimensions:
                term = dimension.get("name", "")
                definition = dimension.get("description", "")
                score = _score_overlap(
                    query_tokens,
                    _tokenize(" ".join([term, definition, asset.name, " ".join(asset.synonyms)])),
                ) + table_bonus
                if score > 0:
                    results.append(
                        {
                            "term": term,
                            "definition": definition,
                            "sql_fragment": "",
                            "synonyms": list(asset.synonyms),
                            "similarity": min(score, 1.0),
                            "source": "semantic_model",
                            "table": asset.name,
                            "kind": "dimension",
                            "field_name": term,
                        }
                    )

            for dimension in asset.time_dimensions:
                term = dimension.get("name", "")
                definition = dimension.get("description", "")
                score = _score_overlap(
                    query_tokens,
                    _tokenize(" ".join([term, definition, asset.name, " ".join(asset.synonyms)])),
                ) + table_bonus
                if score > 0:
                    results.append(
                        {
                            "term": term,
                            "definition": definition,
                            "sql_fragment": "",
                            "synonyms": list(asset.synonyms),
                            "similarity": min(score, 1.0),
                            "source": "semantic_model",
                            "table": asset.name,
                            "kind": "time_dimension",
                            "field_name": term,
                        }
                    )

        results.sort(key=lambda item: item["similarity"], reverse=True)
        deduped: list[dict[str, Any]] = []
        seen: set[tuple[str, str, str]] = set()
        for item in results:
            key = (
                item["term"].lower(),
                item.get("table", ""),
                item.get("kind", ""),
            )
            if key in seen:
                continue
            seen.add(key)
            deduped.append(item)
        return deduped

    def _search_rules(
        self,
        *,
        query_norm: str,
        query_tokens: set[str],
    ) -> list[dict[str, Any]]:
        matches: list[dict[str, Any]] = []
        for asset in self._rules:
            synonyms = list(asset.synonyms)
            phrase_bonus = 0.0
            if asset.term and asset.term.lower() in query_norm:
                phrase_bonus += 0.3
            for synonym in synonyms:
                if synonym and synonym.lower() in query_norm:
                    phrase_bonus += 0.2
            score = _score_overlap(query_tokens, set(asset.tokens)) + phrase_bonus
            if score <= 0:
                continue
            matches.append(
                {
                    "term": asset.term,
                    "definition": asset.definition,
                    "sql_fragment": asset.sql_fragment,
                    "synonyms": list(asset.synonyms),
                    "categories": list(asset.categories),
                    "rule_type": asset.rule_type,
                    "similarity": min(score, 1.0),
                    "source": "semantic_model",
                }
            )
        matches.sort(key=lambda item: item["similarity"], reverse=True)
        deduped: list[dict[str, Any]] = []
        seen: set[str] = set()
        for item in matches:
            key = item["term"].lower()
            if key in seen:
                continue
            seen.add(key)
            deduped.append(item)
        return deduped

    def _metabase_evidence(
        self,
        *,
        question: str,
        query_tokens: set[str],
        cards: list[dict[str, Any]],
        filters: list[dict[str, Any]],
        focus_name: str,
    ) -> list[dict[str, Any]]:
        if not cards and not filters:
            return []

        evidence: list[dict[str, Any]] = []
        for card in cards:
            name = str(card.get("name", ""))
            sql = str(card.get("sql", ""))
            tables = list(card.get("tables", []) or _extract_tables(sql))
            display = str(card.get("display", ""))
            score = _score_overlap(
                query_tokens,
                _tokenize(" ".join([name, sql, " ".join(tables), focus_name])),
            )
            if score <= 0:
                continue
            evidence.append(
                {
                    "kind": "metabase_card",
                    "name": name,
                    "sql": sql,
                    "tables": tables,
                    "display": display,
                    "score": min(score + 0.1, 1.0),
                    "source": "metabase",
                    "focus_name": focus_name,
                }
            )

        for flt in filters:
            name = str(flt.get("name") or flt.get("slug") or flt.get("id") or "")
            score = _score_overlap(query_tokens, _tokenize(name))
            if score <= 0:
                continue
            evidence.append(
                {
                    "kind": "metabase_filter",
                    "name": name,
                    "sql": "",
                    "tables": [],
                    "display": "",
                    "score": min(score + 0.05, 1.0),
                    "source": "metabase",
                    "focus_name": focus_name,
                }
            )

        evidence.sort(key=lambda item: item["score"], reverse=True)
        return evidence

    @staticmethod
    def _focus_rule_snippets(rules: list[dict[str, Any]]) -> list[dict[str, Any]]:
        snippets: list[dict[str, Any]] = []
        for rule in rules:
            text = rule.get("rule") or rule.get("description") or ""
            if not text:
                continue
            snippets.append(
                {
                    "source": "focus_rule",
                    "table": rule.get("table", ""),
                    "content": str(text),
                    "similarity": 1.0,
                }
            )
        return snippets

    @staticmethod
    def _instruction_snippets(matches: list[dict[str, Any]]) -> list[dict[str, Any]]:
        snippets: list[dict[str, Any]] = []
        for match in matches[:5]:
            snippets.append(
                {
                    "source": "semantic_instruction",
                    "table": "",
                    "content": f"{match['term']}: {match['definition']}",
                    "similarity": match.get("similarity", 0.0),
                }
            )
        return snippets

    @staticmethod
    def _metabase_snippets(evidence: list[dict[str, Any]]) -> list[dict[str, Any]]:
        snippets: list[dict[str, Any]] = []
        for item in evidence[:4]:
            if item.get("kind") != "metabase_card":
                continue
            snippets.append(
                {
                    "source": "metabase_card",
                    "table": ", ".join(item.get("tables", [])),
                    "content": f"Trusted Metabase card: {item.get('name', '')}",
                    "similarity": item.get("score", 0.0),
                }
            )
        return snippets
