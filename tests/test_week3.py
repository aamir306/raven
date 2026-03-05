"""
RAVEN — Week 3 Sub-Module Tests
=================================
Tests for the decomposed sub-modules created in Phase 1 Week 3.
Validates imports, class instantiation, static methods, and basic logic.
Run with: pytest tests/test_week3.py -v
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  IMPORTS — Verify all sub-modules are importable
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestSubModuleImports:
    """Verify every Week 3 sub-module can be imported."""

    # -- Stage 2: Retrieval --
    def test_import_keyword_extractor(self):
        from src.raven.retrieval.keyword_extractor import KeywordExtractor
        assert KeywordExtractor is not None

    def test_import_lsh_matcher(self):
        from src.raven.retrieval.lsh_matcher import LSHMatcher
        assert LSHMatcher is not None

    def test_import_fewshot_retriever(self):
        from src.raven.retrieval.fewshot_retriever import FewShotRetriever
        assert FewShotRetriever is not None

    def test_import_glossary_retriever(self):
        from src.raven.retrieval.glossary_retriever import GlossaryRetriever
        assert GlossaryRetriever is not None

    def test_import_doc_retriever(self):
        from src.raven.retrieval.doc_retriever import DocRetriever
        assert DocRetriever is not None

    def test_import_content_awareness(self):
        from src.raven.retrieval.content_awareness import ContentAwareness
        assert ContentAwareness is not None

    def test_import_information_retriever(self):
        from src.raven.retrieval.information_retriever import InformationRetriever
        assert InformationRetriever is not None

    # -- Stage 3: Schema --
    def test_import_column_filter(self):
        from src.raven.schema.column_filter import ColumnFilter
        assert ColumnFilter is not None

    def test_import_graph_path_finder(self):
        from src.raven.schema.graph_path_finder import GraphPathFinder
        assert GraphPathFinder is not None

    def test_import_table_selector(self):
        from src.raven.schema.table_selector import TableSelector
        assert TableSelector is not None

    def test_import_column_pruner(self):
        from src.raven.schema.column_pruner import ColumnPruner
        assert ColumnPruner is not None

    def test_import_schema_selector(self):
        from src.raven.schema.schema_selector import SchemaSelector
        assert SchemaSelector is not None

    # -- Stage 4: Probes --
    def test_import_probe_planner(self):
        from src.raven.probes.probe_planner import ProbePlanner
        assert ProbePlanner is not None

    def test_import_probe_generator(self):
        from src.raven.probes.probe_generator import ProbeGenerator
        assert ProbeGenerator is not None

    def test_import_probe_executor(self):
        from src.raven.probes.probe_executor import ProbeExecutor
        assert ProbeExecutor is not None

    def test_import_probe_runner(self):
        from src.raven.probes.probe_runner import ProbeRunner
        assert ProbeRunner is not None

    # -- Stage 5: Generation --
    def test_import_divide_and_conquer(self):
        from src.raven.generation.divide_and_conquer import DivideAndConquerGenerator
        assert DivideAndConquerGenerator is not None

    def test_import_execution_plan_cot(self):
        from src.raven.generation.execution_plan_cot import ExecutionPlanCoTGenerator
        assert ExecutionPlanCoTGenerator is not None

    def test_import_fewshot_generator(self):
        from src.raven.generation.fewshot_generator import FewShotGenerator
        assert FewShotGenerator is not None

    def test_import_trino_dialect(self):
        from src.raven.generation.trino_dialect import TrinoDialect
        assert TrinoDialect is not None

    def test_import_revision_loop(self):
        from src.raven.generation.revision_loop import RevisionLoop
        assert RevisionLoop is not None

    def test_import_candidate_generator(self):
        from src.raven.generation.candidate_generator import CandidateGenerator
        assert CandidateGenerator is not None

    # -- Stage 6: Validation --
    def test_import_selection_agent(self):
        from src.raven.validation.selection_agent import SelectionAgent
        assert SelectionAgent is not None

    def test_import_error_taxonomy_checker(self):
        from src.raven.validation.error_taxonomy_checker import ErrorTaxonomyChecker
        assert ErrorTaxonomyChecker is not None

    def test_import_cost_guard(self):
        from src.raven.validation.cost_guard import CostGuard
        assert CostGuard is not None

    def test_import_candidate_selector(self):
        from src.raven.validation.candidate_selector import CandidateSelector
        assert CandidateSelector is not None

    # -- Stage 7: Output --
    def test_import_query_executor(self):
        from src.raven.output.query_executor import QueryExecutor
        assert QueryExecutor is not None

    def test_import_chart_detector(self):
        from src.raven.output.chart_detector import ChartDetector
        assert ChartDetector is not None

    def test_import_chart_generator(self):
        from src.raven.output.chart_generator import ChartGenerator
        assert ChartGenerator is not None

    def test_import_nl_summarizer(self):
        from src.raven.output.nl_summarizer import NLSummarizer
        assert NLSummarizer is not None

    def test_import_output_renderer(self):
        from src.raven.output.renderer import OutputRenderer
        assert OutputRenderer is not None


class TestPackageExports:
    """Verify __init__.py re-exports work correctly."""

    def test_retrieval_package_exports(self):
        from src.raven.retrieval import (
            InformationRetriever, KeywordExtractor, LSHMatcher,
            FewShotRetriever, GlossaryRetriever, DocRetriever,
            ContentAwareness,
        )
        assert all(cls is not None for cls in [
            InformationRetriever, KeywordExtractor, LSHMatcher,
            FewShotRetriever, GlossaryRetriever, DocRetriever,
            ContentAwareness,
        ])

    def test_schema_package_exports(self):
        from src.raven.schema import (
            SchemaSelector, ColumnFilter, GraphPathFinder,
            TableSelector, ColumnPruner,
        )
        assert all(cls is not None for cls in [
            SchemaSelector, ColumnFilter, GraphPathFinder,
            TableSelector, ColumnPruner,
        ])

    def test_probes_package_exports(self):
        from src.raven.probes import (
            ProbeRunner, ProbePlanner, ProbeGenerator, ProbeExecutor,
        )
        assert all(cls is not None for cls in [
            ProbeRunner, ProbePlanner, ProbeGenerator, ProbeExecutor,
        ])

    def test_generation_package_exports(self):
        from src.raven.generation import (
            CandidateGenerator, DivideAndConquerGenerator,
            ExecutionPlanCoTGenerator, FewShotGenerator,
            TrinoDialect, RevisionLoop,
        )
        assert all(cls is not None for cls in [
            CandidateGenerator, DivideAndConquerGenerator,
            ExecutionPlanCoTGenerator, FewShotGenerator,
            TrinoDialect, RevisionLoop,
        ])

    def test_validation_package_exports(self):
        from src.raven.validation import (
            CandidateSelector, SelectionAgent,
            ErrorTaxonomyChecker, CostGuard,
        )
        assert all(cls is not None for cls in [
            CandidateSelector, SelectionAgent,
            ErrorTaxonomyChecker, CostGuard,
        ])

    def test_output_package_exports(self):
        from src.raven.output import (
            OutputRenderer, QueryExecutor, ChartDetector,
            ChartGenerator, NLSummarizer,
        )
        assert all(cls is not None for cls in [
            OutputRenderer, QueryExecutor, ChartDetector,
            ChartGenerator, NLSummarizer,
        ])


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  INSTANTIATION — Verify classes can be constructed with mocks
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestInstantiation:
    """Test that all classes can be constructed with mock dependencies."""

    def _mock_openai(self):
        m = MagicMock()
        m.complete = AsyncMock(return_value="")
        m.embed = AsyncMock(return_value=[0.1] * 1536)
        return m

    def _mock_pgvector(self):
        m = MagicMock()
        m.search = AsyncMock(return_value=[])
        return m

    def _mock_trino(self):
        m = MagicMock()
        m.execute = AsyncMock(return_value=MagicMock())
        return m

    # -- Stage 2 --
    def test_keyword_extractor_init(self):
        from src.raven.retrieval.keyword_extractor import KeywordExtractor
        obj = KeywordExtractor(self._mock_openai())
        assert obj is not None

    def test_lsh_matcher_init(self):
        from src.raven.retrieval.lsh_matcher import LSHMatcher
        obj = LSHMatcher()
        assert obj is not None

    def test_fewshot_retriever_init(self):
        from src.raven.retrieval.fewshot_retriever import FewShotRetriever
        obj = FewShotRetriever(self._mock_pgvector())
        assert obj is not None

    def test_glossary_retriever_init(self):
        from src.raven.retrieval.glossary_retriever import GlossaryRetriever
        obj = GlossaryRetriever(self._mock_pgvector())
        assert obj is not None

    def test_doc_retriever_init(self):
        from src.raven.retrieval.doc_retriever import DocRetriever
        obj = DocRetriever(self._mock_pgvector())
        assert obj is not None

    def test_content_awareness_init_no_path(self):
        from src.raven.retrieval.content_awareness import ContentAwareness
        obj = ContentAwareness()
        assert obj is not None

    def test_information_retriever_init(self):
        from src.raven.retrieval.information_retriever import InformationRetriever
        obj = InformationRetriever(self._mock_openai(), self._mock_pgvector())
        assert obj is not None

    # -- Stage 3 --
    def test_column_filter_init(self):
        from src.raven.schema.column_filter import ColumnFilter
        obj = ColumnFilter(self._mock_openai(), self._mock_pgvector())
        assert obj is not None

    def test_graph_path_finder_init(self):
        from src.raven.schema.graph_path_finder import GraphPathFinder
        obj = GraphPathFinder()
        assert obj is not None

    def test_table_selector_init(self):
        from src.raven.schema.table_selector import TableSelector
        obj = TableSelector(self._mock_openai())
        assert obj is not None

    def test_column_pruner_init(self):
        from src.raven.schema.column_pruner import ColumnPruner
        obj = ColumnPruner(self._mock_openai())
        assert obj is not None

    def test_schema_selector_init(self):
        from src.raven.schema.schema_selector import SchemaSelector
        obj = SchemaSelector(self._mock_openai(), self._mock_pgvector())
        assert obj is not None

    # -- Stage 4 --
    def test_probe_planner_init(self):
        from src.raven.probes.probe_planner import ProbePlanner
        obj = ProbePlanner(self._mock_openai())
        assert obj is not None

    def test_probe_generator_init(self):
        from src.raven.probes.probe_generator import ProbeGenerator
        obj = ProbeGenerator(self._mock_openai())
        assert obj is not None

    def test_probe_executor_init(self):
        from src.raven.probes.probe_executor import ProbeExecutor
        obj = ProbeExecutor(self._mock_trino())
        assert obj is not None

    def test_probe_runner_init(self):
        from src.raven.probes.probe_runner import ProbeRunner
        obj = ProbeRunner(self._mock_openai(), self._mock_trino())
        assert obj is not None

    # -- Stage 5 --
    def test_divide_and_conquer_init(self):
        from src.raven.generation.divide_and_conquer import DivideAndConquerGenerator
        obj = DivideAndConquerGenerator(self._mock_openai())
        assert obj is not None

    def test_execution_plan_cot_init(self):
        from src.raven.generation.execution_plan_cot import ExecutionPlanCoTGenerator
        obj = ExecutionPlanCoTGenerator(self._mock_openai())
        assert obj is not None

    def test_fewshot_generator_init(self):
        from src.raven.generation.fewshot_generator import FewShotGenerator
        obj = FewShotGenerator(self._mock_openai())
        assert obj is not None

    def test_trino_dialect_init(self):
        from src.raven.generation.trino_dialect import TrinoDialect
        obj = TrinoDialect()
        assert obj is not None

    def test_revision_loop_init(self):
        from src.raven.generation.revision_loop import RevisionLoop
        obj = RevisionLoop(self._mock_openai(), self._mock_trino())
        assert obj is not None

    def test_candidate_generator_init(self):
        from src.raven.generation.candidate_generator import CandidateGenerator
        obj = CandidateGenerator(self._mock_openai(), self._mock_trino())
        assert obj is not None

    # -- Stage 6 --
    def test_selection_agent_init(self):
        from src.raven.validation.selection_agent import SelectionAgent
        obj = SelectionAgent(self._mock_openai())
        assert obj is not None

    def test_error_taxonomy_checker_init(self):
        from src.raven.validation.error_taxonomy_checker import ErrorTaxonomyChecker
        obj = ErrorTaxonomyChecker(self._mock_openai())
        assert obj is not None

    def test_cost_guard_init(self):
        from src.raven.validation.cost_guard import CostGuard
        obj = CostGuard(self._mock_trino())
        assert obj is not None

    def test_candidate_selector_init(self):
        from src.raven.validation.candidate_selector import CandidateSelector
        obj = CandidateSelector(self._mock_openai(), self._mock_trino())
        assert obj is not None

    # -- Stage 7 --
    def test_query_executor_init(self):
        from src.raven.output.query_executor import QueryExecutor
        obj = QueryExecutor(self._mock_trino())
        assert obj is not None

    def test_chart_detector_init(self):
        from src.raven.output.chart_detector import ChartDetector
        obj = ChartDetector(self._mock_openai())
        assert obj is not None

    def test_chart_generator_init(self):
        from src.raven.output.chart_generator import ChartGenerator
        obj = ChartGenerator()
        assert obj is not None

    def test_nl_summarizer_init(self):
        from src.raven.output.nl_summarizer import NLSummarizer
        obj = NLSummarizer(self._mock_openai())
        assert obj is not None

    def test_output_renderer_init(self):
        from src.raven.output.renderer import OutputRenderer
        obj = OutputRenderer(self._mock_openai(), self._mock_trino())
        assert obj is not None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  STATIC METHODS & UTILITIES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestKeywordExtractorParsing:
    """Test KeywordExtractor._parse_response."""

    def test_parse_full_response(self):
        from src.raven.retrieval.keyword_extractor import KeywordExtractor
        response = """KEYWORDS: revenue, total sales, monthly
TIME_RANGE: last 30 days
METRICS: total_revenue, average_order_value
ENTITIES: gold.finance.revenue_daily, gold.sales.orders"""
        result = KeywordExtractor._parse_response(response)
        assert "keywords" in result
        assert "revenue" in result["keywords"]
        assert "time_range" in result
        assert "last 30 days" in result["time_range"]
        assert "metrics" in result
        assert len(result["metrics"]) >= 1
        assert "entities" in result
        assert len(result["entities"]) >= 1

    def test_parse_empty_response(self):
        from src.raven.retrieval.keyword_extractor import KeywordExtractor
        result = KeywordExtractor._parse_response("")
        assert isinstance(result, dict)
        assert "keywords" in result


class TestLSHMatcher:
    """Test LSHMatcher tokenization and query without index."""

    def test_tokenize_basic(self):
        from src.raven.retrieval.lsh_matcher import LSHMatcher
        tokens = LSHMatcher._tokenize("orders")
        # 3-gram tokenization: "ord", "rde", "der", "ers"
        assert len(tokens) > 0
        assert "ord" in tokens

    def test_tokenize_short_word(self):
        from src.raven.retrieval.lsh_matcher import LSHMatcher
        tokens = LSHMatcher._tokenize("ab")
        # Should still produce something (or empty for < 3 chars)
        assert isinstance(tokens, (list, set))

    def test_query_without_index_returns_empty(self):
        from src.raven.retrieval.lsh_matcher import LSHMatcher
        matcher = LSHMatcher()
        results = matcher.query("revenue")
        assert results == []


class TestContentAwareness:
    """Test ContentAwareness local lookup."""

    def test_get_nonexistent_returns_empty_dict(self):
        from src.raven.retrieval.content_awareness import ContentAwareness
        ca = ContentAwareness()
        result = ca.get("nonexistent_table", "nonexistent_column")
        assert result == {}


class TestGraphPathFinder:
    """Test GraphPathFinder static methods and graph operations."""

    def test_extract_tables_from_columns(self):
        from src.raven.schema.graph_path_finder import GraphPathFinder
        # _extract_tables expects list of "table.column" strings
        columns = [
            "iceberg.gold.orders.order_id",
            "iceberg.gold.orders.amount",
            "iceberg.silver.users.user_id",
        ]
        tables = GraphPathFinder._extract_tables(columns)
        assert "iceberg.gold.orders" in tables
        assert "iceberg.silver.users" in tables

    def test_expand_tables_without_graph(self):
        from src.raven.schema.graph_path_finder import GraphPathFinder
        finder = GraphPathFinder()
        columns = {"iceberg.gold.orders": ["order_id"]}
        result = finder.expand_tables(columns)
        # Without graph, should return original tables
        assert isinstance(result, (list, dict, set))

    def test_set_graph(self):
        from src.raven.schema.graph_path_finder import GraphPathFinder
        import networkx as nx
        finder = GraphPathFinder()
        g = nx.DiGraph()
        g.add_edge("A", "B", fk="A.id=B.a_id")
        finder.set_graph(g)
        assert finder._graph is not None


class TestTrinoDialect:
    """Test TrinoDialect static SQL utilities."""

    def test_is_read_only_select(self):
        from src.raven.generation.trino_dialect import TrinoDialect
        assert TrinoDialect.is_read_only("SELECT * FROM users") is True

    def test_is_read_only_insert(self):
        from src.raven.generation.trino_dialect import TrinoDialect
        assert TrinoDialect.is_read_only("INSERT INTO users VALUES (1)") is False

    def test_is_read_only_drop(self):
        from src.raven.generation.trino_dialect import TrinoDialect
        assert TrinoDialect.is_read_only("DROP TABLE users") is False

    def test_strip_semicolons(self):
        from src.raven.generation.trino_dialect import TrinoDialect
        assert TrinoDialect.strip_semicolons("SELECT 1;") == "SELECT 1"
        assert TrinoDialect.strip_semicolons("SELECT 1;;;") == "SELECT 1"

    def test_ensure_limit_adds_limit(self):
        from src.raven.generation.trino_dialect import TrinoDialect
        sql = "SELECT * FROM users"
        result = TrinoDialect.ensure_limit(sql, max_rows=100)
        assert "LIMIT" in result.upper()

    def test_ensure_limit_preserves_existing(self):
        from src.raven.generation.trino_dialect import TrinoDialect
        sql = "SELECT * FROM users LIMIT 50"
        result = TrinoDialect.ensure_limit(sql, max_rows=100)
        assert "50" in result

    def test_classify_error_returns_tuple(self):
        from src.raven.generation.trino_dialect import TrinoDialect
        dialect = TrinoDialect()
        result = dialect.classify_error("Table 'xxx' does not exist")
        assert isinstance(result, tuple)
        assert len(result) == 3  # (category, subtype, description)


class TestSQLExtraction:
    """Test extract_sql from divide_and_conquer module."""

    def test_extract_from_code_block(self):
        from src.raven.generation.divide_and_conquer import extract_sql
        response = "Here's the SQL:\n```sql\nSELECT * FROM users\n```"
        result = extract_sql(response)
        assert result.strip() == "SELECT * FROM users"

    def test_extract_bare_select(self):
        from src.raven.generation.divide_and_conquer import extract_sql
        response = "SELECT count(*) FROM orders"
        result = extract_sql(response)
        assert "SELECT" in result

    def test_extract_with_cte(self):
        from src.raven.generation.divide_and_conquer import extract_sql
        response = "WITH cte AS (\n  SELECT 1\n)\nSELECT * FROM cte"
        result = extract_sql(response)
        assert "WITH cte" in result


class TestProbeGeneratorCleaning:
    """Test ProbeGenerator SQL cleaning utilities."""

    def test_clean_sql_strips_semicolons(self):
        from src.raven.probes.probe_generator import ProbeGenerator
        result = ProbeGenerator._clean_sql("SELECT 1;")
        assert not result.endswith(";")

    def test_extract_sql_from_block(self):
        from src.raven.probes.probe_generator import ProbeGenerator
        response = "```sql\nSELECT 1\n```"
        result = ProbeGenerator._extract_sql(response)
        assert "SELECT" in result


class TestCostGuardParsing:
    """Test CostGuard plan parsing statics."""

    def test_parse_scan_size(self):
        from src.raven.validation.cost_guard import CostGuard
        plan_text = "ScanFilterProject: est. 1.5GB scanned, 10M rows"
        result = CostGuard._parse_scan_size(plan_text)
        assert isinstance(result, (int, float))

    def test_parse_output_rows(self):
        from src.raven.validation.cost_guard import CostGuard
        plan_text = "Output: 50000 rows"
        result = CostGuard._parse_output_rows(plan_text)
        assert isinstance(result, (int, float))


class TestChartDetectorParsing:
    """Test ChartDetector response parsing (returns dict)."""

    def test_parse_response_with_chart_type(self):
        from src.raven.output.chart_detector import ChartDetector
        result = ChartDetector._parse_response("CHART_TYPE: BAR\nX_AXIS: date\nY_AXIS: revenue")
        assert isinstance(result, dict)
        assert result["type"] == "BAR"
        assert result["x_axis"] == "date"

    def test_parse_response_defaults_to_table(self):
        from src.raven.output.chart_detector import ChartDetector
        result = ChartDetector._parse_response("")
        assert result["type"] == "TABLE"

    def test_parse_response_uppercase(self):
        from src.raven.output.chart_detector import ChartDetector
        result = ChartDetector._parse_response("CHART_TYPE: pie\nTITLE: Revenue Split")
        assert result["type"] == "PIE"
        assert result["title"] == "Revenue Split"


class TestErrorTaxonomyCheckerParsing:
    """Test ErrorTaxonomyChecker._parse_errors."""

    def test_parse_no_errors(self):
        from src.raven.validation.error_taxonomy_checker import ErrorTaxonomyChecker
        result = ErrorTaxonomyChecker._parse_errors("ERRORS_FOUND: FALSE\nNo errors detected.")
        assert isinstance(result, list)
        assert len(result) == 0

    def test_parse_with_errors(self):
        from src.raven.validation.error_taxonomy_checker import ErrorTaxonomyChecker
        response = """ERRORS_FOUND:
- GROUP_BY_MISSING: Missing GROUP BY for aggregated column
- JOIN_AMBIGUOUS: Ambiguous join condition"""
        result = ErrorTaxonomyChecker._parse_errors(response)
        assert isinstance(result, list)
        assert len(result) >= 1


class TestTableSelectorParsing:
    """Test TableSelector._parse_response (returns tuple)."""

    def test_parse_selected_tables(self):
        from src.raven.schema.table_selector import TableSelector
        response = """SELECTED_TABLES:
- iceberg.gold.orders
- iceberg.gold.customers
JOIN_PATH:
- orders.customer_id = customers.id"""
        result = TableSelector._parse_response(response)
        assert isinstance(result, tuple)
        tables, join_paths = result
        assert "iceberg.gold.orders" in tables
        assert "iceberg.gold.customers" in tables
        assert len(join_paths) >= 1


class TestColumnFilterParsing:
    """Test ColumnFilter._parse_columns (returns list of column refs)."""

    def test_parse_columns_basic(self):
        from src.raven.schema.column_filter import ColumnFilter
        response = """iceberg.gold.orders.order_id — Primary key
iceberg.gold.orders.amount — Order total
iceberg.gold.customers.name — Customer name"""
        result = ColumnFilter._parse_columns(response)
        assert isinstance(result, list)
        assert len(result) >= 2
        assert any("orders" in col for col in result)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ASYNC FUNCTIONALITY — Light integration with mocks
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestKeywordExtractorAsync:
    """Test KeywordExtractor.extract with mocked LLM."""

    @pytest.mark.asyncio
    async def test_extract_returns_parsed_dict(self):
        from src.raven.retrieval.keyword_extractor import KeywordExtractor

        mock_openai = MagicMock()
        mock_openai.complete = AsyncMock(
            return_value="KEYWORDS: revenue, monthly\nTIME_RANGE: last 7 days"
        )

        extractor = KeywordExtractor(mock_openai)
        result = await extractor.extract("Show me revenue for last 7 days")
        assert isinstance(result, dict)
        assert "keywords" in result


class TestChartGeneratorAsync:
    """Test ChartGenerator.generate with mock data."""

    @pytest.mark.asyncio
    async def test_generate_kpi(self):
        from src.raven.output.chart_generator import ChartGenerator
        import pandas as pd

        gen = ChartGenerator()
        df = pd.DataFrame({"total_revenue": [1234567.89]})
        result = await gen.generate("KPI", df, x_axis="", y_axis="total_revenue", title="Revenue")
        assert isinstance(result, dict)
        assert "type" in result or "mark" in result or "$schema" in result or len(result) > 0

    @pytest.mark.asyncio
    async def test_generate_table(self):
        from src.raven.output.chart_generator import ChartGenerator
        import pandas as pd

        gen = ChartGenerator()
        df = pd.DataFrame({"name": ["Alice", "Bob"], "score": [90, 85]})
        result = await gen.generate("TABLE", df, x_axis="name", y_axis="score", title="Scores")
        assert isinstance(result, dict)


class TestCostGuardAsync:
    """Test CostGuard.check with mocked Trino EXPLAIN."""

    @pytest.mark.asyncio
    async def test_check_passes_small_query(self):
        from src.raven.validation.cost_guard import CostGuard
        import pandas as pd

        mock_trino = MagicMock()
        # Simulate EXPLAIN output with small scan
        explain_df = pd.DataFrame({"Query Plan": ["Fragment 0: Output 100 rows, 0.01GB"]})
        mock_trino.execute = AsyncMock(return_value=explain_df)

        guard = CostGuard(mock_trino)
        result = await guard.check("SELECT * FROM small_table LIMIT 10")
        assert isinstance(result, dict)


class TestOutputRendererAsync:
    """Test OutputRenderer.render with mocked dependencies."""

    @pytest.mark.asyncio
    async def test_render_returns_dict(self):
        from src.raven.output.renderer import OutputRenderer
        import pandas as pd

        mock_openai = MagicMock()
        mock_openai.complete = AsyncMock(return_value="TABLE")
        mock_trino = MagicMock()

        renderer = OutputRenderer(mock_openai, mock_trino)
        df = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
        result = await renderer.render("Show users", "SELECT * FROM users", df)
        assert isinstance(result, dict)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  EVAL ACCURACY — Test the evaluator itself
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestEvalAccuracy:
    """Test the accuracy evaluator module."""

    def test_load_test_set(self):
        from tests.eval_accuracy import AccuracyEvaluator
        evaluator = AccuracyEvaluator()
        assert len(evaluator.test_set) == 200

    def test_extract_tables_from_sql(self):
        from tests.eval_accuracy import AccuracyEvaluator
        sql = "SELECT * FROM iceberg.gold.orders JOIN iceberg.gold.customers ON orders.id = customers.order_id"
        tables = AccuracyEvaluator._extract_tables(sql)
        assert "iceberg.gold.orders" in tables
        assert "iceberg.gold.customers" in tables

    def test_table_coverage_full(self):
        from tests.eval_accuracy import AccuracyEvaluator
        score = AccuracyEvaluator._compute_table_coverage(
            ["iceberg.gold.orders"], ["iceberg.gold.orders"],
        )
        assert score == 1.0

    def test_table_coverage_partial(self):
        from tests.eval_accuracy import AccuracyEvaluator
        score = AccuracyEvaluator._compute_table_coverage(
            ["iceberg.gold.orders", "iceberg.gold.items"],
            ["iceberg.gold.orders"],
        )
        assert score == 0.5

    def test_table_coverage_empty_expected(self):
        from tests.eval_accuracy import AccuracyEvaluator
        score = AccuracyEvaluator._compute_table_coverage([], [])
        assert score == 1.0

    @pytest.mark.asyncio
    async def test_dry_run(self):
        from tests.eval_accuracy import AccuracyEvaluator
        evaluator = AccuracyEvaluator()
        summary = await evaluator.run(dry_run=True)
        assert summary.total == 200
        assert summary.composite_score_avg == 100.0

    def test_format_report(self):
        from tests.eval_accuracy import EvalSummary, format_report
        summary = EvalSummary(
            total=10,
            pass_rate=90.0,
            execution_success_rate=80.0,
            difficulty_accuracy=90.0,
            table_coverage_avg=85.0,
            composite_score_avg=75.0,
            latency_avg_s=5.0,
            latency_p95_s=12.0,
            total_cost_usd=0.5,
            simple_pass_rate=85.0,
            complex_pass_rate=100.0,
            category_scores={},
            failures=[],
        )
        report = format_report(summary)
        assert "RAVEN" in report
        assert "75.0" in report
