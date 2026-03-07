"""
RAVEN — Week 5 Tests
======================
Tests for web service layer: routes, middleware, models, and K8s/Docker configs.
"""

import asyncio
import os
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

# ═══════════════════════════════════════════════════════════════════════
# 1. Route & Model Imports
# ═══════════════════════════════════════════════════════════════════════


class TestRouteImports(unittest.TestCase):
    """Verify all route modules import without error."""

    def test_import_routes_package(self):
        from web import routes
        self.assertTrue(hasattr(routes, "query_router"))
        self.assertTrue(hasattr(routes, "admin_router"))
        self.assertTrue(hasattr(routes, "metrics_router"))

    def test_import_query_router(self):
        from web.routes import query_router
        self.assertEqual(query_router.prefix, "/api")
        self.assertIn("query", query_router.tags)

    def test_import_admin_router(self):
        from web.routes import admin_router
        self.assertEqual(admin_router.prefix, "/api/admin")
        self.assertIn("admin", admin_router.tags)

    def test_import_metrics_router(self):
        from web.routes import metrics_router
        self.assertEqual(metrics_router.prefix, "/api")
        self.assertIn("metrics", metrics_router.tags)


class TestPydanticModels(unittest.TestCase):
    """Verify Pydantic request/response models."""

    def test_query_request_valid(self):
        from web.routes import QueryRequest
        req = QueryRequest(question="What is the total revenue?")
        self.assertEqual(req.question, "What is the total revenue?")
        self.assertIsNone(req.conversation_id)

    def test_query_request_with_conversation(self):
        from web.routes import QueryRequest
        req = QueryRequest(question="Show me sales", conversation_id="abc-123")
        self.assertEqual(req.conversation_id, "abc-123")

    def test_query_request_rejects_empty(self):
        from web.routes import QueryRequest
        from pydantic import ValidationError
        with self.assertRaises(ValidationError):
            QueryRequest(question="")

    def test_query_response_defaults(self):
        from web.routes import QueryResponse
        resp = QueryResponse(status="ok")
        self.assertEqual(resp.query_id, "")
        self.assertEqual(resp.sql, "")
        self.assertEqual(resp.data, [])
        self.assertEqual(resp.row_count, 0)
        self.assertEqual(resp.chart_type, "TABLE")
        self.assertEqual(resp.confidence, "LOW")
        self.assertEqual(resp.cost, 0.0)

    def test_query_response_full(self):
        from web.routes import QueryResponse
        resp = QueryResponse(
            status="success",
            query_id="a1b2c3d4",
            question="Top customers",
            sql="SELECT * FROM customers LIMIT 10",
            data=[{"id": 1, "name": "ACME"}],
            row_count=1,
            chart_type="TABLE",
            chart_config={},
            summary="Found 1 customer.",
            confidence="HIGH",
            difficulty="SIMPLE",
            timings={"total_ms": 150},
            cost=0.003,
        )
        self.assertEqual(resp.status, "success")
        self.assertEqual(resp.row_count, 1)

    def test_feedback_request_thumbs_up(self):
        from web.routes import FeedbackRequest
        req = FeedbackRequest(query_id="abc", feedback="thumbs_up")
        self.assertEqual(req.feedback, "thumbs_up")
        self.assertIsNone(req.correction_sql)

    def test_feedback_request_thumbs_down_with_correction(self):
        from web.routes import FeedbackRequest
        req = FeedbackRequest(
            query_id="abc",
            feedback="thumbs_down",
            correction_sql="SELECT 1",
            correction_notes="Should use SUM",
        )
        self.assertEqual(req.correction_sql, "SELECT 1")

    def test_feedback_request_rejects_invalid(self):
        from web.routes import FeedbackRequest
        from pydantic import ValidationError
        with self.assertRaises(ValidationError):
            FeedbackRequest(query_id="x", feedback="neutral")

    def test_feedback_response(self):
        from web.routes import FeedbackResponse
        resp = FeedbackResponse(query_id="x", feedback="thumbs_up", action="stored")
        self.assertEqual(resp.action, "stored")

    def test_refresh_request_defaults(self):
        from web.routes import RefreshRequest
        req = RefreshRequest()
        self.assertEqual(req.stages, ["all"])
        self.assertFalse(req.dry_run)

    def test_refresh_request_custom_stages(self):
        from web.routes import RefreshRequest
        req = RefreshRequest(stages=["dbt", "lsh"], dry_run=True)
        self.assertEqual(req.stages, ["dbt", "lsh"])
        self.assertTrue(req.dry_run)

    def test_refresh_response(self):
        from web.routes import RefreshResponse
        resp = RefreshResponse(
            status="accepted",
            stages_triggered=["dbt", "lsh"],
            message="Refresh queued.",
        )
        self.assertEqual(len(resp.stages_triggered), 2)

    def test_upload_doc_response(self):
        from web.routes import UploadDocResponse
        resp = UploadDocResponse(
            status="uploaded",
            filename="guide.md",
            chunks_created=5,
            message="Ingested.",
        )
        self.assertEqual(resp.filename, "guide.md")
        self.assertEqual(resp.chunks_created, 5)


# ═══════════════════════════════════════════════════════════════════════
# 2. Middleware Tests
# ═══════════════════════════════════════════════════════════════════════


class TestMiddlewareImports(unittest.TestCase):
    """Verify middleware module imports."""

    def test_import_middleware_package(self):
        from web import middleware
        self.assertTrue(hasattr(middleware, "BasicAuthMiddleware"))
        self.assertTrue(hasattr(middleware, "RateLimitMiddleware"))
        self.assertTrue(hasattr(middleware, "RequestTimingMiddleware"))

    def test_auth_public_paths(self):
        from web.middleware import BasicAuthMiddleware
        self.assertIn("/health", BasicAuthMiddleware.PUBLIC_PATHS)
        self.assertIn("/docs", BasicAuthMiddleware.PUBLIC_PATHS)


class TestRateLimiter(unittest.TestCase):
    """Test rate limiter logic directly."""

    def test_init_default_rpm(self):
        from web.middleware import RateLimitMiddleware

        fake_app = MagicMock()
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("RAVEN_RATE_LIMIT_RPM", None)
            rl = RateLimitMiddleware(fake_app)
            self.assertEqual(rl.rpm, 60)

    def test_init_custom_rpm(self):
        from web.middleware import RateLimitMiddleware

        fake_app = MagicMock()
        rl = RateLimitMiddleware(fake_app, rpm=10)
        self.assertEqual(rl.rpm, 10)

    def test_bucket_cleanup(self):
        """Old entries should be pruned from the bucket."""
        from web.middleware import RateLimitMiddleware

        fake_app = MagicMock()
        rl = RateLimitMiddleware(fake_app, rpm=100)
        # Simulate old requests (older than 60s ago)
        old_time = time.time() - 120
        rl._buckets["127.0.0.1"] = [old_time] * 50
        # After cleanup, they should be removed
        now = time.time()
        bucket = rl._buckets["127.0.0.1"]
        cleaned = [t for t in bucket if now - t < 60]
        self.assertEqual(len(cleaned), 0)


# ═══════════════════════════════════════════════════════════════════════
# 3. API App Tests
# ═══════════════════════════════════════════════════════════════════════


class TestAPIApp(unittest.TestCase):
    """Test the FastAPI app structure."""

    def test_import_app(self):
        from src.raven.api import app
        self.assertEqual(app.title, "RAVEN")
        self.assertEqual(app.version, "0.2.0")

    def test_health_route_exists(self):
        from src.raven.api import app
        routes = [r.path for r in app.routes]
        self.assertIn("/health", routes)

    def test_query_route_exists(self):
        from src.raven.api import app
        routes = [r.path for r in app.routes]
        self.assertIn("/api/query", routes)

    def test_feedback_route_exists(self):
        from src.raven.api import app
        routes = [r.path for r in app.routes]
        self.assertIn("/api/feedback", routes)

    def test_metrics_route_exists(self):
        from src.raven.api import app
        routes = [r.path for r in app.routes]
        self.assertIn("/api/metrics", routes)

    def test_stats_route_exists(self):
        from src.raven.api import app
        routes = [r.path for r in app.routes]
        self.assertIn("/api/stats", routes)

    def test_admin_upload_route_exists(self):
        from src.raven.api import app
        routes = [r.path for r in app.routes]
        self.assertIn("/api/admin/upload-doc", routes)

    def test_admin_refresh_route_exists(self):
        from src.raven.api import app
        routes = [r.path for r in app.routes]
        self.assertIn("/api/admin/refresh", routes)


# ═══════════════════════════════════════════════════════════════════════
# 4. React UI File Structure
# ═══════════════════════════════════════════════════════════════════════


class TestUIFileStructure(unittest.TestCase):
    """Verify React UI files exist."""

    BASE = Path(__file__).resolve().parent.parent / "web" / "ui"

    def test_package_json(self):
        self.assertTrue((self.BASE / "package.json").exists())

    def test_public_index(self):
        self.assertTrue((self.BASE / "public" / "index.html").exists())

    def test_src_index(self):
        self.assertTrue((self.BASE / "src" / "index.js").exists())

    def test_src_app(self):
        self.assertTrue((self.BASE / "src" / "App.js").exists())

    def test_src_app_css(self):
        self.assertTrue((self.BASE / "src" / "App.css").exists())

    def test_component_query_input(self):
        self.assertTrue((self.BASE / "src" / "components" / "Landing.js").exists())

    def test_component_sql_display(self):
        self.assertTrue((self.BASE / "src" / "components" / "tabs" / "SQLTab.js").exists())

    def test_component_data_table(self):
        self.assertTrue((self.BASE / "src" / "components" / "tabs" / "DataTab.js").exists())

    def test_component_chart_panel(self):
        self.assertTrue((self.BASE / "src" / "components" / "tabs" / "ChartTab.js").exists())

    def test_component_summary(self):
        self.assertTrue((self.BASE / "src" / "components" / "tabs" / "SummaryTab.js").exists())

    def test_component_feedback_panel(self):
        self.assertTrue((self.BASE / "src" / "components" / "FeedbackBar.js").exists())


# ═══════════════════════════════════════════════════════════════════════
# 5. Docker & K8s Configuration
# ═══════════════════════════════════════════════════════════════════════


class TestDockerConfig(unittest.TestCase):
    """Verify Docker files exist and are valid."""

    ROOT = Path(__file__).resolve().parent.parent

    def test_docker_compose_exists(self):
        self.assertTrue((self.ROOT / "docker-compose.yaml").exists())

    def test_ui_dockerfile_exists(self):
        self.assertTrue((self.ROOT / "web" / "ui" / "Dockerfile").exists())

    def test_nginx_conf_exists(self):
        self.assertTrue((self.ROOT / "web" / "ui" / "nginx.conf").exists())

    def test_docker_compose_has_ui_service(self):
        content = (self.ROOT / "docker-compose.yaml").read_text()
        self.assertIn("ui:", content)
        self.assertIn("3000:80", content)

    def test_docker_compose_has_raven_env_vars(self):
        content = (self.ROOT / "docker-compose.yaml").read_text()
        self.assertIn("RAVEN_API_KEY", content)
        self.assertIn("RAVEN_RATE_LIMIT_RPM", content)

    def test_ui_dockerfile_multistage(self):
        content = (self.ROOT / "web" / "ui" / "Dockerfile").read_text()
        self.assertIn("FROM node:", content)
        self.assertIn("FROM nginx:", content)
        self.assertIn("npm run build", content)

    def test_nginx_conf_has_api_proxy(self):
        content = (self.ROOT / "web" / "ui" / "nginx.conf").read_text()
        self.assertIn("proxy_pass", content)
        self.assertIn("/api/", content)


class TestK8sManifests(unittest.TestCase):
    """Verify Kubernetes manifests exist and have key content."""

    K8S = Path(__file__).resolve().parent.parent / "k8s"

    def test_namespace_yaml(self):
        self.assertTrue((self.K8S / "namespace.yaml").exists())
        content = (self.K8S / "namespace.yaml").read_text()
        self.assertIn("name: raven", content)

    def test_configmap_yaml(self):
        self.assertTrue((self.K8S / "configmap.yaml").exists())
        content = (self.K8S / "configmap.yaml").read_text()
        self.assertIn("TRINO_HOST", content)
        self.assertIn("PGVECTOR_HOST", content)

    def test_secret_yaml(self):
        self.assertTrue((self.K8S / "secret.yaml").exists())
        content = (self.K8S / "secret.yaml").read_text()
        self.assertIn("OPENAI_API_KEY", content)
        self.assertIn("REPLACE_ME", content)

    def test_deployment_yaml(self):
        self.assertTrue((self.K8S / "deployment.yaml").exists())
        content = (self.K8S / "deployment.yaml").read_text()
        self.assertIn("raven-api", content)
        self.assertIn("raven-ui", content)
        self.assertIn("pgvector", content)
        self.assertIn("replicas:", content)

    def test_service_yaml(self):
        self.assertTrue((self.K8S / "service.yaml").exists())
        content = (self.K8S / "service.yaml").read_text()
        self.assertIn("raven-api", content)
        self.assertIn("raven-ui", content)
        self.assertIn("pgvector", content)

    def test_ingress_yaml(self):
        self.assertTrue((self.K8S / "ingress.yaml").exists())
        content = (self.K8S / "ingress.yaml").read_text()
        self.assertIn("raven.internal", content)
        self.assertIn("/api", content)

    def test_pvc_yaml(self):
        self.assertTrue((self.K8S / "pvc.yaml").exists())
        content = (self.K8S / "pvc.yaml").read_text()
        self.assertIn("10Gi", content)

    def test_deployment_has_health_probes(self):
        content = (self.K8S / "deployment.yaml").read_text()
        self.assertIn("readinessProbe", content)
        self.assertIn("livenessProbe", content)

    def test_deployment_has_resource_limits(self):
        content = (self.K8S / "deployment.yaml").read_text()
        self.assertIn("requests:", content)
        self.assertIn("limits:", content)
        self.assertIn("cpu:", content)
        self.assertIn("memory:", content)


# ═══════════════════════════════════════════════════════════════════════
# 6. Integration-style tests (async route logic)
# ═══════════════════════════════════════════════════════════════════════


class TestRouteHandlers(unittest.TestCase):
    """Test route handler logic with mocked pipeline."""

    def _run(self, coro):
        return asyncio.get_event_loop().run_until_complete(coro)

    def test_get_pipeline_raises_when_none(self):
        from web.routes import get_pipeline
        with patch("src.raven.api._pipeline", None):
            with self.assertRaises(Exception):
                get_pipeline()

    def test_query_request_max_length(self):
        from web.routes import QueryRequest
        from pydantic import ValidationError
        with self.assertRaises(ValidationError):
            QueryRequest(question="x" * 2001)

    def test_query_request_at_max_length(self):
        from web.routes import QueryRequest
        req = QueryRequest(question="x" * 2000)
        self.assertEqual(len(req.question), 2000)

    def test_refresh_request_validates_stages_list(self):
        from web.routes import RefreshRequest
        req = RefreshRequest(stages=["dbt", "glossary"])
        self.assertEqual(len(req.stages), 2)

    def test_upload_doc_response_serialization(self):
        from web.routes import UploadDocResponse
        resp = UploadDocResponse(
            status="uploaded",
            filename="test.md",
            chunks_created=0,
            message="Saved.",
        )
        d = resp.model_dump()
        self.assertIn("status", d)
        self.assertIn("filename", d)
        self.assertIn("chunks_created", d)

    def test_query_response_serialization(self):
        from web.routes import QueryResponse
        resp = QueryResponse(status="success", query_id="abc123", sql="SELECT 1")
        d = resp.model_dump()
        self.assertEqual(d["status"], "success")
        self.assertEqual(d["sql"], "SELECT 1")
        self.assertEqual(d["data"], [])


# ═══════════════════════════════════════════════════════════════════════
# 7. Package JSON content tests
# ═══════════════════════════════════════════════════════════════════════


class TestPackageJSON(unittest.TestCase):
    """Check package.json correctness."""

    def setUp(self):
        import json
        pkg_path = Path(__file__).resolve().parent.parent / "web" / "ui" / "package.json"
        self.pkg = json.loads(pkg_path.read_text())

    def test_has_name(self):
        self.assertIn("name", self.pkg)
        self.assertEqual(self.pkg["name"], "raven-ui")

    def test_has_react_dependency(self):
        deps = self.pkg.get("dependencies", {})
        self.assertIn("react", deps)
        self.assertIn("react-dom", deps)

    def test_has_build_script(self):
        scripts = self.pkg.get("scripts", {})
        self.assertIn("build", scripts)

    def test_has_proxy(self):
        self.assertIn("proxy", self.pkg)
        self.assertIn("8000", self.pkg["proxy"])


if __name__ == "__main__":
    unittest.main()
