"""
Metabase MCP Client
====================
Client for Cognition AI's @cognitionai/metabase-mcp-server.
Communicates via stdio JSON-RPC protocol with a subprocess.

Replaces the direct REST-based MetabaseClient with 80+ pre-built
MCP tools for full Metabase lifecycle management.

Falls back to the legacy MetabaseClient if MCP server is unavailable.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import uuid
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


# ── Configuration ─────────────────────────────────────────────────────

@dataclass
class MetabaseMCPConfig:
    """Configuration for Metabase MCP server."""
    url: str = ""                             # https://metabase.pw.live
    api_key: str = ""                         # mb_xxxxxxx
    session_id: str = ""                      # Optional browser session override

    mcp_server: str = "@cognitionai/metabase-mcp-server"
    mcp_args: list[str] = field(default_factory=lambda: ["--all"])

    # Defaults for card/dashboard creation
    default_database_id: int = 1              # Trino database ID in Metabase
    default_collection_name: str = "RAVEN Generated"

    # Visualization type mapping
    visualization_mapping: dict[str, str] = field(default_factory=lambda: {
        "number_card": "scalar",
        "line_chart": "line",
        "bar_chart": "bar",
        "horizontal_bar": "row",
        "pie_chart": "pie",
        "scatter": "scatter",
        "table": "table",
        "TABLE": "table",
        "LINE": "line",
        "BAR": "bar",
        "PIE": "pie",
        "SCATTER": "scatter",
        "AREA": "area",
    })

    @classmethod
    def from_env(cls) -> MetabaseMCPConfig:
        return cls(
            url=os.getenv("METABASE_URL", ""),
            api_key=os.getenv("METABASE_API_KEY", ""),
            session_id=os.getenv("METABASE_SESSION_ID", ""),
        )

    @classmethod
    def from_yaml(cls, path: str) -> MetabaseMCPConfig:
        import yaml
        if not os.path.exists(path):
            return cls.from_env()
        with open(path) as f:
            raw = yaml.safe_load(f) or {}
        mb = raw.get("metabase", {})

        def resolve(val: str) -> str:
            if isinstance(val, str) and val.startswith("${") and val.endswith("}"):
                return os.getenv(val[2:-1], "")
            return val or ""

        defaults = mb.get("defaults", {})
        viz_map = mb.get("visualization_mapping", {})

        return cls(
            url=resolve(mb.get("url", "")),
            api_key=resolve(mb.get("api_key", "")),
            session_id=resolve(mb.get("session_id", "")),
            mcp_server=mb.get("mcp_server", "@cognitionai/metabase-mcp-server"),
            mcp_args=mb.get("mcp_args", ["--all"]),
            default_database_id=defaults.get("database_id", 1),
            default_collection_name=defaults.get("collection_name", "RAVEN Generated"),
            visualization_mapping=viz_map if viz_map else cls().visualization_mapping,
        )

    @property
    def is_configured(self) -> bool:
        return bool(self.url and (self.api_key or self.session_id))


def _parse_tables_from_sql(sql: str) -> list[str]:
    """Best-effort extraction of table names from SQL."""
    pattern = r'(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_.]*)'
    matches = re.findall(pattern, sql, re.IGNORECASE)
    return list(dict.fromkeys(matches))


# ── Main Client ──────────────────────────────────────────────────────

class MetabaseMCPClient:
    """
    Client for Cognition AI's Metabase MCP server.
    Runs as a subprocess, communicates via stdio JSON-RPC.

    Falls back to direct REST API if MCP server is unavailable.
    """

    def __init__(self, config: MetabaseMCPConfig | None = None):
        self.config = config or MetabaseMCPConfig.from_env()
        self._process: asyncio.subprocess.Process | None = None
        self._started = False
        self._lock = asyncio.Lock()
        self._request_id = 0

    @property
    def is_configured(self) -> bool:
        return self.config.is_configured

    async def start(self) -> bool:
        """Start the MCP server subprocess."""
        if self._started:
            return True
        if not self.is_configured:
            logger.warning("Metabase MCP not configured — missing URL or API key")
            return False

        async with self._lock:
            if self._started:
                return True
            try:
                env = {**os.environ}
                env["METABASE_URL"] = self.config.url
                if self.config.api_key:
                    env["METABASE_API_KEY"] = self.config.api_key
                if self.config.session_id:
                    env["METABASE_SESSION_ID"] = self.config.session_id

                self._process = await asyncio.create_subprocess_exec(
                    "npx", self.config.mcp_server, *self.config.mcp_args,
                    env=env,
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                self._started = True
                logger.info("Metabase MCP server started (pid=%d)", self._process.pid)

                # Wait briefly for initialization
                await asyncio.sleep(1)
                return True
            except FileNotFoundError:
                logger.warning(
                    "npx not found — install Node.js to use Metabase MCP. "
                    "Falling back to legacy REST client."
                )
                return False
            except Exception as exc:
                logger.warning("Failed to start Metabase MCP server: %s", exc)
                return False

    async def stop(self):
        """Stop the MCP server subprocess."""
        if self._process:
            try:
                self._process.terminate()
                await asyncio.wait_for(self._process.wait(), timeout=5)
            except Exception:
                self._process.kill()
            self._process = None
            self._started = False
            logger.info("Metabase MCP server stopped")

    async def _ensure_started(self) -> bool:
        """Ensure the subprocess is running."""
        if not self._started:
            return await self.start()
        # Check if process is still alive
        if self._process and self._process.returncode is not None:
            logger.warning("Metabase MCP server died (rc=%d), restarting", self._process.returncode)
            self._started = False
            return await self.start()
        return True

    async def call_tool(self, tool_name: str, arguments: dict) -> dict | list | None:
        """Call an MCP tool via stdio JSON-RPC."""
        if not await self._ensure_started():
            logger.debug("Metabase MCP not available, skipping %s", tool_name)
            return None

        self._request_id += 1
        request = json.dumps({
            "jsonrpc": "2.0",
            "method": "tools/call",
            "params": {"name": tool_name, "arguments": arguments},
            "id": str(self._request_id),
        })

        try:
            async with self._lock:
                self._process.stdin.write(f"{request}\n".encode())
                await self._process.stdin.drain()

                # Read response with timeout
                raw = await asyncio.wait_for(
                    self._process.stdout.readline(), timeout=30
                )
                if not raw:
                    logger.warning("Metabase MCP returned empty response for %s", tool_name)
                    return None

                response = json.loads(raw)
                if "error" in response:
                    logger.warning(
                        "Metabase MCP %s error: %s",
                        tool_name, response["error"]
                    )
                    return None
                return response.get("result", response)

        except asyncio.TimeoutError:
            logger.warning("Metabase MCP %s timed out (30s)", tool_name)
            return None
        except Exception as exc:
            logger.warning("Metabase MCP %s failed: %s", tool_name, exc)
            return None

    # ── Dashboard Operations ──────────────────────────────────────

    async def list_dashboards(self) -> list[dict]:
        """List all non-archived dashboards."""
        result = await self.call_tool("list_dashboards", {})
        if not result:
            return []
        items = result if isinstance(result, list) else result.get("data", [])
        return [
            {
                "id": d.get("id"),
                "name": d.get("name", ""),
                "description": d.get("description", ""),
            }
            for d in items
            if not d.get("archived")
        ]

    async def get_dashboard(self, dashboard_id: int) -> dict | None:
        """Fetch full dashboard details."""
        return await self.call_tool("get_dashboard", {"dashboard_id": dashboard_id})

    async def get_dashboard_cards(self, dashboard_id: int) -> list[dict]:
        """Get all native-query cards from a dashboard (for Focus Mode)."""
        result = await self.get_dashboard(dashboard_id)
        if not result:
            return []
        cards = []
        for dc in result.get("dashcards", result.get("ordered_cards", [])):
            card = dc.get("card", {})
            dq = card.get("dataset_query", {})
            if dq.get("type") == "native":
                sql = dq.get("native", {}).get("query", "")
                cards.append({
                    "card_id": card.get("id"),
                    "name": card.get("name", ""),
                    "sql": sql,
                    "display": card.get("display", "table"),
                    "tables": _parse_tables_from_sql(sql) if sql else [],
                })
        return cards

    async def get_dashboard_meta(self, dashboard_id: int) -> dict:
        """Fetch dashboard metadata (name, filters, owner, database_id)."""
        d = await self.get_dashboard(dashboard_id)
        if not d:
            return {}
        db_id = None
        for dc in d.get("dashcards", d.get("ordered_cards", [])):
            card = dc.get("card", {})
            dq = card.get("dataset_query", {})
            if dq.get("database"):
                db_id = dq["database"]
                break
        return {
            "id": d.get("id"),
            "name": d.get("name", ""),
            "description": d.get("description", ""),
            "filters": d.get("parameters", []),
            "owner": d.get("creator", {}).get("common_name", ""),
            "card_count": len(d.get("dashcards", d.get("ordered_cards", []))),
            "database_id": db_id,
        }

    async def create_dashboard(self, name: str, collection_id: int | None = None,
                                description: str | None = None) -> dict | None:
        """Create a new dashboard."""
        args: dict[str, Any] = {"name": name}
        if collection_id:
            args["collection_id"] = collection_id
        if description:
            args["description"] = description
        return await self.call_tool("create_dashboard", args)

    async def add_card_to_dashboard(self, dashboard_id: int, card_id: int,
                                     row: int = 0, col: int = 0,
                                     size_x: int = 12, size_y: int = 4) -> dict | None:
        """Add a card to a dashboard with positioning."""
        return await self.call_tool("add_card_to_dashboard", {
            "dashboard_id": dashboard_id,
            "card_id": card_id,
            "row": row, "col": col,
            "size_x": size_x, "size_y": size_y,
        })

    # ── Card / Question Operations ────────────────────────────────

    async def create_card(self, name: str, sql: str, display: str,
                          database_id: int | None = None,
                          collection_id: int | None = None,
                          description: str | None = None) -> dict | None:
        """Create a saved question (native query card)."""
        display = self.config.visualization_mapping.get(display, display)
        db_id = database_id or self.config.default_database_id
        args: dict[str, Any] = {
            "name": name,
            "dataset_query": {
                "type": "native",
                "native": {"query": sql, "template-tags": {}},
                "database": db_id,
            },
            "display": display,
            "description": description or "Generated by RAVEN",
        }
        if collection_id:
            args["collection_id"] = collection_id
        result = await self.call_tool("create_card", args)
        if result and result.get("id"):
            return {
                "id": result["id"],
                "url": f"{self.config.url}/question/{result['id']}",
                "name": name,
            }
        return result

    async def get_card(self, card_id: int) -> dict | None:
        """Fetch a single card/question."""
        result = await self.call_tool("get_card", {"card_id": card_id})
        if not result:
            return None
        sql = ""
        dq = result.get("dataset_query", {})
        if dq.get("type") == "native":
            sql = dq.get("native", {}).get("query", "")
        return {
            "card_id": result.get("id"),
            "name": result.get("name", ""),
            "sql": sql,
            "display": result.get("display", "table"),
            "tables": _parse_tables_from_sql(sql) if sql else [],
            "description": result.get("description", ""),
        }

    async def update_card(self, card_id: int, updates: dict) -> dict | None:
        """Update an existing card."""
        args = {"card_id": card_id, **updates}
        return await self.call_tool("update_card", args)

    async def execute_card(self, card_id: int,
                           parameters: dict | None = None) -> dict | None:
        """Execute a card's query and return results."""
        args: dict[str, Any] = {"card_id": card_id}
        if parameters:
            args["parameters"] = parameters
        return await self.call_tool("execute_card", args)

    async def list_cards(self) -> list[dict]:
        """List all saved questions/cards."""
        result = await self.call_tool("list_cards", {})
        if not result:
            return []
        return result if isinstance(result, list) else result.get("data", [])

    # ── Collection Operations ─────────────────────────────────────

    async def list_collections(self) -> list[dict]:
        """List all non-archived collections."""
        result = await self.call_tool("list_collections", {})
        if not result:
            return []
        items = result if isinstance(result, list) else result.get("data", [])
        return [
            {"id": c.get("id"), "name": c.get("name", "")}
            for c in items
            if not c.get("archived")
        ]

    async def get_collection_items(self, collection_id: int) -> list[dict]:
        """Get all cards in a collection."""
        result = await self.call_tool("get_collection_items", {
            "collection_id": collection_id, "models": ["card"]
        })
        if not result:
            return []
        items = result if isinstance(result, list) else result.get("data", [])
        results = []
        for item in items:
            if item.get("model") == "card" or item.get("id"):
                try:
                    card = await self.get_card(item["id"])
                    if card and card.get("sql"):
                        results.append(card)
                except Exception:
                    continue
        return results

    # ── Database / Schema Operations ──────────────────────────────

    async def list_databases(self) -> list[dict]:
        """List available databases."""
        result = await self.call_tool("list_databases", {})
        if not result:
            return []
        return result if isinstance(result, list) else result.get("data", [])

    async def get_database_schema(self, database_id: int) -> dict | None:
        """Get schema for a database."""
        return await self.call_tool("get_database_schema", {
            "database_id": database_id
        })

    async def execute_sql(self, database_id: int, sql: str) -> dict | None:
        """Execute raw SQL against a Metabase database."""
        return await self.call_tool("execute_sql", {
            "database_id": database_id, "query": sql
        })

    # ── Search ────────────────────────────────────────────────────

    async def search(self, query: str) -> list[dict]:
        """Search across all Metabase entities."""
        result = await self.call_tool("search", {"query": query})
        if not result:
            return []
        return result if isinstance(result, list) else result.get("data", [])

    # ── Admin ─────────────────────────────────────────────────────

    async def get_activity(self) -> list[dict]:
        """Get recent Metabase activity."""
        result = await self.call_tool("get_activity", {})
        if not result:
            return []
        return result if isinstance(result, list) else result.get("data", [])

    async def test_connection(self) -> dict:
        """Test MCP server connectivity."""
        try:
            if not await self._ensure_started():
                return {"connected": False, "error": "MCP server not started"}
            dashboards = await self.list_dashboards()
            return {
                "connected": True,
                "mcp": True,
                "dashboards": len(dashboards),
            }
        except Exception as exc:
            return {"connected": False, "error": str(exc)}

    # ── Dashboard Builder (Composite) ─────────────────────────────

    async def build_dashboard(self, name: str, cards: list[dict],
                               collection_id: int | None = None,
                               description: str | None = None) -> dict | None:
        """
        Create a dashboard with multiple cards laid out in a 2-column grid.

        Each card dict should have: name, sql, display.
        """
        dashboard = await self.create_dashboard(name, collection_id, description)
        if not dashboard or not dashboard.get("id"):
            return None
        dashboard_id = dashboard["id"]

        created_cards = []
        for i, card_def in enumerate(cards):
            card = await self.create_card(
                name=card_def["name"],
                sql=card_def["sql"],
                display=card_def.get("display", "table"),
                collection_id=collection_id,
                description=card_def.get("description"),
            )
            if card and card.get("id"):
                row = (i // 2) * 4
                col = (i % 2) * 9
                await self.add_card_to_dashboard(
                    dashboard_id, card["id"],
                    row=row, col=col, size_x=9, size_y=4
                )
                created_cards.append(card)

        return {
            "id": dashboard_id,
            "url": f"{self.config.url}/dashboard/{dashboard_id}",
            "name": name,
            "card_count": len(created_cards),
            "cards": created_cards,
        }
