"""
Graph Path Finder — Stage 3.2
===============================
Uses a NetworkX table-relationship graph (built during preprocessing)
to discover bridge/junction tables between candidate tables.

Implements the QueryWeaver-inspired graph traversal that ensures
JOIN paths are available for multi-table queries.
"""

from __future__ import annotations

import logging
from itertools import combinations
from typing import Any

logger = logging.getLogger(__name__)


class GraphPathFinder:
    """Discover bridge tables and JOIN paths via NetworkX."""

    def __init__(self, graph: Any = None):
        """
        Args:
            graph: A networkx.Graph where nodes are table FQNs and
                   edges represent foreign-key / JOIN relationships.
        """
        self._graph = graph

    def expand_tables(self, candidate_columns: list[str]) -> list[str]:
        """
        Given candidate columns (table.column refs), extract unique tables
        and inject any bridge tables needed to connect them.

        Returns:
            Expanded list of table FQNs (original + bridge tables).
        """
        candidate_tables = self._extract_tables(candidate_columns)

        if not self._graph or len(candidate_tables) < 2:
            return candidate_tables

        import networkx as nx

        full_set = set(candidate_tables)

        for t1, t2 in combinations(candidate_tables, 2):
            try:
                path = nx.shortest_path(self._graph, t1, t2)
                bridge_tables = [t for t in path if t not in full_set]
                if bridge_tables:
                    logger.debug(
                        "Bridge tables between %s and %s: %s",
                        t1, t2, bridge_tables,
                    )
                full_set.update(path)
            except (nx.NetworkXNoPath, nx.NodeNotFound):
                logger.debug("No graph path between %s and %s", t1, t2)

        expanded = list(full_set)
        logger.info(
            "Graph expansion: %d candidate tables → %d (added %d bridge)",
            len(candidate_tables),
            len(expanded),
            len(expanded) - len(candidate_tables),
        )
        return expanded

    def find_join_paths(self, tables: list[str]) -> list[dict]:
        """
        Find concrete JOIN paths between the given tables.

        Returns:
            [
                {
                    "from_table": "a",
                    "to_table": "b",
                    "path": ["a", "bridge", "b"],
                    "join_keys": [("a.id", "bridge.a_id"), ("bridge.b_id", "b.id")],
                },
                ...
            ]
        """
        if not self._graph:
            return []

        import networkx as nx

        paths: list[dict] = []
        seen_pairs: set[tuple[str, str]] = set()

        for t1, t2 in combinations(tables, 2):
            pair = tuple(sorted([t1, t2]))
            if pair in seen_pairs:
                continue
            seen_pairs.add(pair)

            try:
                path = nx.shortest_path(self._graph, t1, t2)
                # Extract join keys from edge attributes
                join_keys: list[tuple[str, str]] = []
                for i in range(len(path) - 1):
                    edge_data = self._graph.get_edge_data(path[i], path[i + 1]) or {}
                    fk_from = edge_data.get("fk_from", f"{path[i]}.id")
                    fk_to = edge_data.get("fk_to", f"{path[i+1]}.id")
                    join_keys.append((fk_from, fk_to))

                paths.append({
                    "from_table": t1,
                    "to_table": t2,
                    "path": path,
                    "join_keys": join_keys,
                })
            except (nx.NetworkXNoPath, nx.NodeNotFound):
                pass

        return paths

    # ── Helpers ─────────────────────────────────────────────────────────

    @staticmethod
    def _extract_tables(candidate_columns: list[str]) -> list[str]:
        """Extract unique table FQNs from column references."""
        tables: set[str] = set()
        for col in candidate_columns:
            if "." in col:
                # Remove the last segment (column name) to get table FQN
                table = col.rsplit(".", 1)[0]
                tables.add(table)
        return sorted(tables)

    def set_graph(self, graph: Any) -> None:
        """Hot-swap the graph (e.g., after preprocessing refresh)."""
        self._graph = graph
