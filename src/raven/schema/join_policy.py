"""
Deterministic join-policy resolution using semantic contracts first.
"""

from __future__ import annotations

from collections import deque
from dataclasses import asdict, dataclass
from typing import Any

from ..semantic_assets import RelationshipAsset, SemanticModelStore


def _join_condition(
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


@dataclass(frozen=True)
class JoinEdge:
    left_table: str
    right_table: str
    condition_sql: str
    source: str
    notes: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class JoinPolicy:
    """Resolve approved join paths from semantic relationships."""

    def __init__(self, semantic_store: SemanticModelStore, graph: Any = None):
        self.semantic_store = semantic_store
        self._graph = graph

    def set_graph(self, graph: Any) -> None:
        self._graph = graph

    def find_path(
        self,
        left_table: str,
        right_table: str,
        *,
        available_tables: set[str] | None = None,
    ) -> list[JoinEdge]:
        left = self.semantic_store.resolve_table_name(left_table, candidates=available_tables)
        right = self.semantic_store.resolve_table_name(right_table, candidates=available_tables)
        if left == right:
            return []

        explicit = self._find_explicit_path(left, right, available_tables=available_tables)
        if explicit is not None:
            return explicit
        return self._find_graph_path(left, right, available_tables=available_tables)

    def connect_tables(
        self,
        tables: list[str],
        *,
        available_tables: set[str] | None = None,
    ) -> tuple[list[str], list[dict[str, Any]]]:
        resolved_tables = [
            self.semantic_store.resolve_table_name(table, candidates=available_tables)
            for table in tables
            if table
        ]
        resolved_tables = list(dict.fromkeys(resolved_tables))
        if not resolved_tables:
            return [], []
        if len(resolved_tables) == 1:
            return resolved_tables, []

        connected_tables = [resolved_tables[0]]
        join_paths: list[dict[str, Any]] = []
        for target in resolved_tables[1:]:
            best_path: list[JoinEdge] | None = None
            best_anchor = connected_tables[0]
            for anchor in connected_tables:
                path = self.find_path(anchor, target, available_tables=available_tables)
                if not path:
                    continue
                if best_path is None or len(path) < len(best_path):
                    best_path = path
                    best_anchor = anchor
            if not best_path:
                return [], []

            path_tables = [best_path[0].left_table]
            for edge in best_path:
                path_tables.append(edge.right_table)
            for table in path_tables:
                if table not in connected_tables:
                    connected_tables.append(table)
            join_paths.append(
                {
                    "from_table": best_anchor,
                    "to_table": target,
                    "path": path_tables,
                    "edges": [edge.to_dict() for edge in best_path],
                    "source": best_path[0].source,
                }
            )

        return connected_tables, join_paths

    def _find_explicit_path(
        self,
        left_table: str,
        right_table: str,
        *,
        available_tables: set[str] | None = None,
    ) -> list[JoinEdge] | None:
        adjacency: dict[str, list[tuple[str, RelationshipAsset, bool]]] = {}
        for relationship in self.semantic_store.relationship_assets:
            left = self.semantic_store.resolve_table_name(
                relationship.left_table,
                candidates=available_tables,
            )
            right = self.semantic_store.resolve_table_name(
                relationship.right_table,
                candidates=available_tables,
            )
            adjacency.setdefault(left, []).append((right, relationship, False))
            adjacency.setdefault(right, []).append((left, relationship, True))

        if left_table not in adjacency or right_table not in adjacency:
            return None

        queue: deque[tuple[str, list[JoinEdge]]] = deque([(left_table, [])])
        seen = {left_table}

        while queue:
            current, path = queue.popleft()
            if current == right_table:
                return path

            for neighbor, relationship, reversed_edge in adjacency.get(current, []):
                if neighbor in seen:
                    continue
                seen.add(neighbor)
                queue.append(
                    (
                        neighbor,
                        path + [self._relationship_to_edge(relationship, reversed_edge, available_tables)],
                    )
                )
        return None

    def _find_graph_path(
        self,
        left_table: str,
        right_table: str,
        *,
        available_tables: set[str] | None = None,
    ) -> list[JoinEdge]:
        if not self._graph:
            return []

        import networkx as nx

        candidates = set(available_tables or set()) | set(self._graph.nodes)
        left = self.semantic_store.resolve_table_name(left_table, candidates=candidates)
        right = self.semantic_store.resolve_table_name(right_table, candidates=candidates)

        try:
            path = nx.shortest_path(self._graph, left, right)
        except (nx.NetworkXNoPath, nx.NodeNotFound):
            return []

        edges: list[JoinEdge] = []
        for idx in range(len(path) - 1):
            edge_data = self._graph.get_edge_data(path[idx], path[idx + 1]) or {}
            left_key = edge_data.get("fk_from", f"{path[idx]}.id")
            right_key = edge_data.get("fk_to", f"{path[idx + 1]}.id")
            edges.append(
                JoinEdge(
                    left_table=path[idx],
                    right_table=path[idx + 1],
                    condition_sql=f"{left_key} = {right_key}",
                    source="local_graph",
                )
            )
        return edges

    def _relationship_to_edge(
        self,
        relationship: RelationshipAsset,
        reversed_edge: bool,
        available_tables: set[str] | None,
    ) -> JoinEdge:
        if reversed_edge:
            left_table = self.semantic_store.resolve_table_name(
                relationship.right_table,
                candidates=available_tables,
            )
            right_table = self.semantic_store.resolve_table_name(
                relationship.left_table,
                candidates=available_tables,
            )
            left_column = relationship.right_column
            right_column = relationship.left_column
        else:
            left_table = self.semantic_store.resolve_table_name(
                relationship.left_table,
                candidates=available_tables,
            )
            right_table = self.semantic_store.resolve_table_name(
                relationship.right_table,
                candidates=available_tables,
            )
            left_column = relationship.left_column
            right_column = relationship.right_column

        return JoinEdge(
            left_table=left_table,
            right_table=right_table,
            condition_sql=_join_condition(
                left_table=left_table,
                left_column=left_column,
                right_table=right_table,
                right_column=right_column,
                cast_required=relationship.cast_required,
                cast_type=relationship.cast_type,
            ),
            source=relationship.source,
            notes=relationship.notes,
        )
