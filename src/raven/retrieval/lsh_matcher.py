"""
LSH Matcher — Stage 2.2
========================
Local MinHash Locality-Sensitive Hashing for entity matching.
Maps user-mentioned values (e.g., "Acme Corp") to actual table.column
references without sending data to external APIs.

Uses the datasketch-based LSH index built during preprocessing
(see preprocessing/build_lsh_index.py).
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


class LSHMatcher:
    """Match keywords against a pre-built MinHash LSH index (100 % local)."""

    def __init__(self, lsh_index: Any = None, metadata: dict | None = None):
        """
        Args:
            lsh_index: The datasketch MinHashLSH object built during preprocessing.
            metadata: Dict mapping signature keys → {table, column, value, ...}.
        """
        self._lsh = lsh_index
        self._metadata = metadata or {}

    # ── Public API ──────────────────────────────────────────────────────

    async def match(self, keywords: list[str]) -> list[dict]:
        """
        Match a list of keywords / entities against the LSH index.

        Returns list of dicts:
            [{keyword, table, column, matched_value, similarity}]
        """
        if not self._lsh or not keywords:
            return []

        matches: list[dict] = []
        for keyword in keywords:
            results = self.query(keyword)
            for result in results:
                matches.append({
                    "keyword": keyword,
                    "table": result.get("table", ""),
                    "column": result.get("column", ""),
                    "matched_value": result.get("value"),
                    "similarity": result.get("similarity", 0.0),
                })

        logger.debug(
            "LSH matched %d keywords → %d entity hits", len(keywords), len(matches),
        )
        return matches

    def query(self, keyword: str, top_k: int = 5) -> list[dict]:
        """
        Query the LSH index for a single keyword.

        Returns up to *top_k* matches sorted by estimated similarity.
        """
        if not self._lsh:
            return []

        try:
            from datasketch import MinHash

            # Build a MinHash for the query keyword
            mh = MinHash(num_perm=128)
            for token in self._tokenize(keyword):
                mh.update(token.encode("utf-8"))

            # Query the LSH index for approximate neighbours
            candidate_keys = self._lsh.query(mh)

            results = []
            for key in candidate_keys[:top_k]:
                meta = self._metadata.get(key, {})
                results.append({
                    "table": meta.get("table", ""),
                    "column": meta.get("column", ""),
                    "value": meta.get("value", key),
                    "similarity": meta.get("similarity", 0.8),
                })
            return results

        except Exception as exc:
            logger.warning("LSH query failed for '%s': %s", keyword, exc)
            return []

    # ── Helpers ─────────────────────────────────────────────────────────

    @staticmethod
    def _tokenize(text: str) -> list[str]:
        """Character n-gram tokenization for MinHash (3-grams)."""
        text = text.lower().strip()
        n = 3
        if len(text) < n:
            return [text]
        return [text[i : i + n] for i in range(len(text) - n + 1)]

    def set_index(self, lsh_index: Any, metadata: dict) -> None:
        """Hot-swap the LSH index (e.g., after a preprocessing refresh)."""
        self._lsh = lsh_index
        self._metadata = metadata
