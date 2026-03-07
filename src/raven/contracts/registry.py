"""
Load semantic contracts from a single YAML file or a domain-pack directory.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from .models import ContractBundle, ContractSource


_DIRECTORY_KINDS = {
    "contracts": "contracts",
    "instructions": "business_rules",
    "queries": "verified_queries",
    "verified_queries": "verified_queries",
}


class ContractRegistry:
    """Load and merge semantic contract assets from configurable sources."""

    def __init__(self, root: str | Path):
        self.root = Path(root)

    def load(self) -> ContractBundle:
        if self.root.is_dir():
            return self._load_directory(self.root)
        return self._load_single_file(self.root)

    def _load_single_file(self, path: Path) -> ContractBundle:
        payload = self._read_yaml(path)
        bundle = ContractBundle(
            name=str(payload.get("name", path.stem)),
            description=str(payload.get("description", "")),
            tables=list(payload.get("tables", []) or []),
            business_rules=list(payload.get("business_rules", []) or []),
            verified_queries=list(payload.get("verified_queries", []) or []),
            relationships=list(payload.get("relationships", []) or []),
            metadata=dict(payload.get("metadata", {}) or {}),
            sources=[ContractSource(path=str(path), kind="bundle")],
        )
        return bundle

    def _load_directory(self, root: Path) -> ContractBundle:
        manifest_path = root / "manifest.yaml"
        manifest = self._read_yaml(manifest_path) if manifest_path.exists() else {}
        bundle = ContractBundle(
            name=str(manifest.get("name", root.name)),
            description=str(manifest.get("description", "")),
            metadata=dict(manifest.get("metadata", {}) or {}),
            sources=[ContractSource(path=str(manifest_path), kind="manifest")] if manifest_path.exists() else [],
        )

        if not bundle.description:
            readme_path = root / "README.md"
            if readme_path.exists():
                bundle.description = readme_path.read_text().strip()

        loaded_any = False
        for dirname, kind in _DIRECTORY_KINDS.items():
            subdir = root / dirname
            if not subdir.exists():
                continue
            for path in sorted(subdir.glob("*.y*ml")):
                payload = self._read_yaml(path)
                self._merge_payload(bundle, payload, kind=kind, path=path)
                loaded_any = True

        if not loaded_any:
            for path in sorted(root.glob("*.y*ml")):
                if path.name == "manifest.yaml":
                    continue
                payload = self._read_yaml(path)
                self._merge_payload(bundle, payload, kind="bundle_fragment", path=path)

        return bundle

    @staticmethod
    def _read_yaml(path: Path) -> dict[str, Any]:
        with open(path) as handle:
            return yaml.safe_load(handle) or {}

    @staticmethod
    def _merge_payload(
        bundle: ContractBundle,
        payload: dict[str, Any],
        *,
        kind: str,
        path: Path,
    ) -> None:
        if not bundle.name:
            bundle.name = str(payload.get("name", path.parent.name or path.stem))
        if not bundle.description and payload.get("description"):
            bundle.description = str(payload.get("description", ""))

        bundle.tables.extend(list(payload.get("tables", []) or []))
        bundle.business_rules.extend(list(payload.get("business_rules", []) or []))
        bundle.verified_queries.extend(list(payload.get("verified_queries", []) or []))
        bundle.relationships.extend(list(payload.get("relationships", []) or []))

        metadata = dict(payload.get("metadata", {}) or {})
        if metadata:
            bundle.metadata.update(metadata)

        bundle.sources.append(ContractSource(path=str(path), kind=kind))
