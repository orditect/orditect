#!/usr/bin/env python3
"""Schema-artifact vocabulary gate (data criterion, publishable surface).

The checked-in JSON Schema artifacts are the PUBLISHABLE contract surface —
their field names, enum values, and format keywords are what the outside
world codes against. This gate scans them for banned vocabulary (same
blacklist as check_business_neutrality.py). Source-side scanning (WI-0.1)
covers the code; this gate covers the artifacts.

Scope: packages/protocol/schemas/**/*.json
Positions: every `properties` key, every `enum` value (exact match).

Run: python scripts/gates/check_schema_vocabulary.py
Exit 0 = clean; 1 = gate hit.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from common import rel_posix, repo_root
from vocab import ALL_BANNED


def _scan_node(node: object, rel: str, path: str, findings: list[str]) -> None:
    """Recursively collect banned property keys and enum values."""
    if isinstance(node, dict):
        properties = node.get("properties")
        if isinstance(properties, dict):
            for key in properties:
                if key.lower() in ALL_BANNED:
                    findings.append(f"{rel}: [property] {path}.{key}")
        enum_values = node.get("enum")
        if isinstance(enum_values, list):
            for value in enum_values:
                if isinstance(value, str) and value.lower() in ALL_BANNED:
                    findings.append(f"{rel}: [enum] {path} = {value!r}")
        for key, value in node.items():
            if key in ("properties", "enum"):
                continue
            _scan_node(value, rel, f"{path}.{key}", findings)
    elif isinstance(node, list):
        for i, item in enumerate(node):
            _scan_node(item, rel, f"{path}[{i}]", findings)


def main() -> int:
    root = repo_root()
    schema_dir = root / "packages" / "protocol" / "schemas"
    if not schema_dir.is_dir():
        print(f"error: schema directory not found: {schema_dir}", file=sys.stderr)
        return 1

    findings: list[str] = []
    files = sorted(schema_dir.rglob("*.json"))
    for path in files:
        rel = rel_posix(root, path)
        artifact = json.loads(path.read_text(encoding="utf-8"))
        _scan_node(artifact, rel, "$", findings)

    if findings:
        print("schema-vocabulary gate FAILED:")
        for finding in findings:
            print(f"  - {finding}")
        return 1

    print(f"schema-vocabulary gate OK: {len(files)} artifacts clean")
    return 0


if __name__ == "__main__":
    sys.exit(main())