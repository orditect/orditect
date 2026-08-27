#!/usr/bin/env python3
"""Error-surface gate (observation non-blocking, T9).

Contract methods may only raise ContractError subclasses — callers wrap sink
writes in try/except and their handling must be total. Two checks:

  E1  every exception named in a contract method's `Raises:` docstring block
      must be a ContractError subclass registered in errors.py
  E2  every public name exported by errors.py must itself be a ContractError
      subclass (or ContractError)

Registered exemptions (intentional, non-negotiable without review):
  - CapabilitySet.supports raises ValueError for an unknown half-domain:
    a programming error made explicit, not a contract-behavior outcome.

Scope: packages/protocol/src/orditect/protocol/domains/*.py + errors.py

Run: python scripts/gates/check_error_surface.py
Exit 0 = clean; 1 = violation.
"""

from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

from common import parse_python, rel_posix, repo_root

_PROTOCOL_SRC = Path("packages/protocol/src/orditect/protocol")

#: (file, method, exception) triples exempt from E1, with reasons.
_E1_EXEMPTIONS: set[tuple[str, str, str]] = set()  # none on contract methods

#: error-class names allowed despite not being ContractError subclasses,
#: {class_name: reason} — applies only where the E1 exemption covers usage.
_ALLOWED_NON_CONTRACT: dict[str, str] = {
    "ValueError": "programming-error explicitness in CapabilitySet.supports "
                  "(not a contract method)",
}

_RAISES_BLOCK_RE = re.compile(
    r"Raises:\s*\n((?:\s+[A-Za-z_][A-Za-z0-9_]*:.*\n?)+)", re.MULTILINE
)
_RAISED_NAME_RE = re.compile(r"^\s+([A-Za-z_][A-Za-z0-9_]*):", re.MULTILINE)


def _contract_error_names(errors_path: Path, root: Path) -> set[str]:
    """All exception class names defined in errors.py (ContractError + subs)."""
    tree = parse_python(errors_path)
    names: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.ClassDef):
            names.add(node.name)
    return names


def _raises_names(docstring: str) -> list[str]:
    """Exception names declared in a docstring's Raises: block."""
    out: list[str] = []
    for block in _RAISES_BLOCK_RE.findall(docstring):
        out.extend(_RAISED_NAME_RE.findall(block))
    return out


def main() -> int:
    root = repo_root()
    src = root / _PROTOCOL_SRC
    errors_path = src / "errors.py"
    contract_names = _contract_error_names(errors_path, root)

    findings: list[str] = []

    # E2: errors.py itself must only define ContractError (+ subclasses) as
    # public exception names.
    for name in sorted(contract_names):
        if name == "ContractError":
            continue
        tree = parse_python(errors_path)
        for node in tree.body:
            if isinstance(node, ast.ClassDef) and node.name == name:
                bases = [
                    b.id for b in node.bases if isinstance(b, ast.Name)
                ]
                if name not in _ALLOWED_NON_CONTRACT and not any(
                    base in contract_names or base == "ContractError"
                    for base in bases
                ):
                    findings.append(
                        f"errors.py: [E2] {name} is not a ContractError subclass"
                    )

    # E1: contract methods' Raises: declarations must be ContractError subs.
    for path in sorted((src / "domains").glob("*.py")):
        rel = rel_posix(root, path)
        tree = parse_python(path)
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            doc = ast.get_docstring(node)
            if not doc:
                continue
            for raised in _raises_names(doc):
                if raised in contract_names:
                    continue
                if (rel, node.name, raised) in _E1_EXEMPTIONS:
                    continue
                if raised in _ALLOWED_NON_CONTRACT:
                    continue
                findings.append(
                    f"{rel}:{node.lineno}: [E1] {node.name} declares "
                    f"{raised} (not a ContractError subclass)"
                )

    if findings:
        print("error-surface gate FAILED:")
        for finding in findings:
            print(f"  - {finding}")
        return 1

    print("error-surface gate OK: contract raises only ContractError subclasses")
    return 0


if __name__ == "__main__":
    sys.exit(main())