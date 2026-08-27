#!/usr/bin/env python3
"""Business-vocabulary neutrality gate for orditect-protocol (data criterion).

Gate positions (any hit exits 1):
  G1  class attribute names and default values (field names, enum members,
      CapabilitySet flags) — exact match against the banned set
  G2  class names — CamelCase segments matched against the banned set
      (catches e.g. an error class named PendingError)
  G3  banned string literals at behavioral positions: ast.Compare nodes and
      function parameter defaults (assignment/call-argument positions are
      data passing, not behavior coupling, and are intentionally NOT scanned)

Exemption: the conformance/ subpackage is fully exempt — its
"passed"/"failed"/"skipped" strings are test-outcome vocabulary and its
"done"/"running" values are deliberately opaque fixtures proving the
protocol embeds no vocabulary (the T3 tests need arbitrary words). The
exemption is itself living evidence of T6.

Advisory (never affects the exit code): docstring hits in scanned modules
and banned-word hits in packages/protocol/**/*.md, written to
vocabulary-advisory.txt at the repo root for review-data accumulation.

Run: python scripts/gates/check_business_neutrality.py
Exit 0 = clean; 1 = gate hit.
"""

from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

from common import (
    PACKAGES,
    iter_python_files,
    parse_python,
    rel_posix,
    repo_root,
)
from vocab import ALL_BANNED

REPORT_NAME = "vocabulary-advisory.txt"

_BANNED_RE = re.compile(
    r"\b(" + "|".join(re.escape(w) for w in sorted(ALL_BANNED)) + r")\b",
    re.IGNORECASE,
)
_CAMEL_RE = re.compile(r"[A-Z]+(?=[A-Z][a-z])|[A-Z][a-z0-9]*|[A-Z]+")


def _camel_segments(name: str) -> list[str]:
    """Split a CamelCase name into lowercase word segments."""
    return [m.group(0).lower() for m in _CAMEL_RE.finditer(name)]


class SurfaceScan(ast.NodeVisitor):
    """AST scan of the gate positions (G1/G2/G3) on one module."""

    def __init__(self, rel: str):
        self.rel = rel
        self.findings: list[str] = []

    def _check_word(self, lineno: int, kind: str, word: object) -> None:
        if isinstance(word, str) and word.lower() in ALL_BANNED:
            self.findings.append(f"{self.rel}:{lineno}: [{kind}] {word!r}")

    # ----- G1: attribute names / defaults; G2: class names -----
    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        for segment in _camel_segments(node.name):
            self._check_word(node.lineno, "class name segment", segment)
        for stmt in node.body:
            target: str | None = None
            value: ast.expr | None = None
            if isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name):
                target, value = stmt.target.id, stmt.value
            elif (
                isinstance(stmt, ast.Assign)
                and len(stmt.targets) == 1
                and isinstance(stmt.targets[0], ast.Name)
            ):
                target, value = stmt.targets[0].id, stmt.value
            if target is None:
                continue
            self._check_word(stmt.lineno, "attribute name", target)
            self._check_default(stmt.lineno, value)
        self.generic_visit(node)

    def _check_default(self, lineno: int, value: ast.expr | None) -> None:
        if value is None:
            return
        if isinstance(value, ast.Constant) and isinstance(value.value, str):
            self._check_word(lineno, "default value", value.value)
            return
        if isinstance(value, ast.Call):
            for kw in value.keywords:
                if (
                    kw.arg == "default"
                    and isinstance(kw.value, ast.Constant)
                    and isinstance(kw.value.value, str)
                ):
                    self._check_word(lineno, "default value", kw.value.value)
                if kw.arg == "default_factory" and isinstance(kw.value, ast.Name):
                    self._check_word(lineno, "default factory", kw.value.id)

    # ----- G3: behavioral positions -----
    def visit_Compare(self, node: ast.Compare) -> None:
        for part in (node.left, *node.comparators):
            if isinstance(part, ast.Constant) and isinstance(part.value, str):
                self._check_word(node.lineno, "behavioral compare", part.value)
        self.generic_visit(node)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._check_param_defaults(node)
        self.generic_visit(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._check_param_defaults(node)
        self.generic_visit(node)

    def _check_param_defaults(
        self, node: ast.FunctionDef | ast.AsyncFunctionDef
    ) -> None:
        for default in (*node.args.defaults, *node.args.kw_defaults):
            if isinstance(default, ast.Constant) and isinstance(default.value, str):
                self._check_word(node.lineno, "parameter default", default.value)


def _advisory_docstrings(rel: str, tree: ast.Module) -> list[str]:
    """Collect banned-word hits inside docstrings of one module (advisory)."""
    hits: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef,
                             ast.FunctionDef, ast.AsyncFunctionDef)):
            doc = ast.get_docstring(node, clean=False)
            if not doc:
                continue
            # ast.Module nodes carry no lineno; report module docstrings at line 1.
            lineno = getattr(node, "lineno", 1)
            for match in _BANNED_RE.finditer(doc):
                hits.append(
                    f"{rel}:{lineno}: [docstring] {match.group(0)!r}"
                )
    return hits


def _advisory_markdown(root: Path, path: Path) -> list[str]:
    """Collect banned-word hits in one Markdown file (advisory)."""
    rel = rel_posix(root, path)
    hits: list[str] = []
    for lineno, line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), start=1
    ):
        for match in _BANNED_RE.finditer(line):
            hits.append(f"{rel}:{lineno}: [markdown] {match.group(0)!r}")
    return hits


def main() -> int:
    root = repo_root()
    proto = PACKAGES["protocol"]
    src = root / str(proto["path"]) / "src" / "orditect" / "protocol"
    if not src.is_dir():
        print(f"error: protocol source not found: {src}", file=sys.stderr)
        return 1

    findings: list[str] = []
    advisory: list[str] = []

    for path in iter_python_files(src):
        if "conformance" in path.parts:
            # Exempted: conformance content is test-fixture vocabulary, not
            # contract surface (see module docstring).
            continue
        rel = rel_posix(root, path)
        tree = parse_python(path)
        scan = SurfaceScan(rel)
        scan.visit(tree)
        findings.extend(scan.findings)
        advisory.extend(_advisory_docstrings(rel, tree))

    for md in sorted((root / str(proto["path"])).rglob("*.md")):
        advisory.extend(_advisory_markdown(root, md))

    report = root / REPORT_NAME
    report.write_text(
        "# Vocabulary advisory report (informational only, never gates CI)\n"
        "# Regenerated by scripts/gates/check_business_neutrality.py\n"
        + "\n".join(advisory)
        + ("\n" if advisory else ""),
        encoding="utf-8",
    )

    if findings:
        print("business-neutrality gate FAILED:")
        for finding in findings:
            print(f"  - {finding}")
        print(f"(advisory: {len(advisory)} non-gating hits -> {REPORT_NAME})")
        return 1

    print(
        f"business-neutrality gate OK: contract surface clean; "
        f"{len(advisory)} advisory hits -> {REPORT_NAME}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())