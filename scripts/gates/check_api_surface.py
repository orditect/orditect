#!/usr/bin/env python3
"""Contract-surface behavioral gate for orditect-protocol (behavior criterion).

Executable form of the orchestration-independence contract: the protocol's
storage contracts answer "what is stored and how it is retrieved" — they must
never expose "drive the execution" semantics. Two checks (any hit exits 1):

  C1  public function/method names on the contract surface whose head verb is
      an active/orchestration verb (schedule/dispatch/trigger/run/execute/
      submit/cancel/vote/notify/resume/rerun/reopen/pause/suspend/acquire/
      release/claim/ready/poll/dequeue/enqueue)
  C2  model field names that would smuggle readiness/scheduling suggestions
      into the data plane ("ready" / "schedulable" / "next_task" in the name)

Scope: orditect.protocol contract surface only —
  src/orditect/protocol/domains/  models/  capabilities.py  errors.py

Exemptions:
- conformance/ is NOT contract surface (run_conformance's "run" is a test-kit
  verb, not a storage-contract verb) — same rationale as the vocabulary gate.
- rules/ (data-rule toolkit, M4) is NOT contract surface either ("run_rules"
  is a toolkit verb) — it is scope-excluded here in advance so M4 needs no
  gate edit.

Run: python scripts/gates/check_api_surface.py
Exit 0 = clean; 1 = violation.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

from common import PACKAGES, parse_python, rel_posix, repo_root

FORBIDDEN_ACTIVE_VERBS: frozenset[str] = frozenset({
    "schedule", "dispatch", "trigger", "run", "execute", "submit",
    "cancel", "vote", "notify", "resume", "rerun", "reopen",
    "pause", "suspend", "acquire", "release", "claim",
    "ready", "poll", "dequeue", "enqueue",
})

FORBIDDEN_FIELD_HINTS: tuple[str, ...] = ("ready", "schedulable", "next_task")

#: Tooling subpackages that live next to the contract surface but are not
#: part of it (their verbs are toolkit verbs, not storage-contract verbs).
_SCOPE_EXCLUDED_DIRS = frozenset({"conformance", "rules"})


def _is_forbidden_verb(name: str) -> bool:
    """Head-verb or exact match against the forbidden active-verb set."""
    lowered = name.lower()
    if lowered in FORBIDDEN_ACTIVE_VERBS:
        return True
    head = lowered.split("_", 1)[0]
    return head in FORBIDDEN_ACTIVE_VERBS


def _contract_surface_files(root: Path) -> list[Path]:
    proto = PACKAGES["protocol"]
    base = root / str(proto["path"]) / "src" / "orditect" / "protocol"
    files: list[Path] = []
    for sub in ("domains", "models"):
        sub_dir = base / sub
        if sub_dir.is_dir():
            for path in sorted(sub_dir.rglob("*.py")):
                if _SCOPE_EXCLUDED_DIRS & set(path.parts):
                    continue
                files.append(path)
    for single in ("capabilities.py", "errors.py"):
        path = base / single
        if path.is_file():
            files.append(path)
    return files


class _SurfaceScan(ast.NodeVisitor):
    """Collect C1/C2 findings on one contract-surface module."""

    def __init__(self, rel: str):
        self.rel = rel
        self.findings: list[str] = []

    def _check_function(
        self, node: ast.FunctionDef | ast.AsyncFunctionDef
    ) -> None:
        if node.name.startswith("_"):
            return  # private helpers are not contract surface
        if _is_forbidden_verb(node.name):
            self.findings.append(
                f"{self.rel}:{node.lineno}: [active verb] {node.name!r} "
                f"(contract surface must not drive execution)"
            )

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._check_function(node)
        self.generic_visit(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._check_function(node)
        self.generic_visit(node)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        # C2: readiness/scheduling hints must not enter the data plane.
        for stmt in node.body:
            target: str | None = None
            if isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name):
                target = stmt.target.id
            elif (
                isinstance(stmt, ast.Assign)
                and len(stmt.targets) == 1
                and isinstance(stmt.targets[0], ast.Name)
            ):
                target = stmt.targets[0].id
            if target is None:
                continue
            lowered = target.lower()
            for hint in FORBIDDEN_FIELD_HINTS:
                if hint in lowered:
                    self.findings.append(
                        f"{self.rel}:{stmt.lineno}: [scheduling field] "
                        f"{target!r} (readiness is computed, never stored "
                        f"as a data-plane field)"
                    )
        self.generic_visit(node)


def main() -> int:
    root = repo_root()
    findings: list[str] = []

    files = _contract_surface_files(root)
    if not files:
        print("error: no contract-surface files found", file=sys.stderr)
        return 1

    for path in files:
        scan = _SurfaceScan(rel_posix(root, path))
        scan.visit(parse_python(path))
        findings.extend(scan.findings)

    if findings:
        print("api-surface gate FAILED:")
        for finding in findings:
            print(f"  - {finding}")
        return 1

    print(f"api-surface gate OK: {len(files)} contract-surface modules clean")
    return 0


if __name__ == "__main__":
    sys.exit(main())