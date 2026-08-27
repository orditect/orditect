"""Shared helpers for the Orditect gate scripts (stdlib-only).

Every gate script runs on a bare interpreter (no pip install), so everything
here uses only the standard library.
"""

from __future__ import annotations

import ast
import sys
from collections.abc import Iterator
from pathlib import Path

#: Registered first-party packages. A packages/ directory that is not
#: registered here is itself a violation (forces explicit registration).
PACKAGES: dict[str, dict[str, object]] = {
    "protocol": {
        "path": "packages/protocol",
        "namespace": "orditect.protocol",
        "allowed_internal": frozenset(),
    },
    "core": {
        "path": "packages/core",
        "namespace": "orditect.core",
        "allowed_internal": frozenset({"orditect.protocol"}),
    },
    "adapter-memory": {
        "path": "packages/adapter-memory",
        "namespace": "orditect.adapter.memory",
        "allowed_internal": frozenset({"orditect.protocol"}),
    },
    "flow": {
        "path": "packages/flow",
        "namespace": "orditect.flow",
        "allowed_internal": frozenset({"orditect.core", "orditect.protocol"}),
    },
    "stream": {
        "path": "packages/stream",
        "namespace": "orditect.stream",
        "allowed_internal": frozenset({
            "orditect.core", "orditect.flow", "orditect.protocol",
        }),
    },
}

#: Business/ecosystem packages that no framework package may ever import.
#: Bridge-ecosystem vocabulary flows back through this gate first.
BUSINESS_IMPORT_BLACKLIST: frozenset[str] = frozenset({
    "langchain", "langchain_core", "langgraph", "autogen", "pyautogen",
    "deepagent", "openai", "anthropic", "llama_index", "haystack",
})

_SKIP_DIRS = frozenset({
    "tests", "__pycache__", ".git", ".mypy_cache", ".pytest_cache",
})


def repo_root() -> Path:
    """Repository root (parent of scripts/)."""
    return Path(__file__).resolve().parents[2]


def iter_python_files(root: Path) -> Iterator[Path]:
    """Yield .py files under root, skipping tests / caches / egg-info."""
    for path in sorted(root.rglob("*.py")):
        if any(p in _SKIP_DIRS or p.endswith(".egg-info") for p in path.parts):
            continue
        yield path


def parse_python(path: Path) -> ast.Module:
    """Parse a Python file into an AST module."""
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def rel_posix(root: Path, path: Path) -> str:
    """POSIX-style path relative to the repo root, for stable reports."""
    return path.relative_to(root).as_posix()


def stdlib_module_names() -> frozenset[str]:
    """Top-level stdlib module names of the running interpreter."""
    return frozenset(sys.stdlib_module_names)