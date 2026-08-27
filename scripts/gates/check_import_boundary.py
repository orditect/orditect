#!/usr/bin/env python3
"""Import-boundary gate for the Orditect framework packages (dependency criterion).

Rules (evaluated in order; any violation exits 1):
  1. business imports — no framework package may import a business/ecosystem
     package (bridge vocabulary flows back through this gate first)
  2. internal imports — importing orditect.* beyond the package itself is
     legal only when the target namespace is in the package's declared
     allowed_internal set (scripts/gates/common.py PACKAGES)
  3. third-party imports — must be declared in the package's pyproject.toml
     (project.dependencies or any optional-dependencies entry)

Notes:
- try/except ImportError guarded imports are legal as long as the target
  itself is allowed (the AST sees the import; the guard is runtime behavior).
- Unregistered packages under packages/ are themselves a violation — the
  registration in common.py forces an explicit architecture decision.
- Declared-but-unused dependencies are NOT checked (that is lint, not boundary).

Run: python scripts/gates/check_import_boundary.py
Exit 0 = clean; 1 = violation.
"""

from __future__ import annotations

import ast
import re
import sys
import tomllib
from pathlib import Path

from common import (
    BUSINESS_IMPORT_BLACKLIST,
    PACKAGES,
    iter_python_files,
    parse_python,
    rel_posix,
    repo_root,
    stdlib_module_names,
)

#: import-name -> distribution-name for the few packages that differ.
_IMPORT_TO_DIST: dict[str, str] = {
    "yaml": "pyyaml",
    "PIL": "pillow",
    "cv2": "opencv-python",
}

_DIST_NAME_RE = re.compile(r"^\s*([A-Za-z0-9._-]+)")


def _normalize_dist(name: str) -> str:
    """Normalize a distribution name (PEP 503-ish, lowercase)."""
    return name.strip().lower().replace("_", "-")


def _declared_third_party(pyproject: Path) -> frozenset[str]:
    """Third-party distribution names declared in one package's pyproject."""
    data = tomllib.loads(pyproject.read_text(encoding="utf-8"))
    project = data.get("project", {})
    names: set[str] = set()
    for dep in project.get("dependencies", []):
        match = _DIST_NAME_RE.match(dep)
        if match:
            names.add(_normalize_dist(match.group(1)))
    for extra_deps in project.get("optional-dependencies", {}).values():
        for dep in extra_deps:
            match = _DIST_NAME_RE.match(dep)
            if match:
                names.add(_normalize_dist(match.group(1)))
    return frozenset(names)


def _classify_import(dotted: str, stdlib: frozenset[str]) -> tuple[str, str]:
    """Classify a dotted import path.

    Returns (category, normalized_target) where category is one of
    "stdlib" / "internal" / "business" / "third_party".
    """
    top = dotted.split(".")[0]
    if top in stdlib:
        return "stdlib", top
    if top in BUSINESS_IMPORT_BLACKLIST:
        return "business", top
    if top == "orditect":
        parts = dotted.split(".")
        if len(parts) >= 2 and parts[1] == "adapter":
            # orditect.adapter.<name> is a two-level namespace portion
            target = ".".join(parts[:3])
        elif len(parts) >= 2 and parts[1] == "bridge":
            # orditect.bridge.<name> is a two-level namespace portion
            target = ".".join(parts[:3])
        else:
            target = ".".join(parts[:2])
        return "internal", target
    return "third_party", top


def _module_imports(tree: ast.Module) -> list[tuple[int, str]]:
    """All top-level import targets (line, dotted-name) of one module."""
    out: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                out.append((node.lineno, alias.name))
        elif isinstance(node, ast.ImportFrom):
            if node.level and node.level > 0:
                continue  # relative import: package-internal by definition
            if node.module:
                out.append((node.lineno, node.module))
    return out


def main() -> int:
    root = repo_root()
    stdlib = stdlib_module_names()

    # Fail fast on unregistered package directories.
    findings: list[str] = []
    packages_dir = root / "packages"
    for child in sorted(packages_dir.iterdir()):
        if child.is_dir() and child.name not in PACKAGES:
            findings.append(
                f"packages/{child.name}: unregistered package directory; "
                f"register it in scripts/gates/common.py PACKAGES"
            )

    for pkg_name, meta in PACKAGES.items():
        pkg_root = root / str(meta["path"])
        namespace = str(meta["namespace"])
        allowed_internal: frozenset[str] = meta["allowed_internal"]  # type: ignore[assignment]

        declared = _declared_third_party(pkg_root / "pyproject.toml")
        declared |= {
            _IMPORT_TO_DIST.get(name, name) for name in declared
        } | declared  # keep both raw and mapped forms for lookup simplicity

        src = pkg_root / "src"
        if not src.is_dir():
            continue
        for path in iter_python_files(src):
            rel = rel_posix(root, path)
            for lineno, dotted in _module_imports(parse_python(path)):
                category, target = _classify_import(dotted, stdlib)

                if category == "business":
                    findings.append(
                        f"{rel}:{lineno}: [business import] {dotted!r} "
                        f"(package {pkg_name!r} must not import business packages)"
                    )
                elif category == "internal":
                    if target == namespace or target in allowed_internal:
                        continue
                    findings.append(
                        f"{rel}:{lineno}: [internal import] {dotted!r} "
                        f"(package {pkg_name!r} may only import "
                        f"{sorted(allowed_internal | {namespace})})"
                    )
                elif category == "third_party":
                    dist = _IMPORT_TO_DIST.get(target, target)
                    if _normalize_dist(dist) not in declared:
                        findings.append(
                            f"{rel}:{lineno}: [third-party import] {dotted!r} "
                            f"not declared in {pkg_name}/pyproject.toml"
                        )

    if findings:
        print("import-boundary gate FAILED:")
        for finding in findings:
            print(f"  - {finding}")
        return 1

    print(f"import-boundary gate OK: {len(PACKAGES)} packages clean")
    return 0


if __name__ == "__main__":
    sys.exit(main())