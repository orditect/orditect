"""Meta test: internal orditect-* dependency floors must match the package's
own version (v0.1.6, issue #7).

Prevents the stream/bridge drift where pyproject kept >=0.1.4 floors while the
CHANGELOG declared >=0.1.5 — published metadata must never diverge from the
declared change set.
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PACKAGES = ROOT / "packages"
_DEP_RE = re.compile(r"(orditect-[\w-]+)>=(\d+\.\d+\.\d+)")


def _internal_deps(data: dict) -> list[str]:
    project = data.get("project", {})
    deps = list(project.get("dependencies", []))
    for extra in project.get("optional-dependencies", {}).values():
        deps.extend(extra)
    return deps


def test_internal_dependency_floors_match_package_versions():
    for pkg_dir in sorted(p for p in PACKAGES.iterdir() if (p / "pyproject.toml").is_file()):
        data = tomllib.loads((pkg_dir / "pyproject.toml").read_text(encoding="utf-8"))
        version = data["project"]["version"]
        for dep in _internal_deps(data):
            match = _DEP_RE.match(dep)
            if match:
                assert match.group(2) == version, (
                    f"{pkg_dir.name}: '{dep}' floor != own version {version}"
                )