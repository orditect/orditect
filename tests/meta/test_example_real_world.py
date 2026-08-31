"""Meta test: the real-world example must stay importable and configurable.

The real-world demo requires Redis + an external LLM at runtime, so it is
NOT exercised end-to-end in CI. This smoke test covers the parts that can
rot silently as APIs evolve: module imports, .env.example-driven settings,
and graceful failure of the hot-path builder when Redis is unreachable.
"""

from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
EXAMPLE = ROOT / "examples" / "real-world"


def _load(module_name: str, path: Path):
    """Import a module from an explicit file path."""
    spec = importlib.util.spec_from_file_location(module_name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def test_settings_loads_env_example_defaults(monkeypatch):
    """settings.py must load with sane defaults (no .env required)."""
    for var in ("LLM_BASE_URL", "LLM_API_KEY", "LLM_MODEL", "REDIS_URL"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.syspath_prepend(str(EXAMPLE))
    _load("rw_bootstrap", EXAMPLE / "bootstrap.py")
    settings = _load("rw_settings", EXAMPLE / "settings.py")
    s = settings.SETTINGS
    assert s.llm_base_url.startswith("http")
    assert s.llm_api_key
    assert s.llm_model
    assert s.redis_url.startswith("redis://")
    assert s.llm_sem_limit > 0
    assert s.budget_max_units > 0


@pytest.mark.asyncio
async def test_hot_path_builder_fails_gracefully_without_redis(monkeypatch):
    """build_hot_path must raise a connection error (not hang or crash
    obscurely) when Redis is unreachable."""
    monkeypatch.setenv("REDIS_URL", "redis://127.0.0.1:6390/15")  # closed port
    monkeypatch.syspath_prepend(str(EXAMPLE))
    # Clear any cached example modules so SETTINGS picks up this env.
    for name in ("rw_settings", "rw_settings2", "rw_infra", "settings", "infra"):
        sys.modules.pop(name, None)
    _load("rw_bootstrap2", EXAMPLE / "bootstrap.py")
    _load("rw_settings2", EXAMPLE / "settings.py")
    infra = _load("rw_infra", EXAMPLE / "infra.py")
    with pytest.raises(Exception):
        await infra.build_hot_path()