"""Environment-driven settings (loads .env if present)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")


@dataclass(frozen=True)
class Settings:
    llm_base_url: str = os.getenv("LLM_BASE_URL", "http://localhost:11434/v1")
    llm_api_key: str = os.getenv("LLM_API_KEY", "ollama")
    llm_model: str = os.getenv("LLM_MODEL", "qwen2.5:7b")
    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    llm_sem_limit: int = int(os.getenv("LLM_SEM_LIMIT", "30"))
    budget_max_units: int = int(os.getenv("BUDGET_MAX_UNITS", "100000"))


SETTINGS = Settings()