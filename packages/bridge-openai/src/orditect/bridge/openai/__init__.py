"""OpenAI-compatible endpoint bridge (reference implementation)."""

from importlib.metadata import PackageNotFoundError, version as _pkg_version

from orditect.bridge.openai.client import GovernedLLMClient

try:
    __version__ = _pkg_version("orditect-bridge-openai")
except PackageNotFoundError:
    __version__ = "0.0.0.dev0"

__all__ = ["GovernedLLMClient", "__version__"]