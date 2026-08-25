"""Lua script loading (importlib.resources, zip-install compatible)."""
from importlib.resources import files


def load_lua(name: str) -> str:
    """Load script content from package-internal lua/ directory."""
    return files("orditect.core").joinpath("lua").joinpath(name).read_text(encoding="utf-8")