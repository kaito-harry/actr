from __future__ import annotations

from typing import Any

from .constants import (
    COMPONENTIZE_PY_VERSION,
    DEFAULT_WIT_RESOURCE,
    DEFAULT_WORLD,
    DEFAULT_WORLD_MODULE,
    ENV_WIT_PATH,
    REPO_WIT_PATH,
)
from .protocol import Workload


def __getattr__(name: str) -> Any:
    if name in {"componentize", "generate_bindings"}:
        from . import build

        return getattr(build, name)
    if name in {"packaged_wit", "repo_wit", "resolved_wit_path"}:
        from . import paths

        return getattr(paths, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "COMPONENTIZE_PY_VERSION",
    "DEFAULT_WIT_RESOURCE",
    "DEFAULT_WORLD",
    "DEFAULT_WORLD_MODULE",
    "ENV_WIT_PATH",
    "REPO_WIT_PATH",
    "Workload",
    "componentize",
    "generate_bindings",
    "packaged_wit",
    "repo_wit",
    "resolved_wit_path",
]
