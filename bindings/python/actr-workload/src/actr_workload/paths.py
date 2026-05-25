from __future__ import annotations

from contextlib import contextmanager
from importlib import resources
from importlib.resources.abc import Traversable
import os
from pathlib import Path
from typing import Iterator

from .constants import DEFAULT_WIT_RESOURCE, ENV_WIT_PATH, REPO_WIT_PATH


def packaged_wit() -> Traversable:
    return resources.files(__package__).joinpath(DEFAULT_WIT_RESOURCE)


def _candidate_repo_wit_paths() -> Iterator[Path]:
    search_roots = [Path.cwd(), Path(__file__).resolve()]
    seen: set[Path] = set()

    for root in search_roots:
        for parent in (root, *root.parents):
            candidate = parent / REPO_WIT_PATH
            if candidate in seen:
                continue
            seen.add(candidate)
            yield candidate


def repo_wit() -> Path | None:
    for candidate in _candidate_repo_wit_paths():
        if candidate.is_file():
            return candidate
    return None


@contextmanager
def resolved_wit_path(wit: str | Path | None = None) -> Iterator[Path]:
    if wit is not None:
        yield Path(wit)
        return

    env_wit = os.environ.get(ENV_WIT_PATH)
    if env_wit:
        yield Path(env_wit)
        return

    repo_wit_path = repo_wit()
    if repo_wit_path is not None:
        yield repo_wit_path
        return

    with resources.as_file(packaged_wit()) as path:
        yield path
