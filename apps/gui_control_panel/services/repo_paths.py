from __future__ import annotations

from pathlib import Path


def get_repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def resolve_repo_path(relative_path: str) -> Path:
    return get_repo_root() / relative_path


def path_exists(relative_path: str) -> bool:
    return resolve_repo_path(relative_path).exists()
