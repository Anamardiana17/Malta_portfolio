from __future__ import annotations

from typing import Any

import yaml

from services.repo_paths import resolve_repo_path


def _load_yaml(relative_path: str) -> dict[str, Any]:
    path = resolve_repo_path(relative_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data


def load_schema_registry() -> dict[str, Any]:
    return _load_yaml("apps/gui_control_panel/config/schema_registry.yaml")


def load_column_alias_registry() -> dict[str, Any]:
    return _load_yaml("apps/gui_control_panel/config/column_alias_registry.yaml")


def get_supported_dataset_types() -> list[str]:
    registry = load_schema_registry()
    dataset_types = registry.get("dataset_types", {})
    return sorted(dataset_types.keys())


def get_required_columns(dataset_type: str) -> list[str]:
    registry = load_schema_registry()
    dataset_cfg = registry.get("dataset_types", {}).get(dataset_type, {})
    return dataset_cfg.get("required_columns", [])
