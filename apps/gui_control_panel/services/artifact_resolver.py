from __future__ import annotations

from typing import Any, Dict

import yaml

from services.repo_paths import resolve_repo_path


REGISTRY_PATH = resolve_repo_path("apps/gui_control_panel/config/artifact_registry.yaml")


def load_registry() -> Dict[str, Any]:
    with open(REGISTRY_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def resolve_artifacts() -> Dict[str, Dict[str, Any]]:
    registry = load_registry().get("artifacts", {})
    resolved: Dict[str, Dict[str, Any]] = {}

    for artifact_name, spec in registry.items():
        active_path = spec.get("active_path")
        fallback_paths = spec.get("fallback_paths", [])

        chosen_path = None
        exists = False

        for candidate in [active_path, *fallback_paths]:
            if not candidate:
                continue

            candidate_path = resolve_repo_path(candidate)
            if candidate_path.exists():
                chosen_path = str(candidate_path)
                exists = True
                break

        resolved[artifact_name] = {
            "panel": spec.get("panel"),
            "role": spec.get("role"),
            "active_path": active_path,
            "resolved_path": chosen_path,
            "exists": exists,
        }

    return resolved
