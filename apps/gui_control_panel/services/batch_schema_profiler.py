from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from services.repo_paths import resolve_repo_path
from services.schema_registry_loader import (
    load_column_alias_registry,
    load_schema_registry,
)


SUPPORTED_PROFILE_EXTENSIONS = {".csv", ".txt"}


@dataclass
class FileProfileResult:
    file_name: str
    file_ext: str
    detected_columns_count: int
    likely_dataset_type: str
    matched_required_columns: int
    total_required_columns: int
    match_score: float
    match_status: str
    missing_required_columns: list[str]
    matched_standard_columns: list[str]


def normalize_column_name(column_name: str) -> str:
    return (
        str(column_name)
        .strip()
        .lower()
        .replace(" ", "_")
        .replace("-", "_")
    )


def build_alias_lookup() -> dict[str, str]:
    alias_registry = load_column_alias_registry().get("column_aliases", {})
    lookup: dict[str, str] = {}

    for standard_col, aliases in alias_registry.items():
        lookup[normalize_column_name(standard_col)] = standard_col
        for alias in aliases:
            lookup[normalize_column_name(alias)] = standard_col

    return lookup


def read_header_columns(file_path: Path) -> list[str]:
    suffix = file_path.suffix.lower()
    if suffix not in SUPPORTED_PROFILE_EXTENSIONS:
        return []

    try:
        df = pd.read_csv(file_path, nrows=0)
        return [str(col) for col in df.columns]
    except Exception:
        return []


def map_to_standard_columns(raw_columns: list[str]) -> list[str]:
    alias_lookup = build_alias_lookup()
    mapped: list[str] = []

    for col in raw_columns:
        normalized = normalize_column_name(col)
        mapped.append(alias_lookup.get(normalized, normalized))

    deduped = []
    seen = set()
    for col in mapped:
        if col not in seen:
            deduped.append(col)
            seen.add(col)
    return deduped


def score_dataset_type(
    standardized_columns: list[str],
    dataset_type: str,
    schema_registry: dict[str, Any],
) -> dict[str, Any]:
    dataset_cfg = schema_registry.get("dataset_types", {}).get(dataset_type, {})
    required_columns = dataset_cfg.get("required_columns", [])

    matched_required = [c for c in required_columns if c in standardized_columns]
    missing_required = [c for c in required_columns if c not in standardized_columns]

    total_required = len(required_columns)
    if total_required == 0:
        score = 0.0
    else:
        score = len(matched_required) / total_required

    if score >= 0.80:
        status = "strong_match"
    elif score >= 0.40:
        status = "partial_match"
    elif score > 0.0:
        status = "weak_match"
    else:
        status = "no_match"

    return {
        "matched_required_columns": len(matched_required),
        "total_required_columns": total_required,
        "match_score": round(score, 4),
        "match_status": status,
        "missing_required_columns": missing_required,
    }


def profile_file(file_path: Path) -> FileProfileResult:
    schema_registry = load_schema_registry()

    raw_columns = read_header_columns(file_path)
    standardized_columns = map_to_standard_columns(raw_columns)

    dataset_types = list(schema_registry.get("dataset_types", {}).keys())

    best_dataset_type = "unknown"
    best_score = -1.0
    best_payload: dict[str, Any] = {
        "matched_required_columns": 0,
        "total_required_columns": 0,
        "match_score": 0.0,
        "match_status": "no_match",
        "missing_required_columns": [],
    }

    for dataset_type in dataset_types:
        payload = score_dataset_type(
            standardized_columns=standardized_columns,
            dataset_type=dataset_type,
            schema_registry=schema_registry,
        )
        score = payload["match_score"]
        if score > best_score:
            best_score = score
            best_dataset_type = dataset_type
            best_payload = payload

    if file_path.suffix.lower() not in SUPPORTED_PROFILE_EXTENSIONS:
        best_dataset_type = "unsupported_file_type"
        best_payload = {
            "matched_required_columns": 0,
            "total_required_columns": 0,
            "match_score": 0.0,
            "match_status": "unsupported",
            "missing_required_columns": [],
        }
    elif not raw_columns:
        best_dataset_type = "unreadable"
        best_payload = {
            "matched_required_columns": 0,
            "total_required_columns": 0,
            "match_score": 0.0,
            "match_status": "unreadable",
            "missing_required_columns": [],
        }
    elif best_payload["match_score"] == 0.0:
        best_dataset_type = "unknown"
        best_payload = {
            "matched_required_columns": 0,
            "total_required_columns": best_payload["total_required_columns"],
            "match_score": 0.0,
            "match_status": "no_match",
            "missing_required_columns": [],
        }

    return FileProfileResult(
        file_name=file_path.name,
        file_ext=file_path.suffix.lower(),
        detected_columns_count=len(raw_columns),
        likely_dataset_type=best_dataset_type,
        matched_required_columns=best_payload["matched_required_columns"],
        total_required_columns=best_payload["total_required_columns"],
        match_score=best_payload["match_score"],
        match_status=best_payload["match_status"],
        missing_required_columns=best_payload["missing_required_columns"],
        matched_standard_columns=standardized_columns,
    )


def profile_batch(batch_id: str) -> pd.DataFrame:
    batch_files_dir = resolve_repo_path(f"data_input/inbox/{batch_id}/files")
    if not batch_files_dir.exists():
        return pd.DataFrame(
            columns=[
                "file_name",
                "file_ext",
                "detected_columns_count",
                "likely_dataset_type",
                "matched_required_columns",
                "total_required_columns",
                "match_score",
                "match_status",
                "missing_required_columns",
                "matched_standard_columns",
            ]
        )

    rows = []
    for file_path in sorted(batch_files_dir.iterdir()):
        if not file_path.is_file():
            continue
        result = profile_file(file_path)
        rows.append(
            {
                "file_name": result.file_name,
                "file_ext": result.file_ext,
                "detected_columns_count": result.detected_columns_count,
                "likely_dataset_type": result.likely_dataset_type,
                "matched_required_columns": result.matched_required_columns,
                "total_required_columns": result.total_required_columns,
                "match_score": result.match_score,
                "match_status": result.match_status,
                "missing_required_columns": ", ".join(result.missing_required_columns),
                "matched_standard_columns": ", ".join(result.matched_standard_columns),
            }
        )

    return pd.DataFrame(rows)
