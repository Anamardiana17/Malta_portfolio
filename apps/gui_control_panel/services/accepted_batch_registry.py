from __future__ import annotations

from pathlib import Path

import pandas as pd

from services.repo_paths import resolve_repo_path


PROCESSING_HISTORY_HEADER = [
    "batch_id",
    "processed_at",
    "processing_step",
    "script_name",
    "result_status",
    "output_folder",
    "qa_status",
    "note",
]


def _load_acceptance_review_registry() -> pd.DataFrame:
    path = resolve_repo_path("data_input/registry/acceptance_review_registry.csv")
    if not path.exists():
        return pd.DataFrame()

    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def _accepted_batch_ids_from_review_registry() -> list[str]:
    df = _load_acceptance_review_registry()
    if df.empty:
        return []

    if "batch_id" not in df.columns or "review_outcome" not in df.columns:
        return []

    accepted_df = df[df["review_outcome"].astype(str) == "accepted_manual_review"].copy()
    if accepted_df.empty:
        return []

    batch_ids = (
        accepted_df["batch_id"]
        .dropna()
        .astype(str)
        .str.strip()
        .tolist()
    )

    batch_ids = [batch_id for batch_id in batch_ids if batch_id]
    return sorted(set(batch_ids))


def list_accepted_batch_ids() -> list[str]:
    accepted_root = resolve_repo_path("data_input/accepted")
    folder_batch_ids = []
    if accepted_root.exists():
        folder_batch_ids = sorted(
            [p.name for p in accepted_root.iterdir() if p.is_dir()]
        )

    registry_batch_ids = _accepted_batch_ids_from_review_registry()
    return sorted(set(folder_batch_ids) | set(registry_batch_ids))


def accepted_batch_exists(batch_id: str) -> bool:
    batch_id = str(batch_id or "").strip()
    if not batch_id:
        return False

    if resolve_repo_path(f"data_input/accepted/{batch_id}").is_dir():
        return True

    return batch_id in _accepted_batch_ids_from_review_registry()


def load_processing_history() -> pd.DataFrame:
    path = resolve_repo_path("data_input/registry/processing_history.csv")
    if not path.exists():
        return pd.DataFrame(columns=PROCESSING_HISTORY_HEADER)

    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame(columns=PROCESSING_HISTORY_HEADER)
