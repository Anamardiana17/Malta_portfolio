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


def list_accepted_batch_ids() -> list[str]:
    accepted_root = resolve_repo_path("data_input/accepted")
    if not accepted_root.exists():
        return []

    batch_ids = sorted(
        [p.name for p in accepted_root.iterdir() if p.is_dir()]
    )
    return batch_ids


def accepted_batch_exists(batch_id: str) -> bool:
    if not batch_id.strip():
        return False
    return resolve_repo_path(f"data_input/accepted/{batch_id.strip()}").is_dir()


def load_processing_history() -> pd.DataFrame:
    path = resolve_repo_path("data_input/registry/processing_history.csv")
    if not path.exists():
        return pd.DataFrame(columns=PROCESSING_HISTORY_HEADER)

    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame(columns=PROCESSING_HISTORY_HEADER)
