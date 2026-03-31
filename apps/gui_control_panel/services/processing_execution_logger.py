from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

import pandas as pd


EXECUTION_LOG_COLUMNS = [
    "execution_event_ts",
    "batch_id",
    "processing_step",
    "execution_status",
    "qa_status",
    "output_folder",
    "execution_note",
]


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def get_processing_execution_log_path() -> Path:
    return _repo_root() / "data_input" / "registry" / "processing_execution_log.csv"


def ensure_processing_execution_log() -> Path:
    path = get_processing_execution_log_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        pd.DataFrame(columns=EXECUTION_LOG_COLUMNS).to_csv(path, index=False)
    return path


def load_processing_execution_log() -> pd.DataFrame:
    path = ensure_processing_execution_log()
    try:
        df = pd.read_csv(path)
    except pd.errors.EmptyDataError:
        df = pd.DataFrame(columns=EXECUTION_LOG_COLUMNS)

    for column in EXECUTION_LOG_COLUMNS:
        if column not in df.columns:
            df[column] = ""

    return df[EXECUTION_LOG_COLUMNS].copy()


def append_processing_execution_event(
    *,
    batch_id: str,
    processing_step: str,
    execution_status: str,
    qa_status: str = "",
    output_folder: str = "",
    execution_note: str = "",
) -> Dict[str, Any]:
    path = ensure_processing_execution_log()
    df = load_processing_execution_log()

    event = {
        "execution_event_ts": datetime.now(timezone.utc).isoformat(),
        "batch_id": str(batch_id or "").strip(),
        "processing_step": str(processing_step or "").strip(),
        "execution_status": str(execution_status or "").strip(),
        "qa_status": str(qa_status or "").strip(),
        "output_folder": str(output_folder or "").strip(),
        "execution_note": str(execution_note or "").strip(),
    }

    df = pd.concat([df, pd.DataFrame([event])], ignore_index=True)
    df.to_csv(path, index=False)
    return event


def get_latest_execution_event(batch_id: str) -> Optional[Dict[str, Any]]:
    if not str(batch_id or "").strip():
        return None

    df = load_processing_execution_log()
    if df.empty:
        return None

    subset = df[df["batch_id"].astype(str) == str(batch_id).strip()].copy()
    if subset.empty:
        return None

    subset = subset.sort_values("execution_event_ts", ascending=False, na_position="last")
    return subset.iloc[0].to_dict()


def get_latest_execution_events() -> pd.DataFrame:
    df = load_processing_execution_log()
    if df.empty:
        return df.copy()

    ordered = df.sort_values("execution_event_ts", ascending=False, na_position="last").copy()
    deduped = ordered.drop_duplicates(subset=["batch_id", "processing_step"], keep="first")
    return deduped.reset_index(drop=True)


def has_execution_log() -> bool:
    path = get_processing_execution_log_path()
    return path.exists()
