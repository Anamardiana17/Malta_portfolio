from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd


EXECUTION_LOG_COLUMNS = [
    "execution_event_ts",
    "batch_id",
    "processing_step",
    "script_name",
    "execution_status",
    "qa_status",
    "output_folder",
    "note",
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
    script_name: str = "",
    execution_status: str,
    qa_status: str = "",
    output_folder: str = "",
    note: str = "",
) -> Dict[str, Any]:
    path = ensure_processing_execution_log()
    df = load_processing_execution_log()

    event = {
        "execution_event_ts": pd.Timestamp.now().isoformat(),
        "batch_id": str(batch_id or "").strip(),
        "processing_step": str(processing_step or "").strip(),
        "script_name": str(script_name or "").strip(),
        "execution_status": str(execution_status or "").strip(),
        "qa_status": str(qa_status or "").strip(),
        "output_folder": str(output_folder or "").strip(),
        "note": str(note or "").strip(),
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

    subset = subset.reset_index(drop=False).rename(columns={"index": "_row_order"})
    subset["execution_event_ts"] = pd.to_datetime(subset["execution_event_ts"], errors="coerce")

    status_rank = {
        "execution_completed": 3,
        "execution_started": 2,
        "trigger_recorded": 1,
    }
    subset["_status_rank"] = subset["execution_status"].astype(str).map(status_rank).fillna(0)

    subset = subset.sort_values(
        by=["execution_event_ts", "_status_rank", "_row_order"],
        ascending=[False, False, False],
        na_position="last",
    )

    return subset.iloc[0][EXECUTION_LOG_COLUMNS].to_dict()


def get_latest_execution_events() -> pd.DataFrame:
    df = load_processing_execution_log()
    if df.empty:
        return df.copy()

    ordered = df.reset_index(drop=False).rename(columns={"index": "_row_order"})
    ordered["execution_event_ts"] = pd.to_datetime(ordered["execution_event_ts"], errors="coerce")

    status_rank = {
        "execution_completed": 3,
        "execution_started": 2,
        "trigger_recorded": 1,
    }
    ordered["_status_rank"] = ordered["execution_status"].astype(str).map(status_rank).fillna(0)

    ordered = ordered.sort_values(
        by=["execution_event_ts", "_status_rank", "_row_order"],
        ascending=[False, False, False],
        na_position="last",
    )

    deduped = ordered.drop_duplicates(subset=["batch_id", "processing_step"], keep="first")
    return deduped[EXECUTION_LOG_COLUMNS].reset_index(drop=True)


def has_execution_log() -> bool:
    path = get_processing_execution_log_path()
    return path.exists()
