from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path

from services.batch_creation_helper import ensure_csv_with_header
from services.repo_paths import resolve_repo_path


PROCESSING_TRIGGER_HEADER = [
    "trigger_event_ts",
    "batch_id",
    "processing_step",
    "trigger_source",
    "eligibility_status",
    "trigger_status",
    "operator_note",
]

PROCESSING_EXECUTION_HEADER = [
    "execution_event_ts",
    "batch_id",
    "processing_step",
    "script_name",
    "execution_status",
    "qa_status",
    "output_folder",
    "note",
]

PROCESSING_HISTORY_LOG_HEADER = [
    "history_event_ts",
    "batch_id",
    "processing_step",
    "script_name",
    "result_status",
    "output_folder",
    "qa_status",
    "note",
]


def _append_row(path_str: str, header: list[str], row: dict) -> None:
    path = resolve_repo_path(path_str)
    ensure_csv_with_header(path, header)

    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writerow(row)


def append_processing_trigger_registry(
    batch_id: str,
    processing_step: str,
    trigger_source: str,
    eligibility_status: str,
    trigger_status: str,
    operator_note: str,
) -> None:
    _append_row(
        "data_input/registry/processing_trigger_registry.csv",
        PROCESSING_TRIGGER_HEADER,
        {
            "trigger_event_ts": datetime.now().isoformat(timespec="seconds"),
            "batch_id": batch_id.strip(),
            "processing_step": processing_step.strip(),
            "trigger_source": trigger_source.strip(),
            "eligibility_status": eligibility_status.strip(),
            "trigger_status": trigger_status.strip(),
            "operator_note": operator_note.strip(),
        },
    )


def append_processing_execution_registry(
    batch_id: str,
    processing_step: str,
    script_name: str,
    execution_status: str,
    qa_status: str,
    output_folder: str,
    note: str,
) -> None:
    _append_row(
        "data_input/registry/processing_execution_registry.csv",
        PROCESSING_EXECUTION_HEADER,
        {
            "execution_event_ts": datetime.now().isoformat(timespec="seconds"),
            "batch_id": batch_id.strip(),
            "processing_step": processing_step.strip(),
            "script_name": script_name.strip(),
            "execution_status": execution_status.strip(),
            "qa_status": qa_status.strip(),
            "output_folder": output_folder.strip(),
            "note": note.strip(),
        },
    )


def append_processing_history_log(
    batch_id: str,
    processing_step: str,
    script_name: str,
    result_status: str,
    output_folder: str,
    qa_status: str,
    note: str,
) -> None:
    _append_row(
        "data_input/registry/processing_history_log.csv",
        PROCESSING_HISTORY_LOG_HEADER,
        {
            "history_event_ts": datetime.now().isoformat(timespec="seconds"),
            "batch_id": batch_id.strip(),
            "processing_step": processing_step.strip(),
            "script_name": script_name.strip(),
            "result_status": result_status.strip(),
            "output_folder": output_folder.strip(),
            "qa_status": qa_status.strip(),
            "note": note.strip(),
        },
    )


def load_processing_history_log():
    import pandas as pd

    path = resolve_repo_path("data_input/registry/processing_history_log.csv")
    if not path.exists():
        return pd.DataFrame(columns=PROCESSING_HISTORY_LOG_HEADER)

    df = pd.read_csv(path)
    for col in PROCESSING_HISTORY_LOG_HEADER:
        if col not in df.columns:
            df[col] = ""
    return df[PROCESSING_HISTORY_LOG_HEADER].copy()
