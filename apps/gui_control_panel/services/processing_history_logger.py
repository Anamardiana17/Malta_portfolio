from __future__ import annotations

import csv
from datetime import datetime

from services.batch_creation_helper import ensure_csv_with_header
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


def append_processing_history(
    batch_id: str,
    processing_step: str,
    script_name: str,
    result_status: str,
    output_folder: str,
    qa_status: str,
    note: str,
) -> None:
    path = resolve_repo_path("data_input/registry/processing_history.csv")
    ensure_csv_with_header(path, PROCESSING_HISTORY_HEADER)

    row = {
        "batch_id": batch_id.strip(),
        "processed_at": datetime.now().isoformat(timespec="seconds"),
        "processing_step": processing_step.strip(),
        "script_name": script_name.strip(),
        "result_status": result_status.strip(),
        "output_folder": output_folder.strip(),
        "qa_status": qa_status.strip(),
        "note": note.strip(),
    }

    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=PROCESSING_HISTORY_HEADER)
        writer.writerow(row)
