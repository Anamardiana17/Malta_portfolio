from __future__ import annotations

import csv
import json
import re
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

from services.repo_paths import resolve_repo_path


@dataclass
class UploadedFilePayload:
    name: str
    bytes_data: bytes


@dataclass
class BatchCreationResult:
    batch_id: str
    batch_label: str
    batch_dir: Path
    files_dir: Path
    manifest_path: Path
    file_count: int
    total_size_bytes: int


class BatchCreationError(Exception):
    pass


def sanitize_batch_label(batch_label: str) -> str:
    cleaned = batch_label.strip().lower()
    cleaned = re.sub(r"\s+", "_", cleaned)
    cleaned = re.sub(r"[^a-z0-9_-]", "", cleaned)
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    if not cleaned:
        raise BatchCreationError("batch_label is empty after sanitization.")
    return cleaned


def generate_batch_id(batch_label: str, now: datetime | None = None) -> str:
    ts = (now or datetime.now()).strftime("%Y%m%d_%H%M%S")
    return f"{ts}_{sanitize_batch_label(batch_label)}"


def ensure_csv_with_header(path: Path, header: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(header)


def append_csv_row(path: Path, header: list[str], row: dict) -> None:
    ensure_csv_with_header(path, header)
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writerow(row)


def _safe_filename(filename: str) -> str:
    cleaned = Path(filename).name.strip()
    cleaned = re.sub(r"[^\w.\- ]", "_", cleaned)
    cleaned = re.sub(r"\s+", "_", cleaned)
    if not cleaned:
        raise BatchCreationError("Invalid uploaded filename.")
    return cleaned


def _dedupe_filename(target_dir: Path, filename: str) -> str:
    candidate = filename
    stem = Path(filename).stem
    suffix = Path(filename).suffix
    counter = 1
    while (target_dir / candidate).exists():
        candidate = f"{stem}__{counter}{suffix}"
        counter += 1
    return candidate


def create_batch(
    batch_label: str,
    uploaded_files: Iterable[UploadedFilePayload],
    notes: str = "",
    source_type: str = "gui_upload",
) -> BatchCreationResult:
    files = list(uploaded_files)
    if not files:
        raise BatchCreationError("No uploaded files provided.")

    repo_root = resolve_repo_path(".")
    inbox_root = resolve_repo_path("data_input/inbox")
    accepted_root = resolve_repo_path("data_input/accepted")
    rejected_root = resolve_repo_path("data_input/rejected")
    input_registry_csv = resolve_repo_path("data_input/registry/input_registry.csv")
    upload_log_csv = resolve_repo_path("data_input/registry/upload_log.csv")

    inbox_root.mkdir(parents=True, exist_ok=True)
    accepted_root.mkdir(parents=True, exist_ok=True)
    rejected_root.mkdir(parents=True, exist_ok=True)

    batch_id = generate_batch_id(batch_label)
    batch_dir = inbox_root / batch_id
    files_dir = batch_dir / "files"
    manifest_path = batch_dir / "manifest.json"

    if batch_dir.exists():
        raise BatchCreationError(f"Batch directory already exists: {batch_dir}")

    batch_dir.mkdir(parents=True, exist_ok=False)
    files_dir.mkdir(parents=True, exist_ok=False)

    created_dt = datetime.now()
    uploaded_at = created_dt.isoformat(timespec="seconds")
    upload_date = created_dt.strftime("%Y-%m-%d")
    upload_time = created_dt.strftime("%H:%M:%S")

    manifest_files: list[dict] = []
    total_size_bytes = 0

    try:
        for file_obj in files:
            safe_name = _safe_filename(file_obj.name)
            stored_name = _dedupe_filename(files_dir, safe_name)
            target_path = files_dir / stored_name
            target_path.write_bytes(file_obj.bytes_data)

            size_bytes = target_path.stat().st_size
            total_size_bytes += size_bytes

            relpath = target_path.relative_to(repo_root).as_posix()
            manifest_files.append(
                {
                    "original_name": file_obj.name,
                    "stored_name": stored_name,
                    "relative_path": relpath,
                    "extension": Path(stored_name).suffix.lower(),
                    "size_bytes": size_bytes,
                }
            )

            append_csv_row(
                upload_log_csv,
                header=[
                    "event_ts",
                    "batch_id",
                    "batch_label",
                    "zone",
                    "file_name",
                    "file_ext",
                    "file_size_bytes",
                    "stored_relpath",
                    "upload_status",
                ],
                row={
                    "event_ts": uploaded_at,
                    "batch_id": batch_id,
                    "batch_label": sanitize_batch_label(batch_label),
                    "zone": "inbox",
                    "file_name": stored_name,
                    "file_ext": Path(stored_name).suffix.lower(),
                    "file_size_bytes": size_bytes,
                    "stored_relpath": relpath,
                    "upload_status": "registered",
                },
            )

        manifest = {
            "batch_id": batch_id,
            "batch_label": sanitize_batch_label(batch_label),
            "uploaded_at": uploaded_at,
            "upload_date": upload_date,
            "upload_time": upload_time,
            "source_type": source_type,
            "zone": "inbox",
            "status": "registered",
            "file_count": len(manifest_files),
            "total_size_bytes": total_size_bytes,
            "notes": notes,
            "files": manifest_files,
        }

        manifest_path.write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

        append_csv_row(
            input_registry_csv,
            header=[
                "batch_id",
                "batch_label",
                "uploaded_at",
                "upload_date",
                "upload_time",
                "source_type",
                "status",
                "file_count",
                "notes",
            ],
            row={
                "batch_id": batch_id,
                "batch_label": sanitize_batch_label(batch_label),
                "uploaded_at": uploaded_at,
                "upload_date": upload_date,
                "upload_time": upload_time,
                "source_type": source_type,
                "status": "registered",
                "file_count": len(manifest_files),
                "notes": notes,
            },
        )

        return BatchCreationResult(
            batch_id=batch_id,
            batch_label=sanitize_batch_label(batch_label),
            batch_dir=batch_dir,
            files_dir=files_dir,
            manifest_path=manifest_path,
            file_count=len(manifest_files),
            total_size_bytes=total_size_bytes,
        )

    except Exception:
        if batch_dir.exists():
            shutil.rmtree(batch_dir, ignore_errors=True)
        raise
