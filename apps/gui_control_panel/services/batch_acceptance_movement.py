from __future__ import annotations

import shutil
from dataclasses import dataclass

from services.repo_paths import resolve_repo_path


@dataclass
class BatchMovementResult:
    batch_id: str
    source_relpath: str
    destination_relpath: str
    batch_location: str
    movement_status: str
    movement_note: str


def move_batch_by_review_outcome(batch_id: str, review_outcome: str) -> BatchMovementResult:
    source_dir = resolve_repo_path(f"data_input/inbox/{batch_id}")

    if not source_dir.exists() or not source_dir.is_dir():
        raise FileNotFoundError(f"Inbox batch folder not found for batch_id={batch_id}")

    if review_outcome == "hold_manual_review":
        return BatchMovementResult(
            batch_id=batch_id,
            source_relpath=f"data_input/inbox/{batch_id}",
            destination_relpath=f"data_input/inbox/{batch_id}",
            batch_location="inbox",
            movement_status="no_movement_hold",
            movement_note="Batch remains in inbox because manual review outcome is hold.",
        )

    if review_outcome == "accepted_manual_review":
        destination_dir = resolve_repo_path(f"data_input/accepted/{batch_id}")
        batch_location = "accepted"
        movement_status = "moved_to_accepted"
        movement_note = "Batch moved from inbox to accepted after manual review."
    elif review_outcome == "rejected_manual_review":
        destination_dir = resolve_repo_path(f"data_input/rejected/{batch_id}")
        batch_location = "rejected"
        movement_status = "moved_to_rejected"
        movement_note = "Batch moved from inbox to rejected after manual review."
    else:
        raise ValueError(f"Unsupported review_outcome: {review_outcome}")

    if destination_dir.exists():
        raise FileExistsError(
            f"Destination batch folder already exists for batch_id={batch_id}: {destination_dir}"
        )

    destination_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(source_dir), str(destination_dir))

    return BatchMovementResult(
        batch_id=batch_id,
        source_relpath=f"data_input/inbox/{batch_id}",
        destination_relpath=f"data_input/{batch_location}/{batch_id}",
        batch_location=batch_location,
        movement_status=movement_status,
        movement_note=movement_note,
    )
