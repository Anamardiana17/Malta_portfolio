from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime

import pandas as pd

from services.batch_creation_helper import ensure_csv_with_header
from services.repo_paths import resolve_repo_path


REVIEW_REGISTRY_HEADER = [
    "batch_id",
    "last_reviewed_at",
    "reviewer_name",
    "review_outcome",
    "review_outcome_label",
    "recommendation_code",
    "reviewer_status_recommendation",
    "decision_summary",
    "recommended_dataset_types",
    "profiled_files",
    "readable_files",
    "strong_match_files",
    "partial_match_files",
    "weak_match_files",
    "no_match_files",
    "unreadable_files",
    "unsupported_files",
    "average_match_score",
    "batch_location",
    "movement_status",
    "movement_note",
    "review_notes",
]

REVIEW_LOG_HEADER = [
    "review_event_ts",
    "batch_id",
    "reviewer_name",
    "review_outcome",
    "review_outcome_label",
    "recommendation_code",
    "reviewer_status_recommendation",
    "decision_summary",
    "recommended_dataset_types",
    "profiled_files",
    "readable_files",
    "strong_match_files",
    "partial_match_files",
    "weak_match_files",
    "no_match_files",
    "unreadable_files",
    "unsupported_files",
    "average_match_score",
    "batch_location",
    "movement_status",
    "movement_note",
    "review_notes",
]

INPUT_REGISTRY_HEADER = [
    "batch_id",
    "batch_label",
    "uploaded_at",
    "upload_date",
    "upload_time",
    "source_type",
    "status",
    "file_count",
    "notes",
]

MANUAL_OUTCOME_LABELS = {
    "accepted_manual_review": "Accepted for processing gate",
    "hold_manual_review": "Hold for manual clarification",
    "rejected_manual_review": "Rejected for current processing gate",
}


@dataclass
class BatchDecisionSummary:
    batch_id: str
    profiled_files: int
    readable_files: int
    strong_match_files: int
    partial_match_files: int
    weak_match_files: int
    no_match_files: int
    unreadable_files: int
    unsupported_files: int
    average_match_score: float
    recommended_dataset_types: list[str]
    recommendation_code: str
    reviewer_status_recommendation: str
    decision_summary: str
    rationale: list[str]


def build_batch_decision_summary(batch_id: str, profile_df: pd.DataFrame) -> BatchDecisionSummary:
    if profile_df.empty:
        return BatchDecisionSummary(
            batch_id=batch_id,
            profiled_files=0,
            readable_files=0,
            strong_match_files=0,
            partial_match_files=0,
            weak_match_files=0,
            no_match_files=0,
            unreadable_files=0,
            unsupported_files=0,
            average_match_score=0.0,
            recommended_dataset_types=[],
            recommendation_code="manual_hold_recommended",
            reviewer_status_recommendation="Hold batch for manual clarification.",
            decision_summary="No readable files were profiled. Reviewer action is required before any downstream processing.",
            rationale=[
                "Profiler returned no readable file rows.",
                "Batch should remain in inbox until reviewer confirms file validity.",
            ],
        )

    working_df = profile_df.copy()
    working_df["match_status"] = working_df["match_status"].fillna("").astype(str)
    working_df["likely_dataset_type"] = working_df["likely_dataset_type"].fillna("").astype(str)
    working_df["match_score"] = pd.to_numeric(working_df["match_score"], errors="coerce").fillna(0.0)

    profiled_files = len(working_df)
    unreadable_files = int((working_df["match_status"] == "unreadable").sum())
    unsupported_files = int((working_df["match_status"] == "unsupported").sum())
    readable_files = profiled_files - unreadable_files - unsupported_files

    strong_match_files = int((working_df["match_status"] == "strong_match").sum())
    partial_match_files = int((working_df["match_status"] == "partial_match").sum())
    weak_match_files = int((working_df["match_status"] == "weak_match").sum())
    no_match_files = int((working_df["match_status"] == "no_match").sum())

    average_match_score = round(float(working_df["match_score"].mean()), 4)

    recommended_dataset_types = sorted(
        {
            dataset_type
            for dataset_type in working_df["likely_dataset_type"].tolist()
            if dataset_type
            and dataset_type not in {"unknown", "unreadable", "unsupported_file_type"}
        }
    )

    rationale: list[str] = []
    recommendation_code = "manual_hold_recommended"
    reviewer_status_recommendation = "Hold batch for manual clarification."

    all_strong = (
        profiled_files > 0
        and strong_match_files == profiled_files
    )

    no_viable_match = (
        strong_match_files == 0
        and partial_match_files == 0
        and weak_match_files == 0
    )

    if all_strong:
        recommendation_code = "accept_recommended"
        reviewer_status_recommendation = (
            "Profiler indicates the batch is structurally ready for manual acceptance."
        )
        rationale.append("All profiled files are strong schema matches.")
        rationale.append("No unsupported, unreadable, or no-match files were detected.")
    elif no_viable_match:
        recommendation_code = "reject_recommended"
        reviewer_status_recommendation = (
            "Profiler indicates the batch is not currently suitable for processing acceptance."
        )
        rationale.append("No viable schema match was detected across profiled files.")
        if unreadable_files > 0:
            rationale.append("One or more files are unreadable.")
        if unsupported_files > 0:
            rationale.append("One or more files use unsupported file types.")
        if no_match_files > 0:
            rationale.append("One or more files do not align with any governed intake schema.")
    else:
        recommendation_code = "manual_hold_recommended"
        reviewer_status_recommendation = (
            "Profiler indicates partial structural alignment. Manual review hold is recommended."
        )
        if partial_match_files > 0:
            rationale.append("At least one file is only a partial schema match.")
        if weak_match_files > 0:
            rationale.append("At least one file is only a weak schema match.")
        if no_match_files > 0:
            rationale.append("At least one file has no schema match.")
        if unreadable_files > 0:
            rationale.append("At least one file is unreadable.")
        if unsupported_files > 0:
            rationale.append("At least one file uses an unsupported file type.")

    decision_summary = (
        f"Profiled {profiled_files} file(s): "
        f"{strong_match_files} strong, "
        f"{partial_match_files} partial, "
        f"{weak_match_files} weak, "
        f"{no_match_files} no-match, "
        f"{unreadable_files} unreadable, "
        f"{unsupported_files} unsupported. "
        f"Average match score = {average_match_score:.4f}."
    )

    if recommended_dataset_types:
        rationale.append(
            "Recommended dataset types inferred: " + ", ".join(recommended_dataset_types)
        )
    else:
        rationale.append("No governed dataset type could be confidently inferred.")

    return BatchDecisionSummary(
        batch_id=batch_id,
        profiled_files=profiled_files,
        readable_files=readable_files,
        strong_match_files=strong_match_files,
        partial_match_files=partial_match_files,
        weak_match_files=weak_match_files,
        no_match_files=no_match_files,
        unreadable_files=unreadable_files,
        unsupported_files=unsupported_files,
        average_match_score=average_match_score,
        recommended_dataset_types=recommended_dataset_types,
        recommendation_code=recommendation_code,
        reviewer_status_recommendation=reviewer_status_recommendation,
        decision_summary=decision_summary,
        rationale=rationale,
    )


def _upsert_registry_row(path, header: list[str], row: dict, key_field: str) -> None:
    ensure_csv_with_header(path, header)

    rows: list[dict] = []
    found = False

    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for existing_row in reader:
            if existing_row.get(key_field) == row.get(key_field):
                rows.append(row)
                found = True
            else:
                rows.append(existing_row)

    if not found:
        rows.append(row)

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)


def _append_log_row(path, header: list[str], row: dict) -> None:
    ensure_csv_with_header(path, header)
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writerow(row)


def _update_input_registry_status(batch_id: str, new_status: str) -> None:
    input_registry_path = resolve_repo_path("data_input/registry/input_registry.csv")
    ensure_csv_with_header(input_registry_path, INPUT_REGISTRY_HEADER)

    rows: list[dict] = []
    found = False

    with input_registry_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("batch_id") == batch_id:
                row["status"] = new_status
                found = True
            rows.append(row)

    if not found:
        return

    with input_registry_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=INPUT_REGISTRY_HEADER)
        writer.writeheader()
        writer.writerows(rows)


def record_manual_acceptance_review(
    batch_id: str,
    reviewer_name: str,
    review_outcome: str,
    review_notes: str,
    summary: BatchDecisionSummary,
    batch_location: str,
    movement_status: str,
    movement_note: str,
) -> None:
    if review_outcome not in MANUAL_OUTCOME_LABELS:
        raise ValueError(f"Unsupported review_outcome: {review_outcome}")

    review_ts = datetime.now().isoformat(timespec="seconds")
    recommended_dataset_types = ", ".join(summary.recommended_dataset_types)

    review_row = {
        "batch_id": batch_id,
        "last_reviewed_at": review_ts,
        "reviewer_name": reviewer_name.strip(),
        "review_outcome": review_outcome,
        "review_outcome_label": MANUAL_OUTCOME_LABELS[review_outcome],
        "recommendation_code": summary.recommendation_code,
        "reviewer_status_recommendation": summary.reviewer_status_recommendation,
        "decision_summary": summary.decision_summary,
        "recommended_dataset_types": recommended_dataset_types,
        "profiled_files": summary.profiled_files,
        "readable_files": summary.readable_files,
        "strong_match_files": summary.strong_match_files,
        "partial_match_files": summary.partial_match_files,
        "weak_match_files": summary.weak_match_files,
        "no_match_files": summary.no_match_files,
        "unreadable_files": summary.unreadable_files,
        "unsupported_files": summary.unsupported_files,
        "average_match_score": summary.average_match_score,
        "review_notes": review_notes.strip(),
    }

    log_row = {
        "review_event_ts": review_ts,
        "batch_id": batch_id,
        "reviewer_name": reviewer_name.strip(),
        "review_outcome": review_outcome,
        "review_outcome_label": MANUAL_OUTCOME_LABELS[review_outcome],
        "recommendation_code": summary.recommendation_code,
        "reviewer_status_recommendation": summary.reviewer_status_recommendation,
        "decision_summary": summary.decision_summary,
        "recommended_dataset_types": recommended_dataset_types,
        "profiled_files": summary.profiled_files,
        "readable_files": summary.readable_files,
        "strong_match_files": summary.strong_match_files,
        "partial_match_files": summary.partial_match_files,
        "weak_match_files": summary.weak_match_files,
        "no_match_files": summary.no_match_files,
        "unreadable_files": summary.unreadable_files,
        "unsupported_files": summary.unsupported_files,
        "average_match_score": summary.average_match_score,
        "review_notes": review_notes.strip(),
    }

    registry_path = resolve_repo_path("data_input/registry/acceptance_review_registry.csv")
    log_path = resolve_repo_path("data_input/registry/acceptance_review_log.csv")

    _upsert_registry_row(
        path=registry_path,
        header=REVIEW_REGISTRY_HEADER,
        row=review_row,
        key_field="batch_id",
    )
    _append_log_row(
        path=log_path,
        header=REVIEW_LOG_HEADER,
        row=log_row,
    )

    input_registry_status_map = {
        "accepted_manual_review": "accepted_manual_review",
        "hold_manual_review": "hold_manual_review",
        "rejected_manual_review": "rejected_manual_review",
    }
    _update_input_registry_status(
        batch_id=batch_id,
        new_status=input_registry_status_map[review_outcome],
    )
