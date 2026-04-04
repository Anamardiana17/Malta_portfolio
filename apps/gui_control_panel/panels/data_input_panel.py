from __future__ import annotations

import pandas as pd
import streamlit as st


def _format_gui_flag_value(value):
    text = str(value)
    if text in {"1", "True", "true", "np.int64(1)"}:
        return "Yes"
    if text in {"0", "False", "false", "np.int64(0)"}:
        return "No"
    return text

def _format_gui_flag_dict(flag_dict):
    if not hasattr(flag_dict, "items"):
        return flag_dict
    return {k: _format_gui_flag_value(v) for k, v in flag_dict.items()}


def _gui_clean_flag_dict(value):
    try:
        if hasattr(value, "items"):
            cleaned = {}
            for k, v in value.items():
                sv = str(v)
                if "np.int64(1)" in sv or sv == "1":
                    cleaned[k] = "Yes"
                elif "np.int64(0)" in sv or sv == "0":
                    cleaned[k] = "No"
                else:
                    cleaned[k] = sv
            return cleaned
    except Exception:
        pass
    return value


from services.batch_creation_helper import (
    BatchCreationError,
    UploadedFilePayload,
    create_batch,
)
from services.batch_schema_profiler import profile_batch
from services.manual_acceptance_review import (
    MANUAL_OUTCOME_LABELS,
    build_batch_decision_summary,
    record_manual_acceptance_review,
)
from services.batch_acceptance_movement import move_batch_by_review_outcome
from services.repo_paths import resolve_repo_path
from services.schema_registry_loader import get_supported_dataset_types


def _load_csv_or_empty(relative_path: str, expected_columns: list[str]) -> pd.DataFrame:
    path = resolve_repo_path(relative_path)
    if path.exists():
        try:
            return pd.read_csv(path)
        except Exception:
            return pd.DataFrame(columns=expected_columns)
    return pd.DataFrame(columns=expected_columns)


def _count_batch_dirs(relative_path: str) -> int:
    path = resolve_repo_path(relative_path)
    if not path.exists():
        return 0
    return sum(1 for p in path.iterdir() if p.is_dir())


def _safe_text(value: object, default: str = "-") -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text if text else default


def _build_acceptance_evidence_summary(
    acceptance_review_registry: pd.DataFrame,
    batch_id: str | None = None,
) -> dict[str, object]:
    if acceptance_review_registry.empty:
        return {
            "has_data": False,
            "latest_batch_id": "-",
            "latest_reviewed_at": "-",
            "latest_recommendation": "-",
            "latest_manual_outcome": "-",
            "latest_batch_location": "-",
            "latest_movement_status": "-",
            "latest_recommended_dataset_types": "-",
            "latest_decision_summary": "No acceptance evidence has been recorded yet.",
            "latest_review_notes": "-",
            "reviewed_batches": 0,
            "accepted_reviews": 0,
            "held_reviews": 0,
            "rejected_reviews": 0,
            "governance_note": (
                "Acceptance evidence is not yet available. A reviewer cannot yet inspect a governed intake-to-review-to-movement trail."
            ),
        }

    registry = acceptance_review_registry.fillna("").copy()

    if batch_id is not None and str(batch_id).strip():
        batch_registry = registry[
            registry["batch_id"].astype(str).str.strip() == str(batch_id).strip()
        ].copy()
    else:
        batch_registry = registry

    if batch_registry.empty:
        return {
            "has_data": False,
            "latest_batch_id": _safe_text(batch_id),
            "latest_reviewed_at": "-",
            "latest_recommendation": "-",
            "latest_manual_outcome": "-",
            "latest_batch_location": "-",
            "latest_movement_status": "-",
            "latest_recommended_dataset_types": "-",
            "latest_decision_summary": "No acceptance evidence has been recorded yet for the selected batch.",
            "latest_review_notes": "-",
            "reviewed_batches": len(registry),
            "accepted_reviews": int(
                (registry["review_outcome"].astype(str) == "accepted_manual_review").sum()
            ),
            "held_reviews": int(
                (registry["review_outcome"].astype(str) == "hold_manual_review").sum()
            ),
            "rejected_reviews": int(
                (registry["review_outcome"].astype(str) == "rejected_manual_review").sum()
            ),
            "governance_note": (
                "The selected batch does not yet have reviewer-facing acceptance evidence in the registry."
            ),
        }

    latest_row = batch_registry.iloc[-1]

    latest_batch_id = _safe_text(latest_row.get("batch_id"))
    latest_reviewed_at = _safe_text(latest_row.get("last_reviewed_at"))
    latest_recommendation = _safe_text(latest_row.get("reviewer_status_recommendation"))
    latest_manual_outcome = _safe_text(latest_row.get("review_outcome_label"))
    latest_batch_location = _safe_text(latest_row.get("batch_location"))
    latest_movement_status = _safe_text(latest_row.get("movement_status"))
    latest_recommended_dataset_types = _safe_text(
        latest_row.get("recommended_dataset_types"),
        default="none confidently inferred",
    )
    latest_decision_summary = _safe_text(latest_row.get("decision_summary"))
    latest_review_notes = _safe_text(latest_row.get("review_notes"))

    reviewed_batches = len(batch_registry)
    accepted_reviews = int(
        (batch_registry["review_outcome"].astype(str) == "accepted_manual_review").sum()
    )
    held_reviews = int(
        (batch_registry["review_outcome"].astype(str) == "hold_manual_review").sum()
    )
    rejected_reviews = int(
        (batch_registry["review_outcome"].astype(str) == "rejected_manual_review").sum()
    )

    governance_note = (
        f"The latest reviewed batch {latest_batch_id} received recommendation "
        f"'{latest_recommendation}', manual outcome '{latest_manual_outcome}', "
        f"and movement status '{latest_movement_status}' with resulting location "
        f"'{latest_batch_location}'. This supports reviewer visibility into governed intake control, including cases where reviewer judgment overrides a profiler hold recommendation for controlled GUI validation."
    )

    return {
        "has_data": True,
        "latest_batch_id": latest_batch_id,
        "latest_reviewed_at": latest_reviewed_at,
        "latest_recommendation": latest_recommendation,
        "latest_manual_outcome": latest_manual_outcome,
        "latest_batch_location": latest_batch_location,
        "latest_movement_status": latest_movement_status,
        "latest_recommended_dataset_types": latest_recommended_dataset_types,
        "latest_decision_summary": latest_decision_summary,
        "latest_review_notes": latest_review_notes,
        "reviewed_batches": reviewed_batches,
        "accepted_reviews": accepted_reviews,
        "held_reviews": held_reviews,
        "rejected_reviews": rejected_reviews,
        "governance_note": governance_note,
    }



def _render_acceptance_evidence_summary(summary: dict[str, object]) -> None:
    st.markdown("### Acceptance Evidence Summary")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Latest batch", summary.get("latest_batch_id", "-"))
    c2.metric("Recommendation", summary.get("latest_recommendation", "-"))
    c3.metric("Manual outcome", summary.get("latest_manual_outcome", "-"))
    c4.metric("Movement status", summary.get("latest_movement_status", "-"))

    c5, c6, c7, c8 = st.columns(4)
    c5.metric("Reviewed batches", int(summary.get("reviewed_batches", 0)))
    c6.metric("Accepted", int(summary.get("accepted_reviews", 0)))
    c7.metric("Held", int(summary.get("held_reviews", 0)))
    c8.metric("Rejected", int(summary.get("rejected_reviews", 0)))

    st.write("**Latest reviewed at:**", summary.get("latest_reviewed_at", "-"))
    st.write("**Latest batch location:**", summary.get("latest_batch_location", "-"))
    st.write(
        "**Recommended dataset types:**",
        summary.get("latest_recommended_dataset_types", "-"),
    )
    st.write("**Decision summary:**", summary.get("latest_decision_summary", "-"))
    st.write("**Review notes:**", summary.get("latest_review_notes", "-"))

    if summary.get("has_data"):
        st.success(summary.get("governance_note", "Acceptance evidence available."))
    else:
        st.info(summary.get("governance_note", "No acceptance evidence available yet."))

    st.caption(
        "This is the primary reviewer-facing intake evidence block for the Data Input Panel. Profiler recommendation and manual review outcome are shown separately to preserve governed override visibility."
    )

def _render_decision_summary(batch_id: str, profile_df: pd.DataFrame) -> None:
    summary = build_batch_decision_summary(batch_id=batch_id, profile_df=profile_df)

    st.markdown("### Decision Summary")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Profiled files", summary.profiled_files)
    c2.metric("Strong matches", summary.strong_match_files)
    c3.metric("Partial matches", summary.partial_match_files)
    c4.metric("No-match / unreadable", summary.no_match_files + summary.unreadable_files)
    c5.metric("Avg match score", f"{summary.average_match_score:.4f}")

    if summary.recommendation_code == "accept_recommended":
        st.success(
            f"Reviewer-facing recommendation: {summary.reviewer_status_recommendation}"
        )
    elif summary.recommendation_code == "reject_recommended":
        st.error(
            f"Reviewer-facing recommendation: {summary.reviewer_status_recommendation}"
        )
    else:
        st.warning(
            f"Reviewer-facing recommendation: {summary.reviewer_status_recommendation}"
        )

    st.write(summary.decision_summary)

    if summary.recommended_dataset_types:
        st.write(
            "**Inferred governed dataset types:** "
            + ", ".join(summary.recommended_dataset_types)
        )
    else:
        st.write("**Inferred governed dataset types:** none confidently inferred")

    st.markdown("### Reviewer Rationale")
    for item in summary.rationale:
        st.write(f"- {item}")


def render() -> None:
    st.subheader("Data Input Panel")
    st.caption("Batch ingestion governance layer for uploaded datasets before processing.")

    supported_dataset_types = get_supported_dataset_types()

    st.markdown("### Supported Dataset Types")
    st.write(", ".join(supported_dataset_types))

    input_registry = _load_csv_or_empty(
        "data_input/registry/input_registry.csv",
        [
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
    )

    processing_history = _load_csv_or_empty(
        "data_input/registry/processing_history.csv",
        [
            "batch_id",
            "processed_at",
            "processing_step",
            "script_name",
            "result_status",
            "output_folder",
            "qa_status",
            "note",
        ],
    )

    upload_log = _load_csv_or_empty(
        "data_input/registry/upload_log.csv",
        [
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
    )

    acceptance_review_registry = _load_csv_or_empty(
        "data_input/registry/acceptance_review_registry.csv",
        [
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
        ],
    )

    acceptance_review_log = _load_csv_or_empty(
        "data_input/registry/acceptance_review_log.csv",
        [
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
        ],
    )

    selected_acceptance_batch_id = None
    if not acceptance_review_registry.empty and "batch_id" in acceptance_review_registry.columns:
        acceptance_batch_options = (
            acceptance_review_registry["batch_id"]
            .astype(str)
            .str.strip()
            .replace("", pd.NA)
            .dropna()
            .drop_duplicates()
            .tolist()
        )
        if acceptance_batch_options:
            selected_acceptance_batch_id = st.selectbox(
                "Acceptance evidence batch_id",
                options=acceptance_batch_options,
                index=len(acceptance_batch_options) - 1,
                key="acceptance_evidence_batch_id",
                help="Reviewer-facing acceptance evidence is shown for the selected governed batch.",
            )

    acceptance_evidence_summary = _build_acceptance_evidence_summary(
        acceptance_review_registry=acceptance_review_registry,
        batch_id=selected_acceptance_batch_id,
    )

    _render_acceptance_evidence_summary(acceptance_evidence_summary)

    inbox_count = _count_batch_dirs("data_input/inbox")
    accepted_count = _count_batch_dirs("data_input/accepted")
    rejected_count = _count_batch_dirs("data_input/rejected")

    if not acceptance_review_registry.empty and "review_outcome" in acceptance_review_registry.columns:
        accepted_reviews_count = int(
            (acceptance_review_registry["review_outcome"].astype(str) == "accepted_manual_review").sum()
        )
        rejected_reviews_count = int(
            (acceptance_review_registry["review_outcome"].astype(str) == "rejected_manual_review").sum()
        )
        held_reviews_count = int(
            (acceptance_review_registry["review_outcome"].astype(str) == "hold_manual_review").sum()
        )
    else:
        accepted_reviews_count = 0
        rejected_reviews_count = 0
        held_reviews_count = 0

    accepted_count = max(accepted_count, accepted_reviews_count)
    rejected_count = max(rejected_count, rejected_reviews_count)
    registry_rows = max(len(input_registry), len(acceptance_review_registry))
    reviewed_rows = len(acceptance_review_registry)

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Inbox batches", inbox_count)
    c2.metric("Accepted batches", accepted_count)
    c3.metric("Rejected batches", rejected_count)
    c4.metric("Registry rows", registry_rows)
    c5.metric("Reviewed batches", reviewed_rows)

    st.markdown("### Batch Registration")

    batch_label = st.text_input(
        "Batch label",
        placeholder="e.g. march_pos_roster",
        help="Used to generate batch folder suffix.",
    )

    notes = st.text_area(
        "Upload notes",
        placeholder="Optional reviewer / operator notes for this intake batch.",
    )

    uploaded_files = st.file_uploader(
        "Upload new dataset files",
        accept_multiple_files=True,
    )

    if st.button("Register new batch", type="primary", width="stretch"):
        try:
            if not batch_label.strip():
                st.error("Batch label is required.")
                return

            if not uploaded_files:
                st.error("At least one file must be uploaded.")
                return

            payloads = [
                UploadedFilePayload(
                    name=file.name,
                    bytes_data=file.getvalue(),
                )
                for file in uploaded_files
            ]

            result = create_batch(
                batch_label=batch_label,
                uploaded_files=payloads,
                notes=notes.strip(),
                source_type="gui_upload",
            )

            st.success("Batch registered successfully.")
            st.write(f"**Batch ID:** `{result.batch_id}`")
            st.write(f"**Batch folder:** `{result.batch_dir}`")
            st.write(f"**Manifest:** `{result.manifest_path}`")
            st.write(f"**File count:** {result.file_count}")
            st.write(f"**Total size bytes:** {result.total_size_bytes:,}")

            profile_df = profile_batch(result.batch_id)
            st.markdown("### New Batch Profiling Result")
            if profile_df.empty:
                st.warning("No readable files found for the newly registered batch.")
            else:
                st.dataframe(profile_df, width="stretch")
                _render_decision_summary(result.batch_id, profile_df)

            st.info("Refresh the page to reload registry tables and updated inbox counts.")

        except BatchCreationError as exc:
            st.error(f"Batch registration failed: {exc}")
        except Exception as exc:
            st.exception(exc)

    st.markdown("### Batch Schema Profiling")

    batch_id_to_profile = st.text_input(
        "Inbox batch id to profile",
        placeholder="e.g. 20260330_115452_fresha_rawkit_malta_demo",
        help="Profiles files inside data_input/inbox/<batch_id>/files",
    )

    if st.button("Profile inbox batch", width="stretch"):
        if not batch_id_to_profile.strip():
            st.error("Batch id is required for profiling.")
        else:
            profile_df = profile_batch(batch_id_to_profile.strip())
            if profile_df.empty:
                st.warning("No readable files found for this inbox batch.")
            else:
                st.dataframe(profile_df, width="stretch")
                _render_decision_summary(batch_id_to_profile.strip(), profile_df)

    st.markdown("### Manual Acceptance Review")

    review_batch_id = st.text_input(
        "Inbox batch id for manual review",
        placeholder="e.g. 20260330_115452_fresha_rawkit_malta_demo",
        help="Manual reviewer decision is persisted to registry/log and drives governed batch movement by outcome.",
    )

    reviewer_name = st.text_input(
        "Reviewer name",
        placeholder="e.g. ana_mardiana",
    )

    reviewer_outcome = st.selectbox(
        "Manual review outcome",
        options=list(MANUAL_OUTCOME_LABELS.keys()),
        format_func=lambda value: MANUAL_OUTCOME_LABELS[value],
    )

    reviewer_notes = st.text_area(
        "Reviewer notes",
        placeholder="Explain why the batch is accepted, held, or rejected for the current processing gate.",
    )

    if review_batch_id.strip():
        review_profile_df = profile_batch(review_batch_id.strip())
        if review_profile_df.empty:
            st.warning("No readable files found for this inbox batch.")
        else:
            st.dataframe(review_profile_df, width="stretch")
            _render_decision_summary(review_batch_id.strip(), review_profile_df)

            if st.button("Save manual review outcome", width="stretch"):
                if not reviewer_name.strip():
                    st.error("Reviewer name is required.")
                else:
                    summary = build_batch_decision_summary(
                        batch_id=review_batch_id.strip(),
                        profile_df=review_profile_df,
                    )
                    movement = move_batch_by_review_outcome(
                        batch_id=review_batch_id.strip(),
                        review_outcome=reviewer_outcome,
                    )
                    record_manual_acceptance_review(
                        batch_id=review_batch_id.strip(),
                        reviewer_name=reviewer_name.strip(),
                        review_outcome=reviewer_outcome,
                        review_notes=reviewer_notes.strip(),
                        summary=summary,
                        batch_location=movement.batch_location,
                        movement_status=movement.movement_status,
                        movement_note=movement.movement_note,
                    )
                    st.success(
                        f"Manual review outcome saved. Batch location: {movement.destination_relpath} | "
                        f"movement_status={movement.movement_status}"
                    )

    st.markdown("### Input Layer Path Check")
    path_checks = {
        "data_input/inbox": resolve_repo_path("data_input/inbox").exists(),
        "data_input/accepted": resolve_repo_path("data_input/accepted").exists(),
        "data_input/rejected": resolve_repo_path("data_input/rejected").exists(),
        "data_input/registry/input_registry.csv": resolve_repo_path("data_input/registry/input_registry.csv").exists(),
        "data_input/registry/processing_history.csv": resolve_repo_path("data_input/registry/processing_history.csv").exists(),
        "data_input/registry/upload_log.csv": resolve_repo_path("data_input/registry/upload_log.csv").exists(),
        "data_input/registry/acceptance_review_registry.csv": resolve_repo_path("data_input/registry/acceptance_review_registry.csv").exists(),
        "data_input/registry/acceptance_review_log.csv": resolve_repo_path("data_input/registry/acceptance_review_log.csv").exists(),
    }
    path_df = pd.DataFrame([{"path": k, "exists": v} for k, v in path_checks.items()])
    st.dataframe(path_df, width="stretch")

    st.markdown("### Input Registry")
    if input_registry.empty:
        st.info("No batch registration preview is currently available in this panel.")
    else:
        st.dataframe(input_registry, width="stretch")

    st.markdown("### Acceptance Review Registry")
    if acceptance_review_registry.empty:
        st.info("No manual acceptance review has been recorded yet.")
    else:
        st.dataframe(acceptance_review_registry, width="stretch")

    st.markdown("### Acceptance Review Log")
    if acceptance_review_log.empty:
        st.info("No manual acceptance review event has been logged yet.")
    else:
        st.dataframe(acceptance_review_log, width="stretch")

    st.markdown("### Upload Log")
    if upload_log.empty:
        st.info("No upload log has been recorded yet.")
    else:
        st.dataframe(upload_log, width="stretch")

    st.markdown("### Processing History")
    if processing_history.empty:
        st.info("No legacy processing history is shown here. Reviewer-facing execution evidence is surfaced in the Processing / QA Panel and Reviewer Pack.")
    else:
        st.dataframe(processing_history, width="stretch")

    st.markdown("### Batch Governance Policy")
    st.write("- New datasets should enter through data_input/inbox/")
    st.write("- Each upload should have its own timestamped batch folder")
    st.write("- Each batch should carry upload date and upload time")
    st.write("- Processing should start only after validation / manual acceptance review")
    st.write("- Manual acceptance review moves batches to accepted/, rejected/, or keeps them in inbox when held")
    st.write("- data_processed/ remains output territory, not raw upload territory")

    st.markdown("### Recommended Batch Folder Pattern")
    st.code("YYYYMMDD_HHMMSS_<batch_label>", language="text")

    st.markdown("### Recommended Flow")
    st.write("upload -> inbox -> profile -> manual review -> accepted/rejected/hold movement -> processing -> qa -> dashboard_export")
