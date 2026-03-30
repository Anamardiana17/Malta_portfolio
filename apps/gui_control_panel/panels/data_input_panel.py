from __future__ import annotations

import pandas as pd
import streamlit as st

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
            "review_notes",
        ],
    )

    inbox_count = _count_batch_dirs("data_input/inbox")
    accepted_count = _count_batch_dirs("data_input/accepted")
    rejected_count = _count_batch_dirs("data_input/rejected")
    registry_rows = len(input_registry)
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

    if st.button("Register new batch", type="primary", use_container_width=True):
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

    if st.button("Profile inbox batch", use_container_width=True):
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
        help="Manual reviewer decision is persisted to registry/log only. No batch movement is triggered here.",
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

            if st.button("Save manual review outcome", use_container_width=True):
                if not reviewer_name.strip():
                    st.error("Reviewer name is required.")
                else:
                    summary = build_batch_decision_summary(
                        batch_id=review_batch_id.strip(),
                        profile_df=review_profile_df,
                    )
                    record_manual_acceptance_review(
                        batch_id=review_batch_id.strip(),
                        reviewer_name=reviewer_name.strip(),
                        review_outcome=reviewer_outcome,
                        review_notes=reviewer_notes.strip(),
                        summary=summary,
                    )
                    st.success(
                        "Manual review outcome saved. Batch remains in inbox until a future movement layer is explicitly implemented."
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
        st.info("No uploaded batch has been registered yet.")
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
        st.info("No processing history has been recorded yet.")
    else:
        st.dataframe(processing_history, width="stretch")

    st.markdown("### Batch Governance Policy")
    st.write("- New datasets should enter through data_input/inbox/")
    st.write("- Each upload should have its own timestamped batch folder")
    st.write("- Each batch should carry upload date and upload time")
    st.write("- Processing should start only after validation / manual acceptance review")
    st.write("- Manual acceptance review does not move inbox batches")
    st.write("- data_processed/ remains output territory, not raw upload territory")

    st.markdown("### Recommended Batch Folder Pattern")
    st.code("YYYYMMDD_HHMMSS_<batch_label>", language="text")

    st.markdown("### Recommended Flow")
    st.write("upload -> inbox -> profile -> manual review -> future acceptance/rejection movement -> processing -> qa -> dashboard_export")
