from __future__ import annotations

import pandas as pd
import streamlit as st

from services.batch_creation_helper import (
    BatchCreationError,
    UploadedFilePayload,
    create_batch,
)
from services.batch_schema_profiler import profile_batch
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

    inbox_count = _count_batch_dirs("data_input/inbox")
    accepted_count = _count_batch_dirs("data_input/accepted")
    rejected_count = _count_batch_dirs("data_input/rejected")
    registry_rows = len(input_registry)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Inbox batches", inbox_count)
    c2.metric("Accepted batches", accepted_count)
    c3.metric("Rejected batches", rejected_count)
    c4.metric("Registry rows", registry_rows)

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

    st.markdown("### Input Layer Path Check")
    path_checks = {
        "data_input/inbox": resolve_repo_path("data_input/inbox").exists(),
        "data_input/accepted": resolve_repo_path("data_input/accepted").exists(),
        "data_input/rejected": resolve_repo_path("data_input/rejected").exists(),
        "data_input/registry/input_registry.csv": resolve_repo_path("data_input/registry/input_registry.csv").exists(),
        "data_input/registry/processing_history.csv": resolve_repo_path("data_input/registry/processing_history.csv").exists(),
        "data_input/registry/upload_log.csv": resolve_repo_path("data_input/registry/upload_log.csv").exists(),
    }
    path_df = pd.DataFrame([{"path": k, "exists": v} for k, v in path_checks.items()])
    st.dataframe(path_df, width="stretch")

    st.markdown("### Input Registry")
    if input_registry.empty:
        st.info("No uploaded batch has been registered yet.")
    else:
        st.dataframe(input_registry, width="stretch")

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
    st.write("- Processing should start only after validation / acceptance")
    st.write("- data_processed/ remains output territory, not raw upload territory")

    st.markdown("### Recommended Batch Folder Pattern")
    st.code("YYYYMMDD_HHMMSS_<batch_label>", language="text")

    st.markdown("### Recommended Flow")
    st.write("upload -> inbox -> validate -> accepted/rejected -> processing -> qa -> dashboard_export")
