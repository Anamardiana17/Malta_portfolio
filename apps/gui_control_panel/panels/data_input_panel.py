from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

from services.repo_paths import resolve_repo_path


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

    inbox_count = _count_batch_dirs("data_input/inbox")
    accepted_count = _count_batch_dirs("data_input/accepted")
    rejected_count = _count_batch_dirs("data_input/rejected")
    registry_rows = len(input_registry)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Inbox batches", inbox_count)
    c2.metric("Accepted batches", accepted_count)
    c3.metric("Rejected batches", rejected_count)
    c4.metric("Registry rows", registry_rows)

    st.markdown("### Input Layer Path Check")
    path_checks = {
        "data_input/inbox": resolve_repo_path("data_input/inbox").exists(),
        "data_input/accepted": resolve_repo_path("data_input/accepted").exists(),
        "data_input/rejected": resolve_repo_path("data_input/rejected").exists(),
        "data_input/registry/input_registry.csv": resolve_repo_path("data_input/registry/input_registry.csv").exists(),
        "data_input/registry/processing_history.csv": resolve_repo_path("data_input/registry/processing_history.csv").exists(),
    }
    path_df = pd.DataFrame(
        [{"path": k, "exists": v} for k, v in path_checks.items()]
    )
    st.dataframe(path_df, width="stretch")

    st.markdown("### Input Registry")
    if input_registry.empty:
        st.info("No uploaded batch has been registered yet.")
    else:
        st.dataframe(input_registry, width="stretch")

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
