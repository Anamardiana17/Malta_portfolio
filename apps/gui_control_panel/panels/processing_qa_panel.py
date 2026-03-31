from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

from services.accepted_batch_registry import (
    accepted_batch_exists,
    list_accepted_batch_ids,
    load_processing_history,
)
from services.artifact_resolver import resolve_artifacts
from services.processing_history_logger import append_processing_history
from services.repo_paths import resolve_repo_path
from services.qa_runner import get_qa_status_summary


def _format_mtime(path_str: str | None) -> str:
    if not path_str:
        return "-"
    path = Path(path_str)
    if not path.exists():
        return "-"
    return pd.Timestamp(path.stat().st_mtime, unit="s").strftime("%Y-%m-%d %H:%M:%S")


def render() -> None:
    st.subheader("Processing / QA Panel")
    st.caption("Artifact readiness, governance visibility, and QA wrapper layer.")

    accepted_batch_ids = list_accepted_batch_ids()
    processing_history = load_processing_history()

    st.markdown("### Accepted Batch Processing Gate")
    if not accepted_batch_ids:
        st.warning(
            "No accepted batch is currently available. Processing should remain blocked until at least one batch passes manual acceptance review."
        )
    else:
        selected_batch_id = st.selectbox(
            "Accepted batch eligible for processing",
            options=accepted_batch_ids,
            index=len(accepted_batch_ids) - 1,
            help="Only batches inside data_input/accepted/ are eligible for downstream processing.",
        )

        is_eligible = accepted_batch_exists(selected_batch_id)
        gate_status = "eligible_for_processing" if is_eligible else "blocked"

        c_gate_1, c_gate_2, c_gate_3 = st.columns(3)
        c_gate_1.metric("Accepted batches", len(accepted_batch_ids))
        c_gate_2.metric("Selected batch", selected_batch_id)
        c_gate_3.metric("Gate status", gate_status)

        if is_eligible:
            st.success(f"Processing gate open for accepted batch: {selected_batch_id}")
        else:
            st.error(f"Selected batch is not eligible for processing: {selected_batch_id}")

        st.markdown("### Controlled Processing Trigger")
        processing_step = st.selectbox(
            "Processing step",
            options=[
                "gui_processing_gate_check",
                "gui_processing_trigger",
                "gui_qa_review_checkpoint",
            ],
            index=1,
        )
        processing_note = st.text_area(
            "Processing note",
            placeholder="Document why this accepted batch is being triggered for processing.",
        )

        if st.button("Log processing trigger", use_container_width=True):
            if not is_eligible:
                st.error("Processing trigger is blocked because the selected batch is not eligible.")
            else:
                append_processing_history(
                    batch_id=selected_batch_id,
                    processing_step=processing_step,
                    script_name="gui_control_panel_manual_trigger",
                    result_status="trigger_logged",
                    output_folder="pending_processing_output",
                    qa_status="pending",
                    note=processing_note.strip() or "Accepted batch manually triggered from GUI processing panel.",
                )
                st.success(
                    f"Processing trigger logged for accepted batch: {selected_batch_id}"
                )

    st.markdown("### Processing History")
    if processing_history.empty:
        st.info("No processing history has been recorded yet.")
    else:
        st.dataframe(processing_history, width="stretch")

    qa_summary = get_qa_status_summary()
    st.info(f"QA status: {qa_summary['status']} | {qa_summary['note']}")

    resolved = resolve_artifacts()

    rows = []
    for name, spec in resolved.items():
        rows.append(
            {
                "artifact": name,
                "panel": spec["panel"],
                "role": spec["role"],
                "exists": spec["exists"],
                "active_path": spec["active_path"],
                "resolved_path": spec["resolved_path"],
                "last_modified": _format_mtime(spec["resolved_path"]),
            }
        )

    artifact_df = pd.DataFrame(rows)

    total_artifacts = len(artifact_df)
    ready_artifacts = int(artifact_df["exists"].sum()) if not artifact_df.empty else 0
    management_ready = int(
        artifact_df["active_path"].astype(str).str.startswith("data_processed/management/").sum()
    ) if not artifact_df.empty else 0
    dashboard_ready = int(
        artifact_df["active_path"].astype(str).str.startswith("data_processed/dashboard_export/").sum()
    ) if not artifact_df.empty else 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Tracked artifacts", total_artifacts)
    c2.metric("Ready artifacts", ready_artifacts)
    c3.metric("Management-layer tracked", management_ready)
    c4.metric("Dashboard-export tracked", dashboard_ready)

    st.markdown("### Artifact Readiness Matrix")
    readiness_cols = [
        "artifact",
        "panel",
        "role",
        "exists",
        "active_path",
        "last_modified",
    ]
    st.dataframe(
        artifact_df[readiness_cols].sort_values(
            by=["panel", "role", "artifact"],
            ascending=[True, True, True],
        ),
        width="stretch",
    )

    st.markdown("### Executive and Decision Artifact Status")
    core_panels = {"executive_dashboard", "decision_support"}
    core_df = artifact_df[artifact_df["panel"].isin(core_panels)].copy()
    st.dataframe(
        core_df[
            [
                "artifact",
                "panel",
                "role",
                "exists",
                "active_path",
                "last_modified",
            ]
        ].sort_values(by=["panel", "artifact"]),
        width="stretch",
    )

    st.markdown("### Governance / Input Support Artifacts")
    governance_df = artifact_df[artifact_df["panel"] == "data_input"].copy()
    if governance_df.empty:
        st.warning("No governance support artifacts registered.")
    else:
        st.dataframe(
            governance_df[
                [
                    "artifact",
                    "role",
                    "exists",
                    "active_path",
                    "last_modified",
                ]
            ].sort_values(by=["artifact"]),
            width="stretch",
        )

    st.markdown("### Source Policy")
    st.write("- Active pipeline outputs only")
    st.write("- No backups as GUI default source")
    st.write("- No snapshots as GUI default source")
    st.write("- GUI remains a control/presentation layer only")

    st.markdown("### Repo Path Check")
    repo_checks = {
        "management_dir": resolve_repo_path("data_processed/management").exists(),
        "dashboard_export_dir": resolve_repo_path("data_processed/dashboard_export").exists(),
        "gui_app_dir": resolve_repo_path("apps/gui_control_panel").exists(),
        "gui_docs_dir": resolve_repo_path("docs/gui").exists(),
    }
    repo_check_df = pd.DataFrame(
        [{"path_check": k, "exists": v} for k, v in repo_checks.items()]
    )
    st.dataframe(repo_check_df, width="stretch")
