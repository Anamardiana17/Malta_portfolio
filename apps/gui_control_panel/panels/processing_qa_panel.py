from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

from services.accepted_batch_registry import (
    accepted_batch_exists,
    list_accepted_batch_ids,
)
from services.processing_execution_logger import load_processing_execution_log
from services.artifact_resolver import resolve_artifacts
from services.gui_processing_executor import execute_gui_processing_trigger
from services.repo_paths import resolve_repo_path
from services.qa_runner import get_qa_status_summary


def _format_mtime(path_str: str | None) -> str:
    if not path_str:
        return "-"
    path = Path(path_str)
    if not path.exists():
        return "-"
    return pd.Timestamp(path.stat().st_mtime, unit="s").strftime("%Y-%m-%d %H:%M:%S")


def _safe_str(value: object, default: str = "-") -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text if text else default


def _build_execution_evidence_summary(processing_history: pd.DataFrame) -> dict[str, object]:
    if processing_history.empty:
        return {
            "has_data": False,
            "latest_batch_id": "-",
            "latest_processing_step": "-",
            "latest_result_status": "-",
            "latest_qa_status": "-",
            "latest_output_folder": "-",
            "latest_note": "No controlled processing evidence has been recorded yet.",
            "latest_event_ts": "-",
            "total_events": 0,
            "completed_events": 0,
            "pending_events": 0,
            "latest_batch_event_count": 0,
            "governance_note": (
                "Processing execution evidence is not yet available. A reviewer cannot yet inspect a governed trigger-to-execution trail."
            ),
        }

    history = processing_history.fillna("").copy()
    latest_row = history.iloc[-1]

    latest_batch_id = _safe_str(latest_row.get("batch_id"))
    latest_processing_step = _safe_str(latest_row.get("processing_step"))
    latest_result_status = _safe_str(latest_row.get("execution_status"))
    latest_qa_status = _safe_str(latest_row.get("qa_status"))
    latest_output_folder = _safe_str(latest_row.get("output_folder"))
    latest_note = _safe_str(latest_row.get("note"))
    latest_event_ts = _safe_str(latest_row.get("execution_event_ts"))

    total_events = len(history)
    completed_events = int((history["execution_status"].astype(str) == "execution_completed").sum())
    pending_events = int((history["qa_status"].astype(str).isin(["pending", "pending_review"])).sum())
    latest_batch_event_count = int(
        (history["batch_id"].astype(str).str.strip() == latest_batch_id).sum()
    )

    governance_note = (
        f"The latest governed run for batch {latest_batch_id} reached result status "
        f"'{latest_result_status}' with QA status '{latest_qa_status}'. "
        "This supports reviewer visibility into controlled execution history without changing the core Malta pipeline."
    )

    return {
        "has_data": True,
        "latest_batch_id": latest_batch_id,
        "latest_processing_step": latest_processing_step,
        "latest_result_status": latest_result_status,
        "latest_qa_status": latest_qa_status,
        "latest_output_folder": latest_output_folder,
        "latest_note": latest_note,
        "latest_event_ts": latest_event_ts,
        "total_events": total_events,
        "completed_events": completed_events,
        "pending_events": pending_events,
        "latest_batch_event_count": latest_batch_event_count,
        "governance_note": governance_note,
    }


def render() -> None:
    st.subheader("Processing / QA Panel")
    st.caption("Artifact readiness, governance visibility, and QA wrapper layer.")

    accepted_batch_ids = list_accepted_batch_ids()
    processing_history = load_processing_execution_log()

    st.markdown("### Accepted Batch Processing Gate")
    selected_batch_id = None
    is_eligible = False
    gate_status = "blocked"

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

        if st.button("Run controlled processing trigger", use_container_width=True):
            if not is_eligible:
                st.error("Processing trigger is blocked because the selected batch is not eligible.")
            else:
                result = execute_gui_processing_trigger(
                    batch_id=selected_batch_id,
                    processing_step=processing_step,
                    operator_note=processing_note.strip(),
                )
                st.success(
                    f"Controlled processing execution recorded for accepted batch: {result.batch_id} | "
                    f"status={result.execution_status} | qa_status={result.qa_status}"
                )
                processing_history = load_processing_execution_log()

    st.markdown("### Recruiter-Facing Execution Evidence Summary")
    evidence = _build_execution_evidence_summary(processing_history)

    c_ev_1, c_ev_2, c_ev_3, c_ev_4 = st.columns(4)
    c_ev_1.metric("Latest batch", evidence["latest_batch_id"])
    c_ev_2.metric("Latest result", evidence["latest_result_status"])
    c_ev_3.metric("Latest QA status", evidence["latest_qa_status"])
    c_ev_4.metric("Recorded events", evidence["total_events"])

    c_ev_5, c_ev_6, c_ev_7 = st.columns(3)
    c_ev_5.metric("Completed events", evidence["completed_events"])
    c_ev_6.metric("Pending QA events", evidence["pending_events"])
    c_ev_7.metric("Latest batch events", evidence["latest_batch_event_count"])

    if selected_batch_id:
        batch_alignment = (
            "selected batch matches latest governed run"
            if selected_batch_id == evidence["latest_batch_id"]
            else "selected batch differs from latest governed run"
        )
        st.write(f"**Batch alignment:** {batch_alignment}")

    st.write(f"**Latest processing step:** {evidence['latest_processing_step']}")
    st.write(f"**Latest event timestamp:** {evidence['latest_event_ts']}")
    st.write(f"**Latest output folder:** {evidence['latest_output_folder']}")
    st.write(f"**Latest execution note:** {evidence['latest_note']}")

    st.info(evidence["governance_note"])
    st.caption(
        "This summary is reviewer-facing execution evidence only. It uses the governed processing_execution_log.csv layer and does not replace the core Malta processing pipeline or introduce synthetic intra-day logic."
    )

    st.markdown("### Execution Summary")
    if processing_history.empty:
        st.info("No processing execution summary is available yet.")
    else:
        latest_row = processing_history.iloc[-1].fillna("")
        total_processing_events = len(processing_history)
        last_batch_id = str(latest_row.get("batch_id", "-")).strip() or "-"
        last_result_status = str(latest_row.get("execution_status", "-")).strip() or "-"
        last_qa_status = str(latest_row.get("qa_status", "-")).strip() or "-"

        c_exec_1, c_exec_2, c_exec_3, c_exec_4 = st.columns(4)
        c_exec_1.metric("Accepted batches", len(accepted_batch_ids))
        c_exec_2.metric("Processing events", total_processing_events)
        c_exec_3.metric("Last result status", last_result_status)
        c_exec_4.metric("Last QA status", last_qa_status)

        st.write(f"**Last processed batch:** {last_batch_id}")

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
