from __future__ import annotations
from pathlib import Path

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

import pandas as pd

from services.artifact_resolver import resolve_artifacts
from services.export_packager import get_export_pack_summary
from services.processing_execution_logger import (
    get_latest_execution_event,
    has_execution_log,
)


def render() -> None:
    st.subheader("Export / Reviewer Pack Panel")
    st.caption("Reviewer-facing export readiness for validated artifacts.")

    export_summary = get_export_pack_summary()
    st.info("Export status: reviewer-pack packaging pending final connection | Validated dashboard and management artifacts are reserved for governed packaging only.")

    resolved = resolve_artifacts()
    rows = []
    for name, spec in resolved.items():
        if name.startswith("dashboard_") or name in {
            "outlet_management_summary",
            "manager_action_queue",
            "therapist_coaching_summary",
        }:
            rows.append(
                {
                    "artifact": name,
                    "exists": spec["exists"],
                    "active_path": spec["active_path"],
                    "resolved_path": spec["resolved_path"],
                }
            )

    st.dataframe(pd.DataFrame(rows), width="stretch")


    st.markdown("---")
    st.subheader("End-to-End Batch Governance Review Pack")

    review_pack_df = _build_batch_governance_review_pack()

    if review_pack_df.empty:
        st.info("No governed batch review-pack evidence is available yet.")
    else:
        available_batch_ids = review_pack_df["batch_id"].astype(str).tolist()
        selected_batch_id = st.selectbox(
            "Select governed batch_id",
            options=available_batch_ids,
            key="review_pack_batch_id",
        )

        selected_row = review_pack_df.loc[
            review_pack_df["batch_id"].astype(str) == str(selected_batch_id)
        ].iloc[0]

        compact_summary = pd.DataFrame([
            {"field": "batch_id", "value": selected_row["batch_id"]},
            {"field": "last_reviewed_at", "value": selected_row["last_reviewed_at"]},
            {"field": "reviewer_status_recommendation", "value": selected_row["reviewer_status_recommendation"]},
            {"field": "review_outcome_label", "value": selected_row["review_outcome_label"]},
            {"field": "batch_location", "value": selected_row["batch_location"]},
            {"field": "movement_status", "value": selected_row["movement_status"]},
            {"field": "movement_note", "value": selected_row["movement_note"]},
            {"field": "latest_result_status", "value": selected_row["latest_result_status"]},
            {"field": "latest_qa_status", "value": selected_row["latest_qa_status"]},
            {"field": "latest_output_folder", "value": selected_row["latest_output_folder"]},
            {"field": "latest_processing_step", "value": selected_row["latest_processing_step"]},
            {"field": "review_notes", "value": selected_row["review_notes"]},
        ])

        st.dataframe(compact_summary, width="stretch", hide_index=True)
        st.caption(selected_row["governance_interpretation"])


def _safe_read_csv(path_str: str) -> pd.DataFrame:
    path = Path(path_str)
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def _pick_latest_row(df: pd.DataFrame, batch_id: str, time_col_candidates: list[str]):
    if df.empty or "batch_id" not in df.columns:
        return None

    scoped = df[df["batch_id"].astype(str) == str(batch_id)].copy()
    if scoped.empty:
        return None

    chosen_time_col = None
    for col in time_col_candidates:
        if col in scoped.columns:
            chosen_time_col = col
            break

    if chosen_time_col:
        scoped[chosen_time_col] = pd.to_datetime(scoped[chosen_time_col], errors="coerce")
        scoped = scoped.sort_values(by=chosen_time_col, ascending=False, na_position="last")

    return scoped.iloc[0]


def _pull_value(row, *cols) -> str:
    if row is None:
        return ""

    for col in cols:
        value = None
        found = False

        if isinstance(row, dict):
            if col in row:
                value = row.get(col)
                found = True
        else:
            row_index = getattr(row, "index", [])
            if col in row_index:
                value = row[col]
                found = True

        if not found:
            continue
        if pd.isna(value):
            continue
        return str(value)

    return ""


def _build_batch_governance_review_pack() -> pd.DataFrame:
    acceptance_registry = _safe_read_csv("data_input/registry/acceptance_review_registry.csv")
    acceptance_log = _safe_read_csv("data_input/registry/acceptance_review_log.csv")
    processing_trigger_registry = _safe_read_csv("data_input/registry/processing_trigger_registry.csv")
    processing_execution_log = _safe_read_csv("data_input/registry/processing_execution_log.csv")

    batch_ids = set()
    for df in [acceptance_registry, acceptance_log, processing_trigger_registry, processing_execution_log]:
        if not df.empty and "batch_id" in df.columns:
            batch_ids.update(df["batch_id"].dropna().astype(str).unique().tolist())

    rows = []

    for batch_id in sorted(batch_ids):
        acceptance_row = _pick_latest_row(
            acceptance_registry,
            batch_id,
            ["last_reviewed_at", "reviewed_at", "created_at"]
        )
        if acceptance_row is None:
            acceptance_row = _pick_latest_row(
                acceptance_log,
                batch_id,
                ["last_reviewed_at", "reviewed_at", "created_at"]
            )

        processing_row = _pick_latest_row(
            processing_execution_log,
            batch_id,
            ["execution_event_ts", "executed_at", "completed_at", "triggered_at", "created_at"]
        )
        if processing_row is None:
            processing_row = _pick_latest_row(
                processing_trigger_registry,
                batch_id,
                ["trigger_event_ts", "triggered_at", "executed_at", "completed_at", "created_at"]
            )

        review_outcome_label = _pull_value(
            acceptance_row,
            "review_outcome_label",
            "manual_review_outcome",
            "review_outcome"
        )
        execution_row = get_latest_execution_event(batch_id) if has_execution_log() else None
        latest_result_status = _pull_value(
            execution_row or processing_row,
            "latest_result_status",
            "execution_status",
            "result_status",
            "processing_result_status",
            "trigger_status"
        )

        if review_outcome_label and latest_result_status:
            interpretation = (
                f"Batch {batch_id} has a recorded governance review outcome of {review_outcome_label} "
                f"and the latest controlled processing result is {latest_result_status}."
            )
        elif review_outcome_label:
            interpretation = (
                f"Batch {batch_id} has reviewer-facing acceptance evidence recorded as {review_outcome_label} "
                f"and is awaiting later controlled processing evidence."
            )
        elif latest_result_status:
            interpretation = (
                f"Batch {batch_id} has controlled processing evidence with result status {latest_result_status}, "
                f"but reviewer-facing acceptance evidence appears incomplete."
            )
        else:
            interpretation = (
                f"Batch {batch_id} has limited end-to-end governance evidence and should remain under governed review."
            )

        rows.append(
            {
                "batch_id": str(batch_id),
                "last_reviewed_at": _pull_value(acceptance_row, "last_reviewed_at", "reviewed_at", "created_at"),
                "reviewer_status_recommendation": _pull_value(
                    acceptance_row,
                    "reviewer_status_recommendation",
                    "status_recommendation",
                    "review_recommendation"
                ),
                "review_outcome_label": review_outcome_label,
                "batch_location": _pull_value(
                    acceptance_row,
                    "batch_location",
                    "current_batch_location",
                    "batch_path"
                ),
                "movement_status": _pull_value(acceptance_row, "movement_status"),
                "movement_note": _pull_value(acceptance_row, "movement_note"),
                "latest_result_status": latest_result_status,
                "latest_qa_status": _pull_value(
                    processing_row,
                    "latest_qa_status",
                    "qa_status",
                    "processing_qa_status",
                    "eligibility_status"
                ),
                "latest_output_folder": _pull_value(
                    execution_row or processing_row,
                    "output_folder",
                    "latest_output_folder",
                    "output_path",
                    "operator_note"
                ),
                "latest_processing_step": _pull_value(
                    execution_row or processing_row,
                    "processing_step",
                    "latest_processing_step",
                    "step_name"
                ),
                "execution_event_ts": _pull_value(
                    execution_row,
                    "execution_event_ts"
                ),
                "execution_status": _pull_value(
                    execution_row,
                    "execution_status",
                    "trigger_status"
                ),
                "qa_status": _pull_value(
                    execution_row,
                    "qa_status"
                ),
                "execution_note": _pull_value(
                    execution_row,
                    "execution_note",
                    "note",
                    "trigger_note",
                    "operator_note"
                ),
                "processing_evidence_source": (
                    "processing_execution_log.csv"
                    if execution_row
                    else "processing_trigger_registry.csv"
                ),
                "review_notes": _pull_value(
                    acceptance_row,
                    "review_notes",
                    "reviewer_notes",
                    "notes"
                ),
                "governance_interpretation": interpretation,
            }
        )

    return pd.DataFrame(rows)
