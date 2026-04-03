from __future__ import annotations

from pathlib import Path
from typing import Dict

import pandas as pd

from services.processing_execution_logger import load_processing_execution_log


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def _clean(value) -> str:
    if value is None:
        return "-"
    try:
        if pd.isna(value):
            return "-"
    except Exception:
        pass
    text = str(value).strip()
    return text if text else "-"


def _format_output_folder_label(value) -> str:
    text = _clean(value)
    mapping = {
        "pending_processing_output": "Pending processing output (active artifact context)",
        "output": "Output",
        "-": "-",
    }
    return mapping.get(text, text)


def _latest_from_execution_log() -> Dict[str, str]:
    df = load_processing_execution_log()
    if df.empty:
        return {}

    scoped = df.copy().reset_index(drop=False).rename(columns={"index": "_row_order"})
    scoped["execution_event_ts"] = pd.to_datetime(scoped["execution_event_ts"], errors="coerce")

    status_rank = {
        "execution_completed": 3,
        "execution_started": 2,
        "trigger_recorded": 1,
    }
    scoped["_status_rank"] = scoped["execution_status"].astype(str).map(status_rank).fillna(0)

    scoped = scoped.sort_values(
        by=["execution_event_ts", "_status_rank", "_row_order"],
        ascending=[False, False, False],
        na_position="last",
    )

    row = scoped.iloc[0]
    return {
        "source_batch_id": _clean(row.get("batch_id")),
        "latest_execution_status": _clean(row.get("execution_status")),
        "latest_output_folder": _format_output_folder_label(row.get("output_folder")),
        "latest_processing_step": _clean(row.get("processing_step")),
        "execution_event_ts": _clean(row.get("execution_event_ts")),
        "context_source": "processing_execution_log.csv",
    }


def _latest_from_trigger_registry() -> Dict[str, str]:
    path = _repo_root() / "data_input" / "registry" / "processing_trigger_registry.csv"
    df = _safe_read_csv(path)
    if df.empty:
        return {}

    time_col = None
    for candidate in ["trigger_event_ts", "triggered_at", "executed_at", "completed_at", "created_at"]:
        if candidate in df.columns:
            time_col = candidate
            break

    scoped = df.copy().reset_index(drop=False).rename(columns={"index": "_row_order"})
    if time_col:
        scoped[time_col] = pd.to_datetime(scoped[time_col], errors="coerce")
        scoped = scoped.sort_values(
            by=[time_col, "_row_order"],
            ascending=[False, False],
            na_position="last",
        )
    else:
        scoped = scoped.sort_values(by=["_row_order"], ascending=[False])

    row = scoped.iloc[0]
    return {
        "source_batch_id": _clean(row.get("batch_id")),
        "latest_execution_status": _clean(
            row.get("trigger_status")
            or row.get("processing_result_status")
            or row.get("result_status")
        ),
        "latest_output_folder": _format_output_folder_label(
            row.get("latest_output_folder")
            or row.get("output_folder")
            or row.get("output_path")
        ),
        "latest_processing_step": _clean(
            row.get("latest_processing_step")
            or row.get("processing_step")
            or row.get("step_name")
        ),
        "execution_event_ts": _clean(
            row.get(time_col) if time_col else row.get("created_at")
        ),
        "context_source": "processing_trigger_registry.csv",
    }


def get_active_processing_context() -> Dict[str, str]:
    context = _latest_from_execution_log()
    if not context:
        context = _latest_from_trigger_registry()

    if not context:
        context = {
            "source_batch_id": "-",
            "latest_execution_status": "-",
            "latest_output_folder": "-",
            "latest_processing_step": "-",
            "execution_event_ts": "-",
            "context_source": "no_processing_context_available",
        }

    context["month_context_note"] = (
        "Selected analytical month_id is read from active artifact outputs and is not "
        "presented as a direct upload-batch date or exact batch-to-month lineage."
    )
    context["output_context_note"] = (
        "Active artifact output folder reflects the currently resolved artifact context "
        "used by the dashboard view."
    )
    return context
