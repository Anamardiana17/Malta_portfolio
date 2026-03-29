from __future__ import annotations

import streamlit as st
import pandas as pd

from services.artifact_resolver import resolve_artifacts
from services.export_packager import get_export_pack_summary


def render() -> None:
    st.subheader("Export / Reviewer Pack Panel")
    st.caption("Reviewer-facing export readiness for validated artifacts.")

    export_summary = get_export_pack_summary()
    st.info(f"Export status: {export_summary['status']} | {export_summary['note']}")

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
