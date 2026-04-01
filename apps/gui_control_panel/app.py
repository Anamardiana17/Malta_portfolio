from __future__ import annotations

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


from panels import (
    data_input_panel,
    processing_qa_panel,
    executive_dashboard_panel,
    decision_support_panel,
    export_reviewer_pack_panel,
)
from utils.guards import gui_guardrail_notes


st.set_page_config(page_title="Malta GUI Control Panel", layout="wide")

st.title("Malta Portfolio - KPI-Governed Operations Control Panel")
st.caption("Operating/control layer above validated Malta_portfolio artifacts.")

with st.sidebar:
    st.header("Panels")
    selected_panel = st.radio(
        "Select panel",
        [
            "Data Input Panel",
            "Processing / QA Panel",
            "KPI / Executive Dashboard Panel",
            "Decision Support Panel",
            "Export / Reviewer Pack Panel",
        ],
    )

    st.header("Guardrails")
    for note in gui_guardrail_notes():
        st.write(f"- {note}")

if selected_panel == "Data Input Panel":
    data_input_panel.render()
elif selected_panel == "Processing / QA Panel":
    processing_qa_panel.render()
elif selected_panel == "KPI / Executive Dashboard Panel":
    executive_dashboard_panel.render()
elif selected_panel == "Decision Support Panel":
    decision_support_panel.render()
elif selected_panel == "Export / Reviewer Pack Panel":
    export_reviewer_pack_panel.render()
