from __future__ import annotations

import streamlit as st

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
