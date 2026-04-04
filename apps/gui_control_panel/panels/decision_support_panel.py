from __future__ import annotations

import pandas as pd
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


from services.artifact_loader import load_artifact_df
from services.active_processing_context import (
    build_month_context_integrity_summary,
    get_active_processing_context,
)


def _safe_metric_value(value):
    if pd.isna(value):
        return "-"
    if isinstance(value, float):
        return f"{value:,.2f}"
    return value


def _priority_row_style(row: pd.Series) -> list[str]:
    priority = str(row.get("execution_priority", "")).strip().lower()
    if "critical" in priority:
        color = "background-color: rgba(255, 99, 71, 0.30);"
    elif "high" in priority:
        color = "background-color: rgba(255, 165, 0, 0.28);"
    elif "medium" in priority:
        color = "background-color: rgba(255, 215, 0, 0.22);"
    else:
        color = ""
    return [color] * len(row)


def render() -> None:
    st.subheader("Decision Support Panel")
    st.caption("Manager action queue, therapist coaching priorities, and commercial signal translation.")

    action_df = load_artifact_df("dashboard_manager_action_queue")
    coaching_df = load_artifact_df("dashboard_therapist_coaching")
    manager_df = load_artifact_df("manager_action_queue")

    if action_df is None or coaching_df is None or manager_df is None:
        st.error("Required decision-support artifacts are missing.")
        return

    action_df = action_df.copy()
    coaching_df = coaching_df.copy()
    manager_df = manager_df.copy()

    context = get_active_processing_context()

    st.markdown("### Active Governed Processing Context")
    cx1, cx2, cx3 = st.columns(3)
    cx1.metric("Source batch_id", context["source_batch_id"])
    cx2.metric("Latest execution status", context["latest_execution_status"])
    cx3.metric("Active artifact output folder", context["latest_output_folder"])

    st.caption(
        f"Processing step: {context['latest_processing_step']} | "
        f"Event time: {context['execution_event_ts']} | "
        f"Context source: {context['context_source']}"
    )
    st.info(context["month_context_note"])
    st.caption(context["output_context_note"])

    month_candidates = pd.concat(
        [
            action_df["month_id"].astype(str),
            coaching_df["month_id"].astype(str),
            manager_df["month_id"].astype(str),
        ],
        ignore_index=True,
    ).dropna()

    month_options = sorted(month_candidates.unique().tolist())
    selected_month = st.selectbox(
        "Select analytical month_id",
        month_options,
        index=len(month_options) - 1 if month_options else 0,
        key="decision_month_id",
    )

    action_view = action_df[action_df["month_id"].astype(str) == selected_month].copy()
    coaching_view = coaching_df[coaching_df["month_id"].astype(str) == selected_month].copy()
    manager_view = manager_df[manager_df["month_id"].astype(str) == selected_month].copy()

    month_integrity = build_month_context_integrity_summary(
        selected_month,
        {
            "dashboard_manager_action_queue": action_df,
            "dashboard_therapist_coaching": coaching_df,
            "manager_action_queue": manager_df,
        },
    )

    st.markdown("### Active Context Integrity / Coverage Summary")
    ix1, ix2, ix3, ix4 = st.columns(4)
    ix1.metric("Selected analytical month_id", month_integrity["selected_month_id"])
    ix2.metric("Artifacts checked", month_integrity["artifacts_checked"])
    ix3.metric("Artifacts with month_id", month_integrity["artifacts_with_month_column"])
    ix4.metric("Artifacts matching selected month_id", month_integrity["artifacts_matching_month"])

    st.caption(
        f"Integrity status: {month_integrity['integrity_status']} | "
        f"Source batch_id: {context['source_batch_id']}"
    )
    st.info(month_integrity["reviewer_note"])
    st.caption(month_integrity["boundary_note"])
    st.caption("Decision suggestions should be read within the coverage status shown above.")

    with st.expander("Reviewer coverage diagnostics"):
        st.write(f"Missing / empty artifacts: {month_integrity['missing_artifacts']}")
        st.write(
            "Artifacts without month_id column: "
            f"{month_integrity['artifacts_without_month_column']}"
        )
        st.write(
            "Artifacts without selected month_id: "
            f"{month_integrity['artifacts_without_selected_month']}"
        )

    total_actions = len(action_view)
    critical_actions = (
        action_view["execution_priority"].astype(str).str.contains("high|critical", case=False, na=False).sum()
        if not action_view.empty
        else 0
    )
    coach_priority_count = (
        coaching_view["coach_priority_flag"].astype(str).str.lower().eq("yes").sum()
        if "coach_priority_flag" in coaching_view.columns
        else 0
    )
    burnout_risk_count = (
        coaching_view["burnout_risk_flag"].astype(str).str.lower().eq("yes").sum()
        if "burnout_risk_flag" in coaching_view.columns
        else 0
    )

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Actions in scope", total_actions)
    c2.metric("High / critical actions", int(critical_actions))
    c3.metric("Coach priority therapists", int(coach_priority_count))
    c4.metric("Burnout risk flagged", int(burnout_risk_count))

    st.caption(
        "How to read these cards: use them as a quick signal for current management load. They show whether the "
        "active month looks more stable or more pressured, but action should still be prioritized only after "
        "checking context integrity and the ranked queue below."
    )

    st.markdown("### Ranked Action Priority View")
    if action_view.empty:
        st.info("No action rows available for the selected analytical month.")
    else:
        action_chart = action_view.copy()
        action_chart["execution_priority_score"] = pd.to_numeric(
            action_chart["execution_priority_score"], errors="coerce"
        )
        action_chart["action_label"] = (
            action_chart["outlet_name"].astype(str) + " | " + action_chart["action_type"].astype(str)
        )
        action_chart = action_chart.sort_values(
            by=["execution_priority_score", "action_priority_rank"],
            ascending=[False, True],
        ).head(10)

        if not action_chart.empty:
            st.bar_chart(
                action_chart.set_index("action_label")[["execution_priority_score"]],
                height=320,
            )

        st.caption(
            "How to read this chart: higher bars mean higher action priority for the current analytical month. "
            "Read from top to bottom and treat the highest-ranked items as first review candidates, especially "
            "when execution priority and team impact both point to pressure."
        )

    st.markdown("### Manager Action Queue")
    action_type_options = ["All"] + sorted(action_view["action_type"].dropna().astype(str).unique().tolist())
    selected_action_type = st.selectbox("Filter action_type", action_type_options)

    filtered_action_view = action_view.copy()
    if selected_action_type != "All":
        filtered_action_view = filtered_action_view[
            filtered_action_view["action_type"].astype(str) == selected_action_type
        ]

    action_cols = [
        "action_priority_rank",
        "outlet_name",
        "action_type",
        "action_theme",
        "management_signal",
        "recommended_action",
        "revenue_impact_direction",
        "team_impact_direction",
        "execution_priority_score",
        "execution_priority",
        "manager_note",
    ]

    styled_action_view = filtered_action_view[action_cols].sort_values(
        by=["action_priority_rank", "execution_priority_score"],
        ascending=[True, False],
    )

    st.dataframe(
        styled_action_view.style.apply(_priority_row_style, axis=1),
        width="stretch",
        hide_index=True,
    )

    st.caption(
        "How to read this action table: highlighted rows represent management attention priority. Combine the row "
        "priority with integrity notes, management signal, and team impact before acting. Do not treat a highlighted "
        "row as an automatic instruction without managerial review."
    )

    st.markdown("### Therapist Coaching View")
    outlet_options = ["All"] + sorted(coaching_view["outlet_name"].dropna().astype(str).unique().tolist())
    selected_outlet = st.selectbox("Filter coaching by outlet", outlet_options)

    filtered_coaching_view = coaching_view.copy()
    if selected_outlet != "All":
        filtered_coaching_view = filtered_coaching_view[
            filtered_coaching_view["outlet_name"].astype(str) == selected_outlet
        ]

    coaching_priority_options = ["All"] + sorted(
        filtered_coaching_view["coaching_priority_band"].dropna().astype(str).unique().tolist()
    )
    selected_priority_band = st.selectbox("Filter coaching_priority_band", coaching_priority_options)

    if selected_priority_band != "All":
        filtered_coaching_view = filtered_coaching_view[
            filtered_coaching_view["coaching_priority_band"].astype(str) == selected_priority_band
        ]

    coaching_cols = [
        "therapist_name",
        "outlet_name",
        "therapist_role",
        "therapist_consistency_score_0_100",
        "therapist_consistency_band",
        "coaching_priority_band",
        "coach_priority_flag",
        "burnout_risk_flag",
        "upsell_score_0_100",
        "total_commercial_score_0_100",
        "bonus_reward_eligibility_flag",
        "refresh_training_required_flag",
        "coaching_action_recommendation",
    ]
    st.dataframe(
        filtered_coaching_view[coaching_cols].sort_values(
            by=["therapist_consistency_score_0_100", "total_commercial_score_0_100"],
            ascending=[True, False],
        ),
        width="stretch",
    )

    st.caption(
        "How to read this coaching view: use it to identify coaching, recognition, and strain patterns across therapists. "
        "It should support people-management prioritization, not punitive judgment."
    )

    st.markdown("### Therapist Detail")
    therapist_options = filtered_coaching_view["therapist_name"].dropna().astype(str).unique().tolist()
    if therapist_options:
        selected_therapist = st.selectbox("Select therapist", therapist_options)
        therapist_row = filtered_coaching_view[
            filtered_coaching_view["therapist_name"].astype(str) == selected_therapist
        ].head(1)

        if not therapist_row.empty:
            row = therapist_row.iloc[0]

            t1, t2, t3, t4 = st.columns(4)
            t1.metric("Consistency score", _safe_metric_value(row.get("therapist_consistency_score_0_100")))
            t2.metric("Consistency band", _safe_metric_value(row.get("therapist_consistency_band")))
            t3.metric("Commercial score", _safe_metric_value(row.get("total_commercial_score_0_100")))
            t4.metric("Upsell score", _safe_metric_value(row.get("upsell_score_0_100")))

            t5, t6, t7, t8 = st.columns(4)
            t5.metric("Utilization %", _safe_metric_value(row.get("utilization_percent")))
            t6.metric("Yield / sold hour", _safe_metric_value(row.get("yield_eur_per_sold_hour")))
            t7.metric("RevPATH proxy", _safe_metric_value(row.get("revpath_proxy_eur_per_available_hour")))
            t8.metric("Rebooking %", _safe_metric_value(row.get("rebooking_rate_percent")))

            st.markdown("**Coaching action recommendation**")
            st.write(row.get("coaching_action_recommendation", "-"))

            st.markdown("**Managerial story**")
            st.write(row.get("managerial_story", "-"))

            reward_flags = {
                "bonus_reward_eligibility_flag": row.get("bonus_reward_eligibility_flag", "-"),
                "refresh_training_required_flag": row.get("refresh_training_required_flag", "-"),
                "top3_therapist_flag": row.get("top3_therapist_flag", "-"),
                "bottom3_therapist_flag": row.get("bottom3_therapist_flag", "-"),
            }
            st.markdown("**Therapist reward / coaching flags**")
            st.json(_format_gui_flag_dict(reward_flags))
    else:
        st.info("No therapist rows available for the current filters.")

    st.markdown("### Management Queue Snapshot")
    manager_cols = [
        "action_priority_rank",
        "outlet_name",
        "action_type",
        "action_theme",
        "recommended_action",
        "execution_priority_score",
        "execution_priority",
    ]
    st.dataframe(
        manager_view[manager_cols].sort_values(
            by=["action_priority_rank", "execution_priority_score"],
            ascending=[True, False],
        ),
        width="stretch",
    )

    st.caption(
        "How to read this management snapshot: use it as a compact queue view after reviewing the ranked chart and "
        "styled action table above. Higher-priority items should be reviewed first, but action should remain anchored "
        "to internal operating truth and the guardrails shown in context integrity notes."
    )
