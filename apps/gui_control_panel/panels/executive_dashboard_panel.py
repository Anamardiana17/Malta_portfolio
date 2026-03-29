from __future__ import annotations

import pandas as pd
import streamlit as st

from services.artifact_loader import load_artifact_df


def _safe_metric_value(value):
    if pd.isna(value):
        return "-"
    if isinstance(value, float):
        return f"{value:,.2f}"
    return value


def render() -> None:
    st.subheader("KPI / Executive Dashboard Panel")
    st.caption("Executive quick scan across outlet ranking, management signal, and commercial readiness.")

    ranking_df = load_artifact_df("dashboard_outlet_executive_ranking")
    summary_df = load_artifact_df("outlet_management_summary")

    if ranking_df is None or summary_df is None:
        st.error("Required executive artifacts are missing.")
        return

    ranking_df = ranking_df.copy()
    summary_df = summary_df.copy()

    month_options = sorted(ranking_df["month_id"].dropna().astype(str).unique().tolist())
    selected_month = st.selectbox("Select month_id", month_options, index=len(month_options) - 1 if month_options else 0)

    ranking_view = ranking_df[ranking_df["month_id"].astype(str) == selected_month].copy()
    summary_view = summary_df[summary_df["month_id"].astype(str) == selected_month].copy()

    if ranking_view.empty:
        st.warning("No executive ranking rows found for selected month.")
        return

    total_outlets = ranking_view["outlet_name"].nunique()
    avg_priority = ranking_view["executive_priority_score_0_100"].mean()
    total_reward_ready = ranking_view["therapist_bonus_reward_eligible_count"].sum()
    total_refresh_needed = ranking_view["therapist_refresh_training_required_count"].sum()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Outlets in scope", total_outlets)
    c2.metric("Avg executive priority score", f"{avg_priority:,.1f}")
    c3.metric("Bonus reward eligible", int(total_reward_ready))
    c4.metric("Refresh training required", int(total_refresh_needed))

    st.markdown("### Outlet Executive Leaderboard")
    leaderboard_cols = [
        "executive_rank_within_month",
        "outlet_name",
        "management_signal",
        "executive_priority_score_0_100",
        "executive_priority_band",
        "recommended_manager_action",
        "retail_reward_eligible_staff_count",
        "therapist_bonus_reward_eligible_count",
        "therapist_refresh_training_required_count",
    ]
    leaderboard = ranking_view[leaderboard_cols].sort_values(
        by=["executive_rank_within_month", "executive_priority_score_0_100"],
        ascending=[True, False],
    )
    st.dataframe(leaderboard, width="stretch")

    outlet_options = leaderboard["outlet_name"].dropna().unique().tolist()
    selected_outlet = st.selectbox("Select outlet", outlet_options)

    outlet_rank = ranking_view[ranking_view["outlet_name"] == selected_outlet].head(1)
    outlet_summary = summary_view[summary_view["outlet_name"] == selected_outlet].head(1)

    st.markdown("### Outlet Detail")
    if not outlet_rank.empty:
        row = outlet_rank.iloc[0]
        d1, d2, d3, d4 = st.columns(4)
        d1.metric("Executive rank", _safe_metric_value(row.get("executive_rank_within_month")))
        d2.metric("Priority band", _safe_metric_value(row.get("executive_priority_band")))
        d3.metric("People readiness", _safe_metric_value(row.get("people_readiness_score_0_100")))
        d4.metric("Commercial execution", _safe_metric_value(row.get("commercial_execution_score_0_100")))

        st.markdown("**Management recommendation**")
        st.write(row.get("executive_management_recommendation", "-"))

    if not outlet_summary.empty:
        row = outlet_summary.iloc[0]

        s1, s2, s3, s4 = st.columns(4)
        s1.metric("Revenue EUR", _safe_metric_value(row.get("total_revenue_eur")))
        s2.metric("Utilization %", _safe_metric_value(row.get("avg_treatment_utilization_percent")))
        s3.metric("Yield / sold hour", _safe_metric_value(row.get("avg_treatment_yield_eur_per_sold_hour")))
        s4.metric("RevPATH / available hour", _safe_metric_value(row.get("avg_treatment_revpath_eur_per_available_hour")))

        s5, s6, s7, s8 = st.columns(4)
        s5.metric("Management score", _safe_metric_value(row.get("overall_management_signal_score_0_100")))
        s6.metric("Signal band", _safe_metric_value(row.get("overall_management_signal_band")))
        s7.metric("Retail reward eligible", _safe_metric_value(row.get("retail_reward_eligible_staff_count")))
        s8.metric("Training refresh count", _safe_metric_value(row.get("therapist_refresh_training_required_count")))

        st.markdown("**Recommended manager action**")
        st.write(row.get("recommended_manager_action", "-"))

        st.markdown("**Managerial story**")
        st.write(row.get("managerial_story", "-"))

        st.markdown("**Commercial story**")
        st.write(row.get("commercial_story", "-"))

    st.markdown("### Outlet Management Summary Table")
    summary_cols = [
        "outlet_name",
        "management_signal",
        "overall_management_signal_score_0_100",
        "overall_management_signal_band",
        "total_revenue_eur",
        "avg_treatment_utilization_percent",
        "avg_treatment_yield_eur_per_sold_hour",
        "avg_treatment_revpath_eur_per_available_hour",
        "commercial_reward_attention_flag",
        "refresh_training_attention_flag",
    ]
    st.dataframe(
        summary_view[summary_cols].sort_values(
            by=["overall_management_signal_score_0_100", "total_revenue_eur"],
            ascending=[False, False],
        ),
        width="stretch",
    )
