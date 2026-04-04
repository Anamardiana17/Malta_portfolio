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


def _render_outlet_health_heatmap(ranking_view: pd.DataFrame) -> None:
    required_cols = [
        "outlet_name",
        "people_readiness_score_0_100",
        "commercial_execution_score_0_100",
        "executive_priority_score_0_100",
    ]
    if not all(col in ranking_view.columns for col in required_cols):
        return

    heatmap_df = ranking_view[required_cols].copy()
    heatmap_df = heatmap_df.rename(
        columns={
            "outlet_name": "Outlet",
            "people_readiness_score_0_100": "People readiness",
            "commercial_execution_score_0_100": "Commercial execution",
            "executive_priority_score_0_100": "Action priority",
        }
    )

    if heatmap_df.empty:
        return

    heatmap_df = heatmap_df.sort_values("Action priority", ascending=False)
    heatmap_df = heatmap_df.set_index("Outlet")

    st.markdown("### Outlet Health Heatmap")
    st.caption(
        "Reviewer-safe scan of outlet condition across core management dimensions. "
        "Internal operational signals remain the primary truth."
    )

    heatmap_display = heatmap_df.round(1)
    st.dataframe(heatmap_display, width="stretch")

    st.caption(
        "How to read: scan across a row to understand one outlet’s condition, and scan down a column to spot repeated pressure patterns. "
        "Stronger shading indicates stronger signal intensity. Read context and integrity notes before acting."
    )


def _render_business_health_trend(summary_df: pd.DataFrame) -> None:
    required_cols = [
        "month_id",
        "overall_management_signal_score_0_100",
        "avg_treatment_utilization_percent",
        "avg_treatment_revpath_eur_per_available_hour",
    ]
    if not all(col in summary_df.columns for col in required_cols):
        return

    trend_df = (
        summary_df.groupby("month_id", as_index=False)[
            [
                "overall_management_signal_score_0_100",
                "avg_treatment_utilization_percent",
                "avg_treatment_revpath_eur_per_available_hour",
            ]
        ]
        .mean()
        .sort_values("month_id")
        .set_index("month_id")
    )

    if trend_df.empty:
        return

    trend_df = trend_df.rename(
        columns={
            "overall_management_signal_score_0_100": "Management score",
            "avg_treatment_utilization_percent": "Utilization %",
            "avg_treatment_revpath_eur_per_available_hour": "RevPATH / available hour",
        }
    )

    st.markdown("### Business-Health Trend")
    st.caption(
        "Portfolio trend view for management condition across analytical months. "
        "Use this to judge whether conditions look improving, worsening, or volatile."
    )

    st.line_chart(trend_df, width="stretch")

    st.caption(
        "How to read: use the direction of change to judge whether portfolio health is stabilizing or becoming more pressured. "
        "Do not treat a single-month move as stand-alone truth without supporting outlet and context review."
    )


def _render_portfolio_composition_chart(ranking_view: pd.DataFrame) -> None:
    required_cols = ["executive_priority_band"]
    if not all(col in ranking_view.columns for col in required_cols):
        return

    comp_df = (
        ranking_view["executive_priority_band"]
        .fillna("Unknown")
        .value_counts()
        .rename_axis("Priority band")
        .reset_index(name="Outlet count")
    )

    if comp_df.empty:
        return

    desired_order = ["High", "Medium", "Low", "Unknown"]
    comp_df["sort_key"] = comp_df["Priority band"].apply(
        lambda x: desired_order.index(x) if x in desired_order else len(desired_order)
    )
    comp_df = comp_df.sort_values(["sort_key", "Outlet count"], ascending=[True, False]).drop(columns=["sort_key"])
    comp_chart = comp_df.set_index("Priority band")[["Outlet count"]]

    st.markdown("### Portfolio Condition Mix")
    st.caption(
        "Portfolio-level composition of current outlet priority condition. "
        "Useful for quick portfolio reading, not outlet-specific action on its own."
    )

    st.bar_chart(comp_chart, width="stretch")

    st.caption(
        "How to read: this shows how many outlets sit in each current priority condition. "
        "Use it for portfolio-level condition reading first, then move to outlet detail and context notes before acting."
    )


def _render_stage2_visuals(ranking_view: pd.DataFrame, summary_df: pd.DataFrame) -> None:
    _render_outlet_health_heatmap(ranking_view)
    _render_business_health_trend(summary_df)
    _render_portfolio_composition_chart(ranking_view)


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

    month_options = sorted(ranking_df["month_id"].dropna().astype(str).unique().tolist())
    selected_month = st.selectbox(
        "Select analytical month_id",
        month_options,
        index=len(month_options) - 1 if month_options else 0,
    )

    ranking_view = ranking_df[ranking_df["month_id"].astype(str) == selected_month].copy()
    summary_view = summary_df[summary_df["month_id"].astype(str) == selected_month].copy()

    month_integrity = build_month_context_integrity_summary(
        selected_month,
        {
            "dashboard_outlet_executive_ranking": ranking_df,
            "outlet_management_summary": summary_df,
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

    st.caption(
        "How to read these cards: use them as a fast business-health read for the active analytical month. "
        "They help identify whether current conditions look healthy, mixed, pressured, or urgent, but should "
        "not be treated as stand-alone truth without reviewing context integrity and outlet detail below."
    )

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

    st.caption(
        "How to read this leaderboard: higher-ranked outlets should be reviewed first. Use the priority band, "
        "management signal, and recommended action together to decide where management attention should go before "
        "pushing commercial uplift."
    )

    _render_stage2_visuals(ranking_view, summary_df)

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

    st.caption(
        "How to read this summary table: use it to confirm whether a strong or weak outlet signal is supported by "
        "commercial performance, utilization, and management flags. Read outlet detail and context notes before "
        "making an aggressive intervention."
    )
