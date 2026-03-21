from pathlib import Path
import pandas as pd
import numpy as np

BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")

INPUT_TREATMENT = BASE / "data_processed/internal_proxy/treatment_health_score.csv"
INPUT_THERAPIST = BASE / "data_processed/internal_proxy/therapist_consistency_score.csv"
INPUT_CONFLICT = BASE / "data_processed/internal_proxy/conflict_resolution_layer.csv"
INPUT_MGMT = BASE / "data_processed/internal_proxy/management_kpi_signal_layer.csv"
INPUT_EXTERNAL = BASE / "data_processed/internal_proxy/external_demand_proxy_index.csv"

OUTLET_SUMMARY_FP = BASE / "data_processed/insight_mart/outlet_management_summary.csv"
TREATMENT_OPP_FP = BASE / "data_processed/insight_mart/treatment_opportunity_summary.csv"
THERAPIST_COACH_FP = BASE / "data_processed/insight_mart/therapist_coaching_summary.csv"
ACTION_QUEUE_FP = BASE / "data_processed/insight_mart/manager_action_queue.csv"

def safe_read(fp: Path) -> pd.DataFrame:
    if not fp.exists() or fp.stat().st_size == 0:
        return pd.DataFrame()
    return pd.read_csv(fp)

def main():
    ths = safe_read(INPUT_TREATMENT)
    tcs = safe_read(INPUT_THERAPIST)
    crl = safe_read(INPUT_CONFLICT)
    msl = safe_read(INPUT_MGMT)
    ext = safe_read(INPUT_EXTERNAL)

    ext_keep = [c for c in [
        "period_start","period_end","month_id","year","month",
        "market_regime","regime_label","event_flag","market_note",
        "period_regime_impact_note","managerial_interpretation",
        "profitability_pressure_flag","external_demand_proxy_index","external_stress_flag"
    ] if c in ext.columns]

    # ======================================================
    # 1) OUTLET MANAGEMENT SUMMARY
    # ======================================================
    if not msl.empty:
        outlet = msl.copy()

        if not ext.empty:
            outlet = outlet.merge(
                ext[ext_keep].drop_duplicates(),
                on=[c for c in ["period_start","period_end"] if c in ext_keep],
                how="left"
            )

        outlet["outlet_rank_within_period"] = (
            outlet.groupby("period_start")["overall_management_signal_score_0_100"]
            .rank(method="dense", ascending=False)
        )

        outlet["management_status_note"] = np.select(
            [
                outlet["overall_management_signal_band"].astype(str).eq("stable"),
                outlet["overall_management_signal_band"].astype(str).eq("watchlist"),
                outlet["overall_management_signal_band"].astype(str).eq("critical"),
            ],
            [
                "Outlet operating in relatively stable condition; optimize selectively.",
                "Outlet needs focused intervention on priority KPI signals.",
                "Outlet requires urgent escalation and corrective action.",
            ],
            default="Outlet status not classified."
        )

        outlet["managerial_focus_area"] = np.select(
            [
                outlet["primary_management_priority"].astype(str).eq("commercial_growth"),
                outlet["primary_management_priority"].astype(str).eq("team_sustainability"),
            ],
            [
                "Revenue growth and pricing/yield opportunity",
                "Team sustainability and staffing control",
            ],
            default="Balanced monitoring"
        )

        outlet["storyline_note"] = np.where(
            outlet["profitability_pressure_flag"].astype(str).eq("yes"),
            "Demand recovery may not fully translate into profit recovery due to cost pressure, value sensitivity, or team strain.",
            "Market backdrop is more supportive; focus on disciplined growth and consistency."
        )

        outlet_cols = [
            "management_signal_id","outlet_id","outlet_name","market_context","period_type",
            "period_start","period_end","year","month","month_id",
            "market_regime","regime_label","event_flag","market_note","period_regime_impact_note",
            "managerial_interpretation","profitability_pressure_flag","external_demand_proxy_index",
            "external_stress_flag","overall_management_signal_score_0_100",
            "overall_management_signal_band","primary_management_priority",
            "secondary_management_priority","total_revenue_eur","utilization_percent",
            "yield_eur_per_sold_hour","revpath_eur_per_available_hour",
            "avg_treatment_health_score","avg_therapist_consistency_score",
            "high_priority_conflict_count","burnout_risk_case_count","leakage_risk_case_count",
            "manager_action_1","manager_action_2","manager_action_3",
            "outlet_rank_within_period","managerial_focus_area","management_status_note",
            "storyline_note","review_escalation_flag","executive_watchlist_flag",
            "score_confidence_level","status"
        ]
        outlet = outlet[[c for c in outlet_cols if c in outlet.columns]].copy()
    else:
        outlet = pd.DataFrame()

    outlet.to_csv(OUTLET_SUMMARY_FP, index=False)

    # ======================================================
    # 2) TREATMENT OPPORTUNITY SUMMARY
    # ======================================================
    if not ths.empty:
        treat = ths.copy()

        if not ext.empty:
            treat = treat.merge(
                ext[ext_keep].drop_duplicates(),
                on=[c for c in ["period_start","period_end"] if c in ext_keep],
                how="left"
            )

        conflict_note = pd.DataFrame()
        if not crl.empty:
            conflict_note = crl[[
                c for c in [
                    "outlet_id","period_start","period_end","treatment_category",
                    "treatment_variant","session_duration_min",
                    "conflict_pattern_code","recommended_resolution_path"
                ] if c in crl.columns
            ]].drop_duplicates()

            treat = treat.merge(
                conflict_note,
                on=[c for c in [
                    "outlet_id","period_start","period_end","treatment_category",
                    "treatment_variant","session_duration_min"
                ] if c in conflict_note.columns],
                how="left"
            )

        treat["opportunity_type"] = np.select(
            [
                (treat["treatment_health_band"].astype(str).eq("healthy")) & (treat["yield_eur_per_sold_hour"] < treat["recommended_sell_price_eur"] * 0.95),
                treat["treatment_health_band"].astype(str).eq("watchlist"),
                treat["treatment_health_band"].astype(str).eq("critical"),
            ],
            [
                "Yield uplift opportunity",
                "Monitor and optimize",
                "Immediate review required",
            ],
            default="Maintain and scale"
        )

        treat["opportunity_priority"] = np.select(
            [
                treat["treatment_health_band"].astype(str).eq("critical"),
                treat["treatment_health_band"].astype(str).eq("watchlist"),
                treat["treatment_health_band"].astype(str).eq("healthy"),
            ],
            ["high","medium","medium"],
            default="low"
        )

        treat["treatment_rank_within_outlet_period"] = (
            treat.groupby(["outlet_id","period_start"])["treatment_health_score_0_100"]
            .rank(method="dense", ascending=False)
        )

        treat["commercial_opportunity_note"] = np.select(
            [
                treat["conflict_pattern_code"].astype(str).eq("HIGH_DEMAND_LOW_YIELD"),
                treat["treatment_health_band"].astype(str).eq("critical"),
                treat["profitability_pressure_flag"].astype(str).eq("yes"),
            ],
            [
                "Demand exists but monetization is weak; pricing, upsell, and mix should be reviewed.",
                "Treatment requires immediate managerial review due to weak health score.",
                "Demand may recover faster than profitability; protect margin discipline and value architecture.",
            ],
            default="Treatment performance is manageable under current operating conditions."
        )

        treat_cols = [
            "treatment_health_id","outlet_id","outlet_name","period_start","period_end","year","month","month_id",
            "market_regime","regime_label","event_flag","market_note","period_regime_impact_note",
            "managerial_interpretation","profitability_pressure_flag",
            "treatment_category","treatment_variant","session_duration_min","treatment_key",
            "recommended_sell_price_eur","market_price_median_eur",
            "commercial_market_price_median_eur","bookings_count","sold_hours","revenue_eur",
            "utilization_percent","yield_eur_per_sold_hour","rebooking_rate_percent",
            "addon_attach_rate_percent","cancellation_rate_percent","no_show_rate_percent",
            "complaint_rate_percent","treatment_health_score_0_100","treatment_health_band",
            "treatment_health_primary_driver","treatment_health_risk_flag",
            "treatment_health_action_hint","conflict_pattern_code","recommended_resolution_path",
            "opportunity_type","opportunity_priority",
            "treatment_rank_within_outlet_period","commercial_opportunity_note","status"
        ]
        treat = treat[[c for c in treat_cols if c in treat.columns]].copy()
    else:
        treat = pd.DataFrame()

    treat.to_csv(TREATMENT_OPP_FP, index=False)

    # ======================================================
    # 3) THERAPIST COACHING SUMMARY
    # ======================================================
    if not tcs.empty:
        coach = tcs.copy()

        if not ext.empty:
            coach = coach.merge(
                ext[ext_keep].drop_duplicates(),
                on=[c for c in ["period_start","period_end"] if c in ext_keep],
                how="left"
            )

        coach["coaching_priority_level"] = np.select(
            [
                coach["coach_priority_flag"].astype(str).eq("coach_priority"),
                coach["burnout_risk_flag"].astype(str).eq("burnout_watch"),
                coach["therapist_consistency_band"].astype(str).eq("stable"),
            ],
            ["high","high","medium"],
            default="low"
        )

        coach["coaching_theme"] = np.select(
            [
                coach["consistency_primary_gap"].astype(str).eq("attendance_discipline"),
                coach["consistency_secondary_gap"].astype(str).eq("commercial_repeatability"),
                coach["burnout_risk_flag"].astype(str).eq("burnout_watch"),
            ],
            [
                "Attendance and schedule discipline",
                "Upsell/rebooking/commercial repeatability",
                "Workload sustainability and recovery balance",
            ],
            default="Maintain performance discipline"
        )

        coach["coaching_context_note"] = np.where(
            coach["market_regime"].astype(str).isin(["covid_lockdown","covid_partial_reopen","recovery_constrained"]),
            "Assess therapist performance with market context in mind; weak demand periods should not be over-penalized.",
            "Use stable-market periods to reinforce consistency, upsell quality, and guest retention."
        )

        coach["therapist_rank_within_outlet_period"] = (
            coach.groupby(["outlet_id","period_start"])["therapist_consistency_score_0_100"]
            .rank(method="dense", ascending=False)
        )

        coach_cols = [
            "therapist_consistency_id","therapist_id","therapist_name","outlet_id","outlet_name",
            "period_start","period_end","year","month","month_id",
            "market_regime","regime_label","event_flag","market_note","period_regime_impact_note",
            "profitability_pressure_flag","hours_available","hours_sold","utilization_percent",
            "revenue_eur","yield_eur_per_sold_hour","rebooking_rate_percent",
            "addon_attach_rate_percent","retail_attach_rate_percent",
            "schedule_adherence_percent","attendance_reliability_percent",
            "therapist_consistency_score_0_100","therapist_consistency_band",
            "consistency_primary_gap","consistency_secondary_gap","consistency_variance_flag",
            "coach_priority_flag","burnout_risk_flag","coaching_priority_level",
            "coaching_theme","coaching_context_note","therapist_rank_within_outlet_period","status"
        ]
        coach = coach[[c for c in coach_cols if c in coach.columns]].copy()
    else:
        coach = pd.DataFrame()

    coach.to_csv(THERAPIST_COACH_FP, index=False)

    # ======================================================
    # 4) MANAGER ACTION QUEUE
    # ======================================================
    queue_parts = []

    if not msl.empty:
        q1 = msl.copy()

        if not ext.empty:
            q1 = q1.merge(
                ext[ext_keep].drop_duplicates(),
                on=[c for c in ["period_start","period_end"] if c in ext_keep],
                how="left"
            )

        q1["action_scope"] = "outlet_period"
        q1["action_priority"] = np.select(
            [
                q1["executive_watchlist_flag"].astype(str).eq("yes"),
                q1["review_escalation_flag"].astype(str).eq("yes"),
                q1["overall_management_signal_band"].astype(str).eq("watchlist"),
            ],
            ["critical","high","medium"],
            default="normal"
        )
        q1["action_title"] = q1["primary_management_priority"].astype(str).str.replace("_", " ", regex=False).str.title()
        q1["action_note"] = (
            q1["manager_action_1"].fillna("Review outlet scorecard")
            + " | "
            + q1["period_regime_impact_note"].fillna("Review current market context")
        )
        q1["owner"] = "spa_manager"
        q1["queue_source"] = "management_kpi_signal_layer"
        q1 = q1.rename(columns={"management_signal_id": "source_record_id"})
        q1["outlet_or_entity_name"] = q1["outlet_name"]
        q1["period_label"] = q1["period_start"].astype(str) + " to " + q1["period_end"].astype(str)
        queue_parts.append(q1[[
            "source_record_id","queue_source","action_scope","action_priority","action_title",
            "action_note","owner","outlet_id","outlet_or_entity_name","period_label"
        ]])

    if not crl.empty:
        q2 = crl[crl["conflict_case_flag"].astype(str).eq("yes")].copy()
        if not q2.empty:
            if not ext.empty:
                q2 = q2.merge(
                    ext[ext_keep].drop_duplicates(),
                    on=[c for c in ["period_start","period_end"] if c in ext_keep],
                    how="left"
                )
            q2["action_scope"] = "treatment_conflict"
            q2["action_priority"] = q2["conflict_priority_level"].fillna("medium")
            q2["action_title"] = q2["conflict_pattern_code"].astype(str).str.replace("_", " ", regex=False).str.title()
            q2["action_note"] = (
                q2["recommended_resolution_path"].fillna("Review conflict pattern")
                + " | "
                + q2["period_regime_impact_note"].fillna("Context note unavailable")
            )
            q2["owner"] = "spa_manager"
            q2["queue_source"] = "conflict_resolution_layer"
            q2 = q2.rename(columns={"conflict_resolution_id": "source_record_id"})
            q2["outlet_or_entity_name"] = q2["outlet_name"].fillna("") + " | " + q2["treatment_category"].fillna("").astype(str)
            q2["period_label"] = q2["period_start"].astype(str) + " to " + q2["period_end"].astype(str)
            queue_parts.append(q2[[
                "source_record_id","queue_source","action_scope","action_priority","action_title",
                "action_note","owner","outlet_id","outlet_or_entity_name","period_label"
            ]])

    if not tcs.empty:
        q3 = tcs[
            (tcs["coach_priority_flag"].astype(str).eq("coach_priority")) |
            (tcs["burnout_risk_flag"].astype(str).eq("burnout_watch"))
        ].copy()
        if not q3.empty:
            if not ext.empty:
                q3 = q3.merge(
                    ext[ext_keep].drop_duplicates(),
                    on=[c for c in ["period_start","period_end"] if c in ext_keep],
                    how="left"
                )
            q3["action_scope"] = "therapist_coaching"
            q3["action_priority"] = np.where(
                q3["burnout_risk_flag"].astype(str).eq("burnout_watch"),
                "high",
                "medium"
            )
            q3["action_title"] = "Therapist Coaching Review"
            q3["action_note"] = (
                q3["consistency_primary_gap"].fillna("Review therapist consistency").astype(str)
                + " | "
                + q3["period_regime_impact_note"].fillna("Context note unavailable")
            )
            q3["owner"] = "spa_manager"
            q3["queue_source"] = "therapist_consistency_score"
            q3 = q3.rename(columns={"therapist_consistency_id": "source_record_id"})
            q3["outlet_or_entity_name"] = q3["outlet_name"].fillna("") + " | " + q3["therapist_name"].fillna("")
            q3["period_label"] = q3["period_start"].astype(str) + " to " + q3["period_end"].astype(str)
            queue_parts.append(q3[[
                "source_record_id","queue_source","action_scope","action_priority","action_title",
                "action_note","owner","outlet_id","outlet_or_entity_name","period_label"
            ]])

    if queue_parts:
        queue = pd.concat(queue_parts, ignore_index=True)
        priority_order = {"critical": 1, "high": 2, "medium": 3, "normal": 4}
        queue["priority_sort"] = queue["action_priority"].map(priority_order).fillna(99)
        queue = queue.sort_values(["priority_sort","outlet_id","period_label","action_scope"]).drop(columns=["priority_sort"])
        queue.insert(0, "manager_action_queue_id", [f"MAQ_{i+1:05d}" for i in range(len(queue))])
    else:
        queue = pd.DataFrame(columns=[
            "manager_action_queue_id","source_record_id","queue_source","action_scope","action_priority",
            "action_title","action_note","owner","outlet_id","outlet_or_entity_name","period_label"
        ])

    queue.to_csv(ACTION_QUEUE_FP, index=False)

    print(f"[OK] saved: {OUTLET_SUMMARY_FP} | rows={len(outlet)}")
    print(f"[OK] saved: {TREATMENT_OPP_FP} | rows={len(treat)}")
    print(f"[OK] saved: {THERAPIST_COACH_FP} | rows={len(coach)}")
    print(f"[OK] saved: {ACTION_QUEUE_FP} | rows={len(queue)}")

if __name__ == "__main__":
    main()
