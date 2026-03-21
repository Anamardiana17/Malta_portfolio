from pathlib import Path
import pandas as pd
import numpy as np

BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")

INPUT_TREATMENT = BASE / "data_processed/internal_proxy/treatment_health_score.csv"
INPUT_THERAPIST = BASE / "data_processed/internal_proxy/therapist_consistency_score.csv"
INPUT_CONFLICT = BASE / "data_processed/internal_proxy/conflict_resolution_layer.csv"
INPUT_CAPACITY = BASE / "data_processed/internal_proxy/outlet_capacity_proxy.csv"
INPUT_BANDS = BASE / "data_processed/reference/score_band_reference.csv"

OUTPUT_FP = BASE / "data_processed/internal_proxy/management_kpi_signal_layer.csv"

OUTPUT_COLUMNS = [
    "management_signal_id","outlet_id","outlet_name","market_context","period_type","period_start","period_end",
    "rolling_window_weeks","total_revenue_eur","total_sold_hours","total_hours_available","utilization_percent",
    "yield_eur_per_sold_hour","revpath_eur_per_available_hour","rebooking_rate_percent","addon_attach_rate_percent",
    "retail_attach_rate_percent","cancellation_rate_percent","no_show_rate_percent","complaint_rate_percent",
    "payroll_efficiency_percent","capacity_pressure_percent","external_demand_proxy_index","external_stress_flag",
    "avg_treatment_health_score","avg_therapist_consistency_score","critical_conflict_count",
    "high_priority_conflict_count","burnout_risk_case_count","leakage_risk_case_count","revenue_growth_signal_score",
    "utilization_signal_score","yield_signal_score","revpath_signal_score","team_sustainability_signal_score",
    "leakage_control_signal_score","guest_quality_signal_score","overall_management_signal_score_0_100",
    "overall_management_signal_band","primary_management_priority","secondary_management_priority","manager_action_1",
    "manager_action_2","manager_action_3","review_escalation_flag","executive_watchlist_flag",
    "score_confidence_level","coverage_ratio_percent","dependency_coverage_flag","qa_status","audit_note","status"
]

def safe_read_csv(fp: Path) -> pd.DataFrame:
    if not fp.exists() or fp.stat().st_size == 0:
        return pd.DataFrame()
    return pd.read_csv(fp)

def assign_band(score: float, bands: pd.DataFrame, score_type: str) -> str:
    sub = bands[bands["score_type"].eq(score_type)].copy()
    if sub.empty or pd.isna(score):
        return "unclassified"
    for _, r in sub.iterrows():
        if float(r["lower_bound"]) <= score <= float(r["upper_bound"]):
            return str(r["band_label"])
    return "unclassified"

def main():
    treatment = safe_read_csv(INPUT_TREATMENT)
    therapist = safe_read_csv(INPUT_THERAPIST)
    conflict = safe_read_csv(INPUT_CONFLICT)
    capacity = safe_read_csv(INPUT_CAPACITY)
    bands = safe_read_csv(INPUT_BANDS)

    if treatment.empty:
        out = pd.DataFrame(columns=OUTPUT_COLUMNS)
        out.to_csv(OUTPUT_FP, index=False)
        print(f"[OK] saved empty scaffold: {OUTPUT_FP}")
        print("[INFO] treatment_health_score.csv not ready yet")
        return

    group_cols = [c for c in ["outlet_id","outlet_name","market_context","period_type","period_start","period_end","rolling_window_weeks"] if c in treatment.columns]
    agg_map = {
        "revenue_eur": "sum",
        "sold_hours": "sum",
        "hours_available": "sum",
        "utilization_percent": "mean",
        "yield_eur_per_sold_hour": "mean",
        "revpath_eur_per_available_hour": "mean",
        "rebooking_rate_percent": "mean",
        "addon_attach_rate_percent": "mean",
        "retail_attach_rate_percent": "mean",
        "cancellation_rate_percent": "mean",
        "no_show_rate_percent": "mean",
        "complaint_rate_percent": "mean",
        "external_demand_proxy_index": "mean",
        "treatment_health_score_0_100": "mean",
    }
    use_agg = {k: v for k, v in agg_map.items() if k in treatment.columns}
    out = treatment.groupby(group_cols, dropna=False, as_index=False).agg(use_agg)

    rename_map = {
        "revenue_eur": "total_revenue_eur",
        "sold_hours": "total_sold_hours",
        "hours_available": "total_hours_available",
        "treatment_health_score_0_100": "avg_treatment_health_score",
    }
    out = out.rename(columns=rename_map)

    if not therapist.empty:
        g2 = [c for c in ["outlet_id","period_start","period_end"] if c in therapist.columns]
        if len(g2) == 3:
            th = therapist.groupby(g2, as_index=False).agg({
                "therapist_consistency_score_0_100": "mean"
            })
            th = th.rename(columns={"therapist_consistency_score_0_100": "avg_therapist_consistency_score"})
            out = out.merge(th, on=g2, how="left")
        else:
            out["avg_therapist_consistency_score"] = np.nan
    else:
        out["avg_therapist_consistency_score"] = np.nan

    if not conflict.empty:
        g3 = [c for c in ["outlet_id","period_start","period_end"] if c in conflict.columns]
        if len(g3) == 3:
            tmp = conflict.copy()
            tmp["critical_conflict_count"] = (tmp["conflict_priority_level"].astype(str) == "critical").astype(int)
            tmp["high_priority_conflict_count"] = (tmp["conflict_priority_level"].astype(str) == "high").astype(int)
            tmp["burnout_risk_case_count"] = (tmp["burnout_guardrail_flag"].astype(str) == "yes").astype(int)
            tmp["leakage_risk_case_count"] = (tmp["leakage_risk_flag"].astype(str) == "yes").astype(int)
            cf = tmp.groupby(g3, as_index=False)[["critical_conflict_count","high_priority_conflict_count","burnout_risk_case_count","leakage_risk_case_count"]].sum()
            out = out.merge(cf, on=g3, how="left")
    for c in ["critical_conflict_count","high_priority_conflict_count","burnout_risk_case_count","leakage_risk_case_count"]:
        if c not in out.columns:
            out[c] = 0
        out[c] = out[c].fillna(0)

    if not capacity.empty and {"outlet_id","period_start","period_end"}.issubset(capacity.columns):
        cap_keep = [c for c in ["outlet_id","period_start","period_end","payroll_efficiency_percent","capacity_pressure_percent","external_stress_flag"] if c in capacity.columns]
        out = out.merge(capacity[cap_keep].drop_duplicates(), on=[c for c in ["outlet_id","period_start","period_end"] if c in cap_keep], how="left")

    for c in ["payroll_efficiency_percent","capacity_pressure_percent","external_stress_flag"]:
        if c not in out.columns:
            out[c] = np.nan

    out["revenue_growth_signal_score"] = np.clip(out["total_revenue_eur"].fillna(0) / 120, 0, 100)
    out["utilization_signal_score"] = np.clip(out["utilization_percent"].fillna(0), 0, 100)
    out["yield_signal_score"] = np.clip(out["yield_eur_per_sold_hour"].fillna(0), 0, 100)
    out["revpath_signal_score"] = np.clip(out["revpath_eur_per_available_hour"].fillna(0), 0, 100)
    out["team_sustainability_signal_score"] = np.clip(100 - (out["burnout_risk_case_count"] * 15) - (out["capacity_pressure_percent"].fillna(0) * 0.6), 0, 100)
    out["leakage_control_signal_score"] = np.clip(100 - (out["leakage_risk_case_count"] * 12) - (out["cancellation_rate_percent"].fillna(0) * 0.8) - (out["no_show_rate_percent"].fillna(0) * 0.8), 0, 100)
    out["guest_quality_signal_score"] = np.clip(100 - (out["complaint_rate_percent"].fillna(0) * 2), 0, 100)

    out["overall_management_signal_score_0_100"] = np.clip(
        5
        + (out["revenue_growth_signal_score"] * 0.14)
        + (out["utilization_signal_score"] * 0.14)
        + (out["yield_signal_score"] * 0.15)
        + (out["revpath_signal_score"] * 0.15)
        + (out["team_sustainability_signal_score"] * 0.14)
        + (out["leakage_control_signal_score"] * 0.14)
        + (out["guest_quality_signal_score"] * 0.09)
        + (out["avg_treatment_health_score"].fillna(0) * 0.05),
        0, 100
    ).round(2)

    out["overall_management_signal_band"] = out["overall_management_signal_score_0_100"].apply(lambda x: assign_band(x, bands, "overall_management_signal_score"))
    out["primary_management_priority"] = np.where(out["team_sustainability_signal_score"] < 50, "team_sustainability", "commercial_growth")
    out["secondary_management_priority"] = np.where(out["leakage_control_signal_score"] < 60, "leakage_control", "yield_optimization")
    out["manager_action_1"] = np.where(out["primary_management_priority"].eq("team_sustainability"), "rebalance roster and protect therapist recovery", "push pricing discipline and upsell")
    out["manager_action_2"] = np.where(out["secondary_management_priority"].eq("leakage_control"), "tighten booking confirmation and cancellation control", "optimize treatment mix and conversion")
    out["manager_action_3"] = "review outlet scorecard and conflict patterns weekly"
    out["review_escalation_flag"] = np.where(out["high_priority_conflict_count"] >= 2, "yes", "no")
    out["executive_watchlist_flag"] = np.where(out["overall_management_signal_score_0_100"] < 50, "yes", "no")
    out["score_confidence_level"] = "low"
    out["coverage_ratio_percent"] = 75
    out["dependency_coverage_flag"] = "partial"
    out["qa_status"] = "pending_qa"
    out["audit_note"] = "initial scaffold management signal rollup; weights provisional"
    out["status"] = "scaffold_generated"
    out["management_signal_id"] = [f"MSL_{i+1:05d}" for i in range(len(out))]

    for c in OUTPUT_COLUMNS:
        if c not in out.columns:
            out[c] = np.nan

    out = out[OUTPUT_COLUMNS].copy()
    out.to_csv(OUTPUT_FP, index=False)

    print(f"[OK] saved: {OUTPUT_FP}")
    print(f"[OK] rows: {len(out)}")
    print("\nPreview:")
    print(out.head(10).to_string(index=False))

if __name__ == "__main__":
    main()
