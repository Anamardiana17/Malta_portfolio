from pathlib import Path
import pandas as pd
import numpy as np

BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")

INPUT_THERAPIST_KPI = BASE / "data_processed/internal_proxy/internal_proxy_therapist_kpi.csv"
INPUT_EXTERNAL = BASE / "data_processed/internal_proxy/external_demand_proxy_index.csv"
INPUT_POLICY = BASE / "data_processed/pricing_research/utilization_assumption_policy.csv"
INPUT_BANDS = BASE / "data_processed/reference/score_band_reference.csv"

OUTPUT_FP = BASE / "data_processed/internal_proxy/therapist_consistency_score.csv"

OUTPUT_COLUMNS = [
    "therapist_consistency_id","therapist_id","therapist_name","outlet_id","outlet_name","market_context",
    "period_type","period_start","period_end","rolling_window_weeks","therapist_role","employment_type",
    "active_flag","hours_available","hours_sold","utilization_percent","revenue_eur","yield_eur_per_sold_hour",
    "revpath_proxy_eur_per_available_hour","avg_ticket_eur","rebooking_rate_percent","addon_attach_rate_percent",
    "retail_attach_rate_percent","cancellation_impact_rate_percent","complaint_rate_percent",
    "service_recovery_rate_percent","schedule_adherence_percent","attendance_reliability_percent",
    "performance_level_score","utilization_stability_score","yield_stability_score","guest_quality_stability_score",
    "commercial_consistency_score","attendance_consistency_score","variance_penalty_score",
    "market_fairness_modifier_score","therapist_consistency_score_0_100","therapist_consistency_band",
    "consistency_primary_gap","consistency_secondary_gap","consistency_variance_flag","coach_priority_flag",
    "burnout_risk_flag","score_confidence_level","observed_weeks_count","observed_months_count",
    "minimum_sample_rule_applied","data_coverage_flag","source_dependency_status","qa_status","audit_note","status"
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
    therapist = safe_read_csv(INPUT_THERAPIST_KPI)
    external = safe_read_csv(INPUT_EXTERNAL)
    policy = safe_read_csv(INPUT_POLICY)
    bands = safe_read_csv(INPUT_BANDS)

    if therapist.empty:
        out = pd.DataFrame(columns=OUTPUT_COLUMNS)
        out.to_csv(OUTPUT_FP, index=False)
        print(f"[OK] saved empty scaffold: {OUTPUT_FP}")
        print("[INFO] internal_proxy_therapist_kpi.csv not ready yet")
        return

    needed = ["therapist_id","outlet_id","period_start","period_end"]
    missing = [c for c in needed if c not in therapist.columns]
    if missing:
        raise SystemExit(f"[FAIL] therapist KPI missing required columns: {missing}")

    out = therapist.copy()

    if not external.empty and {"period_start","period_end"}.issubset(external.columns):
        ext_keep = [c for c in ["period_start","period_end","market_context","external_demand_proxy_index"] if c in external.columns]
        out = out.merge(external[ext_keep].drop_duplicates(), on=[c for c in ["period_start","period_end"] if c in ext_keep], how="left")

    defaults = {
        "hours_available": 0,
        "hours_sold": 0,
        "utilization_percent": 0,
        "revenue_eur": 0,
        "yield_eur_per_sold_hour": 0,
        "revpath_proxy_eur_per_available_hour": 0,
        "avg_ticket_eur": 0,
        "rebooking_rate_percent": 0,
        "addon_attach_rate_percent": 0,
        "retail_attach_rate_percent": 0,
        "cancellation_impact_rate_percent": 0,
        "complaint_rate_percent": 0,
        "service_recovery_rate_percent": 0,
        "schedule_adherence_percent": 0,
        "attendance_reliability_percent": 0,
    }
    for c, default in defaults.items():
        if c not in out.columns:
            out[c] = default
        out[c] = pd.to_numeric(out[c], errors="coerce").fillna(default)

    out["performance_level_score"] = np.clip(
        (out["yield_eur_per_sold_hour"] * 0.35)
        + (out["utilization_percent"] * 0.35)
        + (out["rebooking_rate_percent"] * 0.15)
        + (out["addon_attach_rate_percent"] * 0.15),
        0, 100
    )

    out["utilization_stability_score"] = np.clip(100 - abs(out["utilization_percent"] - out["utilization_percent"].median()), 0, 100)
    out["yield_stability_score"] = np.clip(100 - abs(out["yield_eur_per_sold_hour"] - out["yield_eur_per_sold_hour"].median()), 0, 100)
    out["guest_quality_stability_score"] = np.clip(100 - (out["complaint_rate_percent"] * 2), 0, 100)
    out["commercial_consistency_score"] = np.clip(
        (out["rebooking_rate_percent"] * 0.5) + (out["addon_attach_rate_percent"] * 0.5),
        0, 100
    )
    out["attendance_consistency_score"] = np.clip(
        (out["schedule_adherence_percent"] * 0.5) + (out["attendance_reliability_percent"] * 0.5),
        0, 100
    )
    out["variance_penalty_score"] = np.clip(
        (100 - out["utilization_stability_score"]) * 0.4
        + (100 - out["yield_stability_score"]) * 0.4
        + (out["complaint_rate_percent"] * 0.2),
        0, 100
    )
    out["market_fairness_modifier_score"] = np.where(
        out.get("external_demand_proxy_index", pd.Series(index=out.index, dtype=float)).fillna(0) < 0,
        5,
        0
    )

    out["therapist_consistency_score_0_100"] = np.clip(
        (out["performance_level_score"] * 0.35)
        + (out["utilization_stability_score"] * 0.15)
        + (out["yield_stability_score"] * 0.15)
        + (out["guest_quality_stability_score"] * 0.10)
        + (out["commercial_consistency_score"] * 0.10)
        + (out["attendance_consistency_score"] * 0.15)
        - (out["variance_penalty_score"] * 0.20)
        + out["market_fairness_modifier_score"],
        0, 100
    ).round(2)

    out["therapist_consistency_band"] = out["therapist_consistency_score_0_100"].apply(lambda x: assign_band(x, bands, "therapist_consistency_score"))
    out["consistency_primary_gap"] = np.where(out["attendance_consistency_score"] < 60, "attendance_discipline", "performance_stability")
    out["consistency_secondary_gap"] = np.where(out["commercial_consistency_score"] < 50, "commercial_repeatability", "guest_quality")
    out["consistency_variance_flag"] = np.where(out["variance_penalty_score"] >= 25, "variance_elevated", "variance_normal")
    out["coach_priority_flag"] = np.where(out["therapist_consistency_score_0_100"] < 60, "coach_priority", "standard_monitoring")
    out["burnout_risk_flag"] = np.where((out["hours_sold"] > out["hours_available"] * 0.85) & (out["schedule_adherence_percent"] < 70), "burnout_watch", "burnout_normal")
    out["score_confidence_level"] = "low"
    out["observed_weeks_count"] = np.where("week_id" in out.columns, 1, 0)
    out["observed_months_count"] = np.where("month_id" in out.columns, 1, 0)
    out["minimum_sample_rule_applied"] = "scaffold"
    out["data_coverage_flag"] = "partial"
    out["source_dependency_status"] = "scaffold_with_policy_reference"
    out["qa_status"] = "pending_qa"
    out["audit_note"] = "initial scaffold build; consistency logic provisional"

    if "therapist_name" not in out.columns:
        out["therapist_name"] = ""
    if "outlet_name" not in out.columns:
        out["outlet_name"] = ""
    if "market_context" not in out.columns:
        out["market_context"] = "Malta"
    if "period_type" not in out.columns:
        out["period_type"] = "weekly"
    if "rolling_window_weeks" not in out.columns:
        out["rolling_window_weeks"] = 12
    if "therapist_role" not in out.columns:
        out["therapist_role"] = "therapist"
    if "employment_type" not in out.columns:
        out["employment_type"] = "unknown"
    if "active_flag" not in out.columns:
        out["active_flag"] = "yes"

    out["status"] = "scaffold_generated"
    out["therapist_consistency_id"] = [f"TCS_{i+1:05d}" for i in range(len(out))]

    if not policy.empty:
        print(f"[INFO] utilization policy rows found: {len(policy)}")

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
