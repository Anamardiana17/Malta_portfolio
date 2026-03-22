from pathlib import Path
import pandas as pd
import numpy as np

BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")

INPUT_TREATMENT = BASE / "data_processed/internal_proxy/treatment_health_score.csv"
INPUT_THERAPIST = BASE / "data_processed/internal_proxy/therapist_consistency_score.csv"
INPUT_PRICING = BASE / "data_processed/pricing_research/final_treatment_pricing_master.csv"

OUTPUT_FP = BASE / "data_processed/internal_proxy/conflict_resolution_layer.csv"

OUTPUT_COLUMNS = [
    "conflict_resolution_id","entity_scope","outlet_id","outlet_name","therapist_id","therapist_name",
    "treatment_category","treatment_variant","session_duration_min","treatment_key","market_context",
    "period_type","period_start","period_end","rolling_window_weeks","pricing_position",
    "recommended_sell_price_eur","market_price_median_eur","commercial_market_price_median_eur",
    "external_demand_proxy_index","external_stress_flag","revenue_eur","utilization_percent",
    "yield_eur_per_sold_hour","revpath_eur_per_available_hour","bookings_count","rebooking_rate_percent",
    "addon_attach_rate_percent","retail_attach_rate_percent","cancellation_rate_percent","no_show_rate_percent",
    "complaint_rate_percent","schedule_adherence_percent","therapist_consistency_score_0_100",
    "treatment_health_score_0_100","conflict_case_flag","conflict_pattern_code","conflict_pattern_group",
    "conflict_priority_level","primary_tradeoff_dimension","secondary_tradeoff_dimension",
    "root_cause_hypothesis","root_cause_confidence_level","recommended_resolution_path","manager_action_route",
    "pricing_action_flag","staffing_action_flag","training_action_flag","retention_action_flag",
    "governance_review_flag","burnout_guardrail_flag","leakage_risk_flag","score_confidence_level",
    "sample_size_observations","dependency_coverage_flag","qa_status","audit_note","status"
]

def safe_read_csv(fp: Path) -> pd.DataFrame:
    if not fp.exists() or fp.stat().st_size == 0:
        return pd.DataFrame()
    return pd.read_csv(fp)

def main():
    treatment = safe_read_csv(INPUT_TREATMENT)
    therapist = safe_read_csv(INPUT_THERAPIST)
    pricing = safe_read_csv(INPUT_PRICING)

    if treatment.empty:
        out = pd.DataFrame(columns=OUTPUT_COLUMNS)
        out.to_csv(OUTPUT_FP, index=False)
        print(f"[OK] saved empty scaffold: {OUTPUT_FP}")
        print("[INFO] treatment_health_score.csv not ready yet")
        return

    out = treatment.copy()
    out["entity_scope"] = "treatment_outlet_period"
    out["therapist_id"] = np.nan
    out["therapist_name"] = np.nan

    if "schedule_adherence_percent" not in out.columns:
        out["schedule_adherence_percent"] = np.nan
    if "therapist_consistency_score_0_100" not in out.columns:
        out["therapist_consistency_score_0_100"] = np.nan

    if not therapist.empty:
        gcols = [c for c in ["outlet_id", "period_start", "period_end"] if c in therapist.columns]
        if len(gcols) == 3:
            th_agg = (
                therapist.groupby(gcols, as_index=False)
                .agg({
                    "therapist_consistency_score_0_100": "mean",
                    "schedule_adherence_percent": "mean"
                })
            )
            th_agg = th_agg.rename(columns={
                "therapist_consistency_score_0_100": "therapist_consistency_score_0_100_agg",
                "schedule_adherence_percent": "schedule_adherence_percent_agg"
            })
            out = out.merge(th_agg, on=gcols, how="left")

            out["therapist_consistency_score_0_100"] = out["therapist_consistency_score_0_100_agg"]
            out["schedule_adherence_percent"] = out["schedule_adherence_percent_agg"]

            out = out.drop(columns=[
                c for c in ["therapist_consistency_score_0_100_agg", "schedule_adherence_percent_agg"]
                if c in out.columns
            ])

    out["conflict_case_flag"] = "no"
    out["conflict_pattern_code"] = "NO_CONFLICT"
    out["conflict_pattern_group"] = "normal"
    out["conflict_priority_level"] = "low"
    out["primary_tradeoff_dimension"] = "none"
    out["secondary_tradeoff_dimension"] = "none"
    out["root_cause_hypothesis"] = "no material conflict detected"
    out["root_cause_confidence_level"] = "low"
    out["recommended_resolution_path"] = "continue monitoring"
    out["manager_action_route"] = "standard_review"
    out["pricing_action_flag"] = "no"
    out["staffing_action_flag"] = "no"
    out["training_action_flag"] = "no"
    out["retention_action_flag"] = "no"
    out["governance_review_flag"] = "no"
    out["burnout_guardrail_flag"] = "no"
    out["leakage_risk_flag"] = "no"

    cond1 = (out["bookings_count"].fillna(0) >= 10) & (out["yield_eur_per_sold_hour"].fillna(0) < 65)
    out.loc[cond1, ["conflict_case_flag","conflict_pattern_code","conflict_pattern_group","conflict_priority_level",
                    "primary_tradeoff_dimension","root_cause_hypothesis","recommended_resolution_path",
                    "manager_action_route","pricing_action_flag"]] = [
        "yes","HIGH_DEMAND_LOW_YIELD","commercial_tradeoff","high",
        "pricing","strong demand but weak monetization","review pricing upsell and mix","pricing_review","yes"
    ]

    cond2 = (out["utilization_percent"].fillna(0) >= 70) & (out["revpath_eur_per_available_hour"].fillna(0) < 45)
    out.loc[cond2, ["conflict_case_flag","conflict_pattern_code","conflict_pattern_group","conflict_priority_level",
                    "primary_tradeoff_dimension","root_cause_hypothesis","recommended_resolution_path",
                    "manager_action_route","pricing_action_flag","staffing_action_flag"]] = [
        "yes","HIGH_UTIL_LOW_REVPATH","capacity_tradeoff","high",
        "capacity_vs_price","capacity busy but value creation weak","review mix pricing and schedule quality","commercial_ops_review","yes","yes"
    ]

    cond3 = (
        (out["revenue_eur"].fillna(0) > 0) &
        (out["schedule_adherence_percent"].fillna(100) < 82) &
        (out["therapist_consistency_score_0_100"].fillna(100) < 72)
    )
    out.loc[cond3, ["conflict_case_flag","conflict_pattern_code","conflict_pattern_group","conflict_priority_level",
                    "primary_tradeoff_dimension","root_cause_hypothesis","recommended_resolution_path",
                    "manager_action_route","training_action_flag","burnout_guardrail_flag"]] = [
        "yes","HIGH_REV_BURNOUT_RISK","sustainability_tradeoff","high",
        "revenue_vs_team","revenue performance may be creating team strain","rebalance roster and coaching support","staffing_and_coaching","yes","yes"
    ]

    cond4 = (out["cancellation_rate_percent"].fillna(0) >= 12) | (out["no_show_rate_percent"].fillna(0) >= 8)
    out.loc[cond4, ["conflict_case_flag","conflict_pattern_code","conflict_pattern_group","conflict_priority_level",
                    "primary_tradeoff_dimension","root_cause_hypothesis","recommended_resolution_path",
                    "manager_action_route","retention_action_flag","leakage_risk_flag"]] = [
        "yes","HIGH_BOOKING_HIGH_CANCELLATION","leakage_tradeoff","medium",
        "leakage","demand exists but leakage is elevated","review confirmation deposits and booking discipline","leakage_control","yes","yes"
    ]

    if "launch_recommendation_flag" in out.columns:
        out.loc[out["launch_recommendation_flag"].astype(str).eq("review_before_launch"), "governance_review_flag"] = "yes"

    out["score_confidence_level"] = "medium"
    out["sample_size_observations"] = out["sample_size_observations"].fillna(out["bookings_count"].fillna(0))
    out["dependency_coverage_flag"] = "partial"
    out["qa_status"] = "pending_qa"
    out["audit_note"] = "conflict engine v1 stabilized with therapist context"
    out["status"] = "scaffold_generated"
    out["conflict_resolution_id"] = [f"CRL_{i+1:05d}" for i in range(len(out))]

    if pricing.empty:
        print("[INFO] pricing master not found or empty; using upstream treatment_health fields only")

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
