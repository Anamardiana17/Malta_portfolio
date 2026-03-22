from pathlib import Path
import pandas as pd
import numpy as np

BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")

INPUT_TREATMENT_KPI = BASE / "data_processed/internal_proxy/internal_proxy_treatment_kpi.csv"
INPUT_PRICING_MASTER = BASE / "data_processed/pricing_research/final_treatment_pricing_master.csv"
INPUT_BENCHMARK = BASE / "data_processed/pricing_research/competitor_price_benchmark_summary.csv"
INPUT_EXTERNAL = BASE / "data_processed/internal_proxy/external_demand_proxy_index.csv"
INPUT_BANDS = BASE / "data_processed/reference/score_band_reference.csv"

OUTPUT_FP = BASE / "data_processed/internal_proxy/treatment_health_score.csv"

OUTPUT_COLUMNS = [
    "treatment_health_id","outlet_id","outlet_name","market_context","period_type","period_start","period_end",
    "rolling_window_weeks","treatment_category","treatment_variant","session_duration_min","treatment_key",
    "pricing_position","recommended_sell_price_eur","market_price_median_eur","commercial_market_price_median_eur",
    "benchmark_methodology_status","launch_recommendation_flag","external_demand_proxy_index","external_stress_flag",
    "bookings_count","guest_count","sold_hours","hours_available","utilization_percent","revenue_eur",
    "revpath_eur_per_available_hour","yield_eur_per_sold_hour","avg_ticket_eur","rebooking_rate_percent",
    "addon_attach_rate_percent","retail_attach_rate_percent","cancellation_rate_percent","no_show_rate_percent",
    "complaint_rate_percent","service_recovery_rate_percent","demand_strength_score","yield_quality_score",
    "utilization_contribution_score","retention_quality_score","risk_penalty_score","market_fit_score",
    "treatment_health_score_0_100","treatment_health_band","treatment_health_primary_driver",
    "treatment_health_secondary_driver","treatment_health_risk_flag","treatment_health_action_hint",
    "score_confidence_level","sample_size_observations","minimum_sample_rule_applied","data_coverage_flag",
    "source_dependency_status","qa_status","audit_note","status"
]

def safe_read_csv(fp: Path) -> pd.DataFrame:
    if not fp.exists() or fp.stat().st_size == 0:
        return pd.DataFrame()
    return pd.read_csv(fp)

def normalize_key(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    for c in ["treatment_category", "treatment_variant"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip().str.lower()
    if "session_duration_min" in df.columns:
        df["session_duration_min"] = pd.to_numeric(df["session_duration_min"], errors="coerce")
    return df

def assign_band(score: float, bands: pd.DataFrame, score_type: str) -> str:
    sub = bands[bands["score_type"].eq(score_type)].copy()
    if sub.empty or pd.isna(score):
        return "unclassified"
    for _, r in sub.iterrows():
        if float(r["lower_bound"]) <= score <= float(r["upper_bound"]):
            return str(r["band_label"])
    return "unclassified"

def main():
    treatment = normalize_key(safe_read_csv(INPUT_TREATMENT_KPI))
    pricing = normalize_key(safe_read_csv(INPUT_PRICING_MASTER))
    bench = normalize_key(safe_read_csv(INPUT_BENCHMARK))
    external = safe_read_csv(INPUT_EXTERNAL)
    bands = safe_read_csv(INPUT_BANDS)

    if treatment.empty:
        out = pd.DataFrame(columns=OUTPUT_COLUMNS)
        out.to_csv(OUTPUT_FP, index=False)
        print(f"[OK] saved empty scaffold: {OUTPUT_FP}")
        print("[INFO] internal_proxy_treatment_kpi.csv not ready yet")
        return

    needed = ["outlet_id","period_start","period_end","treatment_category","treatment_variant","session_duration_min"]
    missing = [c for c in needed if c not in treatment.columns]
    if missing:
        raise SystemExit(f"[FAIL] treatment KPI missing required columns: {missing}")

    out = treatment.copy()

    if not pricing.empty:
        pricing_keep = [c for c in [
            "treatment_category","treatment_variant","session_duration_min","pricing_position",
            "recommended_sell_price_eur","market_price_median_eur","commercial_market_price_median_eur",
            "benchmark_methodology_status","launch_recommendation_flag"
        ] if c in pricing.columns]
        out = out.merge(
            pricing[pricing_keep].drop_duplicates(),
            on=[c for c in ["treatment_category","treatment_variant","session_duration_min"] if c in pricing_keep],
            how="left"
        )

    if not bench.empty and "market_price_median_eur" not in out.columns:
        bench_keep = [c for c in [
            "treatment_category","treatment_variant","session_duration_min","market_price_median_eur"
        ] if c in bench.columns]
        if len(bench_keep) >= 4:
            out = out.merge(
                bench[bench_keep].drop_duplicates(),
                on=["treatment_category","treatment_variant","session_duration_min"],
                how="left"
            )

    if not external.empty and {"period_start","period_end"}.issubset(external.columns):
        ext_keep = [c for c in ["period_start","period_end","market_context","external_demand_proxy_index","external_stress_flag"] if c in external.columns]
        out = out.merge(external[ext_keep].drop_duplicates(), on=[c for c in ["period_start","period_end"] if c in ext_keep], how="left")

    numeric_defaults = {
        "bookings_count": 0,
        "guest_count": 0,
        "sold_hours": 0,
        "hours_available": 0,
        "utilization_percent": 0,
        "revenue_eur": 0,
        "revpath_eur_per_available_hour": 0,
        "yield_eur_per_sold_hour": 0,
        "avg_ticket_eur": 0,
        "rebooking_rate_percent": 0,
        "addon_attach_rate_percent": 0,
        "retail_attach_rate_percent": 0,
        "cancellation_rate_percent": 0,
        "no_show_rate_percent": 0,
        "complaint_rate_percent": 0,
        "service_recovery_rate_percent": 0,
        "external_demand_proxy_index": 0,
    }
    for c, default in numeric_defaults.items():
        if c not in out.columns:
            out[c] = default
        out[c] = pd.to_numeric(out[c], errors="coerce").fillna(default)

    out["demand_strength_score"] = np.clip(35 + (out["bookings_count"] * 3.5), 0, 100)
    out["yield_quality_score"] = np.clip(out["yield_eur_per_sold_hour"], 0, 100)
    out["utilization_contribution_score"] = np.clip(out["utilization_percent"], 0, 100)
    out["retention_quality_score"] = np.clip((out["rebooking_rate_percent"] * 0.75) + (out["addon_attach_rate_percent"] * 0.25), 0, 100)
    out["risk_penalty_score"] = np.clip(
        (out["cancellation_rate_percent"] * 0.30)
        + (out["no_show_rate_percent"] * 0.30)
        + (out["complaint_rate_percent"] * 0.20),
        0, 100
    )
    out["market_fit_score"] = np.where(
        out.get("market_price_median_eur", pd.Series(index=out.index, dtype=float)).fillna(0) > 0,
        np.clip(100 - abs(out["recommended_sell_price_eur"].fillna(0) - out["market_price_median_eur"].fillna(0)) * 1.8, 0, 100),
        50
    )

    out["treatment_health_score_0_100"] = np.clip(
        8
        + (out["demand_strength_score"] * 0.22)
        + (out["yield_quality_score"] * 0.24)
        + (out["utilization_contribution_score"] * 0.18)
        + (out["retention_quality_score"] * 0.16)
        + (out["market_fit_score"] * 0.12)
        - (out["risk_penalty_score"] * 0.08),
        0, 100
    ).round(2)

    out["treatment_health_band"] = out["treatment_health_score_0_100"].apply(lambda x: assign_band(x, bands, "treatment_health_score"))
    out["treatment_health_primary_driver"] = np.where(out["risk_penalty_score"] >= 30, "risk_pressure", "commercial_performance")
    out["treatment_health_secondary_driver"] = np.where(out["market_fit_score"] < 50, "market_misalignment", "operating_signal")
    out["treatment_health_risk_flag"] = np.where(out["risk_penalty_score"] >= 25, "risk_elevated", "risk_normal")
    out["treatment_health_action_hint"] = np.where(
        out["treatment_health_score_0_100"] < 50,
        "review pricing demand leakage and treatment mix",
        "maintain and monitor"
    )
    out["score_confidence_level"] = np.where(out["bookings_count"] >= 10, "medium", "low")
    out["sample_size_observations"] = out["bookings_count"].fillna(0).astype(int)
    out["minimum_sample_rule_applied"] = np.where(out["bookings_count"] >= 4, "standard", "low_sample_fallback")
    out["data_coverage_flag"] = "partial"
    out["source_dependency_status"] = "scaffold_with_reference_join"
    out["qa_status"] = "pending_qa"
    out["audit_note"] = "initial scaffold build; formula weights are provisional"
    out["status"] = "scaffold_generated"

    if "outlet_name" not in out.columns:
        out["outlet_name"] = ""
    if "market_context" not in out.columns:
        out["market_context"] = "Malta"
    if "period_type" not in out.columns:
        out["period_type"] = "weekly"
    if "rolling_window_weeks" not in out.columns:
        out["rolling_window_weeks"] = 12
    out["treatment_key"] = (
        out["treatment_category"].astype(str) + "|" +
        out["treatment_variant"].astype(str) + "|" +
        out["session_duration_min"].fillna(0).astype(int).astype(str)
    )
    out["treatment_health_id"] = [
        f"THS_{i+1:05d}" for i in range(len(out))
    ]

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
