from pathlib import Path
import math
import pandas as pd

BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")
OUT_DIR = BASE / "data_processed" / "operating_model"
OUT_DIR.mkdir(parents=True, exist_ok=True)

INTERPRET_FP = BASE / "data_processed" / "scenario" / "management_interpretation_layer.csv"

rows = [
    {
        "outlet_name": "Central Malta Spa",
        "outlet_role": "anchor_urban_commercial",
        "location_cluster": "central_urban",
        "seasonality_class": "moderate",
        "business_model": "balanced_commercial_stabilizer",
        "service_style": "mixed_short_medium_long",
        "pricing_posture": "upper_mid_premium",
        "treatment_room_count": 7,
        "operating_hours_per_day": 11,
        "open_days_per_week": 7,
        "paid_shift_hours": 8.5,
        "non_sellable_hours": 2.0,
        "minimum_coverage_ratio": 0.45,
        "safe_productive_ratio": 0.67,
        "peak_productive_ratio": 0.83,
        "minimum_opening_team": 3,
        "seasonal_staffing_logic": "stable urban base with moderate uplift; maintain resilient baseline coverage year-round",
        "managerial_capacity_note": "Anchor outlet for network stability; strong safe staffing protects continuity and revenue discipline.",
    },
    {
        "outlet_name": "Gozo Spa",
        "outlet_role": "destination_leisure",
        "location_cluster": "gozo_destination",
        "seasonality_class": "high",
        "business_model": "lean_seasonal_experience_led",
        "service_style": "medium_long_experience",
        "pricing_posture": "premium_leisure",
        "treatment_room_count": 5,
        "operating_hours_per_day": 10,
        "open_days_per_week": 7,
        "paid_shift_hours": 8.5,
        "non_sellable_hours": 2.0,
        "minimum_coverage_ratio": 0.40,
        "safe_productive_ratio": 0.60,
        "peak_productive_ratio": 0.79,
        "minimum_opening_team": 2,
        "seasonal_staffing_logic": "lean base in low season; flex upward during leisure peaks and weekend compression",
        "managerial_capacity_note": "Designed to stay commercially disciplined in low season while preserving premium leisure credibility.",
    },
    {
        "outlet_name": "Sliema / Balluta Spa",
        "outlet_role": "premium_urban_lifestyle",
        "location_cluster": "sliema_balluta",
        "seasonality_class": "moderate_high",
        "business_model": "yield_and_experience",
        "service_style": "medium_long_premium",
        "pricing_posture": "premium_high",
        "treatment_room_count": 6,
        "operating_hours_per_day": 11,
        "open_days_per_week": 7,
        "paid_shift_hours": 8.5,
        "non_sellable_hours": 2.0,
        "minimum_coverage_ratio": 0.44,
        "safe_productive_ratio": 0.66,
        "peak_productive_ratio": 0.84,
        "minimum_opening_team": 3,
        "seasonal_staffing_logic": "steady premium urban base with selective tourist uplift; protect service consistency at higher occupancy windows",
        "managerial_capacity_note": "Premium site should optimize yield and consistency, not chase raw throughput.",
    },
    {
        "outlet_name": "Valletta Spa",
        "outlet_role": "compact_premium_city",
        "location_cluster": "valletta_heritage",
        "seasonality_class": "moderate",
        "business_model": "curated_high_experience",
        "service_style": "long_curated_premium",
        "pricing_posture": "high",
        "treatment_room_count": 5,
        "operating_hours_per_day": 10,
        "open_days_per_week": 7,
        "paid_shift_hours": 8.5,
        "non_sellable_hours": 2.0,
        "minimum_coverage_ratio": 0.42,
        "safe_productive_ratio": 0.61,
        "peak_productive_ratio": 0.78,
        "minimum_opening_team": 2,
        "seasonal_staffing_logic": "compact city premium demand; prioritize service pacing and margin over raw volume",
        "managerial_capacity_note": "Smaller footprint is deliberate; this outlet wins through selectivity, yield, and experience quality.",
    },
    {
        "outlet_name": "Mellieha Spa",
        "outlet_role": "resort_leisure",
        "location_cluster": "mellieha_resort",
        "seasonality_class": "high",
        "business_model": "seasonal_resort_mix",
        "service_style": "medium_long_resort",
        "pricing_posture": "upper_mid_premium",
        "treatment_room_count": 6,
        "operating_hours_per_day": 10,
        "open_days_per_week": 7,
        "paid_shift_hours": 8.5,
        "non_sellable_hours": 2.0,
        "minimum_coverage_ratio": 0.41,
        "safe_productive_ratio": 0.63,
        "peak_productive_ratio": 0.84,
        "minimum_opening_team": 3,
        "seasonal_staffing_logic": "base structure remains disciplined; seasonal uplift requires visible peak-readiness planning",
        "managerial_capacity_note": "Resort demand justifies medium scale, but staffing must remain seasonally controlled.",
    },
    {
        "outlet_name": "Qawra / St Paul’s Bay Spa",
        "outlet_role": "tourism_mixed_volume",
        "location_cluster": "qawra_st_pauls_bay",
        "seasonality_class": "high",
        "business_model": "throughput_oriented_mixed_demand",
        "service_style": "short_medium_mixed",
        "pricing_posture": "commercial_premium",
        "treatment_room_count": 6,
        "operating_hours_per_day": 11,
        "open_days_per_week": 7,
        "paid_shift_hours": 8.5,
        "non_sellable_hours": 2.0,
        "minimum_coverage_ratio": 0.43,
        "safe_productive_ratio": 0.64,
        "peak_productive_ratio": 0.86,
        "minimum_opening_team": 3,
        "seasonal_staffing_logic": "tourism-volume dynamics create uneven compression windows; stronger safe coverage supports commercial throughput",
        "managerial_capacity_note": "More throughput-oriented than premium city sites; staffing should absorb mixed volume pressure.",
    },
    {
        "outlet_name": "St Julian’s / Paceville Spa",
        "outlet_role": "high_volatility_commercial_hotspot",
        "location_cluster": "st_julians_paceville",
        "seasonality_class": "very_high",
        "business_model": "high_demand_high_turnover",
        "service_style": "short_medium_faster_mix",
        "pricing_posture": "strong_commercial",
        "treatment_room_count": 7,
        "operating_hours_per_day": 12,
        "open_days_per_week": 7,
        "paid_shift_hours": 8.5,
        "non_sellable_hours": 2.0,
        "minimum_coverage_ratio": 0.48,
        "safe_productive_ratio": 0.72,
        "peak_productive_ratio": 0.92,
        "minimum_opening_team": 4,
        "seasonal_staffing_logic": "hotspot volatility requires stronger opening floor, higher safe coverage, and peak resilience against compression and burnout",
        "managerial_capacity_note": "Dual-risk outlet: revenue capture and team sustainability must be managed together.",
    },
]

master = pd.DataFrame(rows)
master["productive_hours_per_therapist_day"] = (
    master["paid_shift_hours"] - master["non_sellable_hours"]
).round(2)

master["primary_identity"] = ""
master["secondary_risk_identity"] = ""

if INTERPRET_FP.exists():
    try:
        interp = pd.read_csv(INTERPRET_FP)
        keep_cols = [c for c in [
            "outlet_name",
            "primary_identity",
            "secondary_risk_identity"
        ] if c in interp.columns]
        if "outlet_name" in keep_cols:
            interp = interp[keep_cols].drop_duplicates("outlet_name")
            master = master.drop(columns=["primary_identity", "secondary_risk_identity"]).merge(
                interp, on="outlet_name", how="left"
            )
            master["primary_identity"] = master["primary_identity"].fillna("")
            if "secondary_risk_identity" not in master.columns:
                master["secondary_risk_identity"] = ""
            else:
                master["secondary_risk_identity"] = master["secondary_risk_identity"].fillna("")
    except Exception as e:
        print(f"[WARN] failed to merge interpretation layer: {e}")

fallback_primary = {
    "St Julian’s / Paceville Spa": "leakage-risk",
}
fallback_secondary = {
    "St Julian’s / Paceville Spa": "team-strain / burnout-risk",
}

for k, v in fallback_primary.items():
    mask = master["outlet_name"].eq(k) & master["primary_identity"].eq("")
    master.loc[mask, "primary_identity"] = v

for k, v in fallback_secondary.items():
    mask = master["outlet_name"].eq(k) & master["secondary_risk_identity"].eq("")
    master.loc[mask, "secondary_risk_identity"] = v

policy_rows = [
    {
        "policy_id": "OCAP_001",
        "policy_group": "definition_rule",
        "policy_title": "Theoretical room-hour capacity",
        "formula_expression": "treatment_room_count * operating_hours_per_day",
        "management_reasoning": "Daily room-hour capacity before staffing realism and commercial productivity assumptions are applied.",
        "status": "policy_defined",
    },
    {
        "policy_id": "OCAP_002",
        "policy_group": "definition_rule",
        "policy_title": "Productive therapist day",
        "formula_expression": "paid_shift_hours - non_sellable_hours",
        "management_reasoning": "Converts paid shift time into realistic productive therapist hours after breaks, setup, turnover, and admin.",
        "status": "policy_defined",
    },
    {
        "policy_id": "OCAP_003",
        "policy_group": "scenario_rule",
        "policy_title": "Minimum productive capacity",
        "formula_expression": "theoretical_room_hours_per_day * minimum_coverage_ratio",
        "management_reasoning": "Represents minimum credible operating coverage rather than full commercial utilization.",
        "status": "policy_defined",
    },
    {
        "policy_id": "OCAP_004",
        "policy_group": "scenario_rule",
        "policy_title": "Safe productive capacity",
        "formula_expression": "theoretical_room_hours_per_day * safe_productive_ratio",
        "management_reasoning": "Represents healthy operating level for normal service continuity and commercial discipline.",
        "status": "policy_defined",
    },
    {
        "policy_id": "OCAP_005",
        "policy_group": "scenario_rule",
        "policy_title": "Peak productive capacity",
        "formula_expression": "theoretical_room_hours_per_day * peak_productive_ratio",
        "management_reasoning": "Represents peak-season or compression-window capacity requirement.",
        "status": "policy_defined",
    },
    {
        "policy_id": "OCAP_006",
        "policy_group": "staffing_rule",
        "policy_title": "Daily therapist requirement",
        "formula_expression": "ceil(productive_hours_scenario / productive_hours_per_therapist_day)",
        "management_reasoning": "Translates room-hour demand into active daily therapist requirement.",
        "status": "policy_defined",
    },
    {
        "policy_id": "OCAP_007",
        "policy_group": "staffing_rule",
        "policy_title": "Opening team floor",
        "formula_expression": "max(calculated_therapists, minimum_opening_team)",
        "management_reasoning": "Prevents unrealistic opening teams that would weaken service continuity and break coverage.",
        "status": "policy_defined",
    },
    {
        "policy_id": "OCAP_008",
        "policy_group": "staffing_rule",
        "policy_title": "Roster conversion factor",
        "formula_expression": "open_days_per_week / therapist_work_days_per_week",
        "management_reasoning": "Converts active daily need into weekly roster headcount requirement.",
        "status": "policy_defined",
    },
    {
        "policy_id": "OCAP_009",
        "policy_group": "staffing_rule",
        "policy_title": "Roster headcount with resilience buffer",
        "formula_expression": "ceil(active_therapists_per_day * roster_conversion_factor * relief_buffer)",
        "management_reasoning": "Adds workforce resilience for day off coverage, leave, sick risk, and demand variability.",
        "status": "policy_defined",
    },
]
policy = pd.DataFrame(policy_rows)

THERAPIST_WORK_DAYS_PER_WEEK = 5
MIN_RELIEF_BUFFER = 1.05
SAFE_RELIEF_BUFFER = 1.10
PEAK_RELIEF_BUFFER = 1.15
AVG_DAYS_PER_MONTH = 30.4

design = master.copy()

design["theoretical_room_hours_per_day"] = (
    design["treatment_room_count"] * design["operating_hours_per_day"]
).round(2)

design["minimum_productive_hours_per_day"] = (
    design["theoretical_room_hours_per_day"] * design["minimum_coverage_ratio"]
).round(2)

design["safe_productive_hours_per_day"] = (
    design["theoretical_room_hours_per_day"] * design["safe_productive_ratio"]
).round(2)

design["peak_productive_hours_per_day"] = (
    design["theoretical_room_hours_per_day"] * design["peak_productive_ratio"]
).round(2)

def ceil_div(x, y):
    return int(math.ceil(x / y))

design["minimum_therapists_active_per_day"] = design.apply(
    lambda r: max(
        ceil_div(r["minimum_productive_hours_per_day"], r["productive_hours_per_therapist_day"]),
        int(r["minimum_opening_team"])
    ),
    axis=1
)

design["safe_therapists_active_per_day"] = design.apply(
    lambda r: max(
        ceil_div(r["safe_productive_hours_per_day"], r["productive_hours_per_therapist_day"]),
        int(r["minimum_therapists_active_per_day"]) + 1
    ),
    axis=1
)

design["peak_therapists_active_per_day"] = design.apply(
    lambda r: max(
        ceil_div(r["peak_productive_hours_per_day"], r["productive_hours_per_therapist_day"]),
        int(r["safe_therapists_active_per_day"]) + 1
    ),
    axis=1
)

design["roster_conversion_factor"] = (
    design["open_days_per_week"] / THERAPIST_WORK_DAYS_PER_WEEK
).round(4)

design["minimum_roster_headcount"] = design.apply(
    lambda r: int(math.ceil(r["minimum_therapists_active_per_day"] * r["roster_conversion_factor"] * MIN_RELIEF_BUFFER)),
    axis=1
)

design["safe_roster_headcount"] = design.apply(
    lambda r: int(math.ceil(r["safe_therapists_active_per_day"] * r["roster_conversion_factor"] * SAFE_RELIEF_BUFFER)),
    axis=1
)

design["peak_roster_headcount"] = design.apply(
    lambda r: int(math.ceil(r["peak_therapists_active_per_day"] * r["roster_conversion_factor"] * PEAK_RELIEF_BUFFER)),
    axis=1
)

design["monthly_theoretical_room_hours"] = (
    design["theoretical_room_hours_per_day"] * AVG_DAYS_PER_MONTH
).round(1)

design["monthly_minimum_productive_hours"] = (
    design["minimum_productive_hours_per_day"] * AVG_DAYS_PER_MONTH
).round(1)

design["monthly_safe_productive_hours"] = (
    design["safe_productive_hours_per_day"] * AVG_DAYS_PER_MONTH
).round(1)

design["monthly_peak_productive_hours"] = (
    design["peak_productive_hours_per_day"] * AVG_DAYS_PER_MONTH
).round(1)

design["capacity_positioning_summary"] = design.apply(
    lambda r: (
        f"{r['business_model']} | rooms={int(r['treatment_room_count'])} | "
        f"safe_headcount={int(r['safe_roster_headcount'])} | "
        f"peak_headcount={int(r['peak_roster_headcount'])}"
    ),
    axis=1
)

master_cols = [
    "outlet_name",
    "outlet_role",
    "location_cluster",
    "seasonality_class",
    "business_model",
    "service_style",
    "pricing_posture",
    "treatment_room_count",
    "operating_hours_per_day",
    "open_days_per_week",
    "paid_shift_hours",
    "non_sellable_hours",
    "productive_hours_per_therapist_day",
    "minimum_coverage_ratio",
    "safe_productive_ratio",
    "peak_productive_ratio",
    "minimum_opening_team",
    "primary_identity",
    "secondary_risk_identity",
    "seasonal_staffing_logic",
    "managerial_capacity_note",
]

design_cols = [
    "outlet_name",
    "outlet_role",
    "location_cluster",
    "seasonality_class",
    "business_model",
    "service_style",
    "pricing_posture",
    "primary_identity",
    "secondary_risk_identity",
    "treatment_room_count",
    "operating_hours_per_day",
    "productive_hours_per_therapist_day",
    "minimum_coverage_ratio",
    "safe_productive_ratio",
    "peak_productive_ratio",
    "minimum_opening_team",
    "theoretical_room_hours_per_day",
    "minimum_productive_hours_per_day",
    "safe_productive_hours_per_day",
    "peak_productive_hours_per_day",
    "minimum_therapists_active_per_day",
    "safe_therapists_active_per_day",
    "peak_therapists_active_per_day",
    "roster_conversion_factor",
    "minimum_roster_headcount",
    "safe_roster_headcount",
    "peak_roster_headcount",
    "monthly_theoretical_room_hours",
    "monthly_minimum_productive_hours",
    "monthly_safe_productive_hours",
    "monthly_peak_productive_hours",
    "seasonal_staffing_logic",
    "managerial_capacity_note",
    "capacity_positioning_summary",
]

fp_master = OUT_DIR / "outlet_operating_model_master.csv"
fp_policy = OUT_DIR / "outlet_capacity_assumption_policy.csv"
fp_design = OUT_DIR / "outlet_capacity_design_output.csv"

master[master_cols].to_csv(fp_master, index=False)
policy.to_csv(fp_policy, index=False)
design[design_cols].to_csv(fp_design, index=False)

print(f"[OK] saved: {fp_master}")
print(f"[OK] saved: {fp_policy}")
print(f"[OK] saved: {fp_design}")

print("\n=== MASTER ===")
print(master[master_cols].to_string(index=False))

print("\n=== DESIGN OUTPUT ===")
print(
    design[[
        "outlet_name",
        "treatment_room_count",
        "operating_hours_per_day",
        "theoretical_room_hours_per_day",
        "minimum_therapists_active_per_day",
        "safe_therapists_active_per_day",
        "peak_therapists_active_per_day",
        "minimum_roster_headcount",
        "safe_roster_headcount",
        "peak_roster_headcount",
        "primary_identity",
        "secondary_risk_identity",
    ]].to_string(index=False)
)
