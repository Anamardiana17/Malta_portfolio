from pathlib import Path
import math
import pandas as pd

BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")
IN_FP = BASE / "data_processed" / "operating_model" / "outlet_capacity_design_output.csv"
MASTER_FP = BASE / "data_processed" / "operating_model" / "outlet_operating_model_master.csv"
OUT_DIR = BASE / "data_processed" / "operating_model"
OUT_DIR.mkdir(parents=True, exist_ok=True)

design = pd.read_csv(IN_FP)
master = pd.read_csv(MASTER_FP)

quality_map = {
    "Central Malta Spa": {
        "treatment_pacing_model": "balanced_commercial_pacing",
        "treatment_complexity_factor": 0.95,
        "premium_consistency_requirement": "medium",
        "max_treatment_hours_per_therapist_day": 5.8,
        "max_long_treatments_per_therapist_day": 3,
        "max_consecutive_treatment_blocks": 4,
        "recovery_reset_minutes_per_block": 15,
        "service_consistency_buffer": 1,
        "minimum_opening_team_revised": 3,
        "managerial_quality_note": "Balanced commercial outlet; protect pacing and therapist reset discipline.",
    },
    "Gozo Spa": {
        "treatment_pacing_model": "leisure_longer_journey",
        "treatment_complexity_factor": 0.91,
        "premium_consistency_requirement": "medium_high",
        "max_treatment_hours_per_therapist_day": 5.4,
        "max_long_treatments_per_therapist_day": 3,
        "max_consecutive_treatment_blocks": 3,
        "recovery_reset_minutes_per_block": 20,
        "service_consistency_buffer": 1,
        "minimum_opening_team_revised": 2,
        "managerial_quality_note": "Leisure-led experience; do not overload long-treatment therapist pacing.",
    },
    "Sliema / Balluta Spa": {
        "treatment_pacing_model": "premium_consistency_protected",
        "treatment_complexity_factor": 0.90,
        "premium_consistency_requirement": "high",
        "max_treatment_hours_per_therapist_day": 5.2,
        "max_long_treatments_per_therapist_day": 3,
        "max_consecutive_treatment_blocks": 3,
        "recovery_reset_minutes_per_block": 20,
        "service_consistency_buffer": 2,
        "minimum_opening_team_revised": 3,
        "managerial_quality_note": "Premium urban outlet; consistency matters more than aggressive room utilization.",
    },
    "Valletta Spa": {
        "treatment_pacing_model": "curated_premium_protected",
        "treatment_complexity_factor": 0.88,
        "premium_consistency_requirement": "very_high",
        "max_treatment_hours_per_therapist_day": 5.0,
        "max_long_treatments_per_therapist_day": 2,
        "max_consecutive_treatment_blocks": 3,
        "recovery_reset_minutes_per_block": 20,
        "service_consistency_buffer": 2,
        "minimum_opening_team_revised": 2,
        "managerial_quality_note": "Curated premium site; deliberately protect therapist pacing and treatment quality.",
    },
    "Mellieha Spa": {
        "treatment_pacing_model": "resort_peak_protected",
        "treatment_complexity_factor": 0.92,
        "premium_consistency_requirement": "high",
        "max_treatment_hours_per_therapist_day": 5.4,
        "max_long_treatments_per_therapist_day": 3,
        "max_consecutive_treatment_blocks": 3,
        "recovery_reset_minutes_per_block": 20,
        "service_consistency_buffer": 1,
        "minimum_opening_team_revised": 4,
        "managerial_quality_note": "Resort outlet needs a stronger minimum floor so peak compression does not damage service quality.",
    },
    "Qawra / St Paul’s Bay Spa": {
        "treatment_pacing_model": "throughput_standardized_control",
        "treatment_complexity_factor": 0.96,
        "premium_consistency_requirement": "medium",
        "max_treatment_hours_per_therapist_day": 5.9,
        "max_long_treatments_per_therapist_day": 3,
        "max_consecutive_treatment_blocks": 4,
        "recovery_reset_minutes_per_block": 15,
        "service_consistency_buffer": 1,
        "minimum_opening_team_revised": 3,
        "managerial_quality_note": "Volume-oriented tourism site; prioritize standardization and therapist rhythm control.",
    },
    "St Julian’s / Paceville Spa": {
        "treatment_pacing_model": "high_compression_protected",
        "treatment_complexity_factor": 0.94,
        "premium_consistency_requirement": "high",
        "max_treatment_hours_per_therapist_day": 5.6,
        "max_long_treatments_per_therapist_day": 3,
        "max_consecutive_treatment_blocks": 4,
        "recovery_reset_minutes_per_block": 15,
        "service_consistency_buffer": 2,
        "minimum_opening_team_revised": 4,
        "managerial_quality_note": "Hotspot site needs stronger therapist protection so revenue capture does not erode consistency.",
    },
}

qdf = (
    pd.DataFrame.from_dict(quality_map, orient="index")
    .reset_index()
    .rename(columns={"index": "outlet_name"})
)

# Merge only when needed, and normalize duplicate columns safely
df = design.merge(
    master,
    on="outlet_name",
    how="left",
    suffixes=("", "_master")
).merge(
    qdf,
    on="outlet_name",
    how="left"
)

def pick_preferred_column(df, base_name):
    if base_name in df.columns:
        return
    alt_names = [
        f"{base_name}_master",
        f"{base_name}_x",
        f"{base_name}_y",
    ]
    for alt in alt_names:
        if alt in df.columns:
            df[base_name] = df[alt]
            return
    df[base_name] = ""

pick_preferred_column(df, "service_style")
pick_preferred_column(df, "pricing_posture")

df["base_productive_hours_per_therapist_day"] = df["productive_hours_per_therapist_day"]
df["quality_adjusted_productive_hours_per_therapist_day"] = (
    df["base_productive_hours_per_therapist_day"] * df["treatment_complexity_factor"]
).round(2)

def ceil_div(x, y):
    return int(math.ceil(x / y))

df["revised_minimum_therapists_active_per_day"] = df.apply(
    lambda r: max(
        ceil_div(r["minimum_productive_hours_per_day"], r["quality_adjusted_productive_hours_per_therapist_day"]),
        int(r["minimum_opening_team_revised"])
    ),
    axis=1
)

df["revised_safe_therapists_active_per_day"] = df.apply(
    lambda r: max(
        ceil_div(r["safe_productive_hours_per_day"], r["quality_adjusted_productive_hours_per_therapist_day"]),
        int(r["revised_minimum_therapists_active_per_day"]) + int(r["service_consistency_buffer"])
    ),
    axis=1
)

df["revised_peak_therapists_active_per_day"] = df.apply(
    lambda r: max(
        ceil_div(r["peak_productive_hours_per_day"], r["quality_adjusted_productive_hours_per_therapist_day"]),
        int(r["revised_safe_therapists_active_per_day"]) + 1
    ),
    axis=1
)

MIN_RELIEF_BUFFER = 1.05
SAFE_RELIEF_BUFFER = 1.10
PEAK_RELIEF_BUFFER = 1.15

df["revised_minimum_roster_headcount"] = df.apply(
    lambda r: int(math.ceil(r["revised_minimum_therapists_active_per_day"] * r["roster_conversion_factor"] * MIN_RELIEF_BUFFER)),
    axis=1
)

df["revised_safe_roster_headcount"] = df.apply(
    lambda r: int(math.ceil(r["revised_safe_therapists_active_per_day"] * r["roster_conversion_factor"] * SAFE_RELIEF_BUFFER)),
    axis=1
)

df["revised_peak_roster_headcount"] = df.apply(
    lambda r: int(math.ceil(r["revised_peak_therapists_active_per_day"] * r["roster_conversion_factor"] * PEAK_RELIEF_BUFFER)),
    axis=1
)

df["roster_quality_positioning_summary"] = df.apply(
    lambda r: (
        f"{r['treatment_pacing_model']} | "
        f"quality_adj_hours={r['quality_adjusted_productive_hours_per_therapist_day']} | "
        f"safe_roster={int(r['revised_safe_roster_headcount'])} | "
        f"peak_roster={int(r['revised_peak_roster_headcount'])}"
    ),
    axis=1
)

policy_rows = [
    {
        "policy_id": "TSVC_001",
        "policy_group": "quality_rule",
        "policy_title": "Quality-adjusted therapist day",
        "formula_expression": "productive_hours_per_therapist_day * treatment_complexity_factor",
        "management_reasoning": "Therapist usable capacity must be discounted when treatment style is longer, more curated, or more consistency-sensitive.",
        "status": "policy_defined",
    },
    {
        "policy_id": "TSVC_002",
        "policy_group": "service_rule",
        "policy_title": "Consistency-protected safe staffing",
        "formula_expression": "max(ceil(safe_productive_hours_per_day / quality_adjusted_productive_hours_per_therapist_day), revised_minimum_therapists_active_per_day + service_consistency_buffer)",
        "management_reasoning": "Safe staffing should reflect both demand coverage and service consistency protection.",
        "status": "policy_defined",
    },
    {
        "policy_id": "TSVC_003",
        "policy_group": "fatigue_rule",
        "policy_title": "Daily treatment intensity guardrail",
        "formula_expression": "max_treatment_hours_per_therapist_day, max_long_treatments_per_therapist_day, max_consecutive_treatment_blocks",
        "management_reasoning": "Therapist load must be managed to protect treatment quality and repeatable guest experience.",
        "status": "policy_defined",
    },
    {
        "policy_id": "TSVC_004",
        "policy_group": "service_rule",
        "policy_title": "Recovery reset allowance",
        "formula_expression": "recovery_reset_minutes_per_block",
        "management_reasoning": "Short recovery/reset windows reduce service deterioration and therapist fatigue accumulation.",
        "status": "policy_defined",
    },
]

policy = pd.DataFrame(policy_rows)

service_model_cols = [
    "outlet_name",
    "service_style",
    "pricing_posture",
    "treatment_pacing_model",
    "treatment_complexity_factor",
    "premium_consistency_requirement",
    "max_treatment_hours_per_therapist_day",
    "max_long_treatments_per_therapist_day",
    "max_consecutive_treatment_blocks",
    "recovery_reset_minutes_per_block",
    "service_consistency_buffer",
    "managerial_quality_note",
]

roster_cols = [
    "outlet_name",
    "base_productive_hours_per_therapist_day",
    "treatment_complexity_factor",
    "quality_adjusted_productive_hours_per_therapist_day",
    "service_consistency_buffer",
    "minimum_opening_team",
    "minimum_opening_team_revised",
    "minimum_therapists_active_per_day",
    "safe_therapists_active_per_day",
    "peak_therapists_active_per_day",
    "revised_minimum_therapists_active_per_day",
    "revised_safe_therapists_active_per_day",
    "revised_peak_therapists_active_per_day",
    "minimum_roster_headcount",
    "safe_roster_headcount",
    "peak_roster_headcount",
    "revised_minimum_roster_headcount",
    "revised_safe_roster_headcount",
    "revised_peak_roster_headcount",
    "roster_quality_positioning_summary",
]

fp_roster = OUT_DIR / "outlet_roster_quality_design_output.csv"
fp_policy = OUT_DIR / "treatment_service_consistency_policy.csv"
fp_service = OUT_DIR / "outlet_treatment_service_model.csv"

df[roster_cols].to_csv(fp_roster, index=False)
policy.to_csv(fp_policy, index=False)
df[service_model_cols].to_csv(fp_service, index=False)

print(f"[OK] saved: {fp_roster}")
print(f"[OK] saved: {fp_policy}")
print(f"[OK] saved: {fp_service}")

print("\n=== ROSTER QUALITY OUTPUT ===")
print(df[[
    "outlet_name",
    "service_style",
    "pricing_posture",
    "quality_adjusted_productive_hours_per_therapist_day",
    "minimum_opening_team_revised",
    "revised_minimum_therapists_active_per_day",
    "revised_safe_therapists_active_per_day",
    "revised_peak_therapists_active_per_day",
    "revised_minimum_roster_headcount",
    "revised_safe_roster_headcount",
    "revised_peak_roster_headcount",
]].to_string(index=False))

print("\n=== TREATMENT SERVICE MODEL ===")
print(df[service_model_cols].to_string(index=False))
