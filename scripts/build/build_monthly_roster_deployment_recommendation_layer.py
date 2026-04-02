from pathlib import Path
import pandas as pd
import numpy as np

BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")

MGMT_FP = BASE / "data_processed/management/external_internal_management_context_monthly.csv"
OUT_FP = BASE / "data_processed/management/monthly_roster_deployment_recommendation.csv"
OUT_FP.parent.mkdir(parents=True, exist_ok=True)

def read_csv_safe(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    print(f"[INFO] reading: {path}")
    return pd.read_csv(path)

def as_num(df, col):
    if col not in df.columns:
        return pd.Series(np.nan, index=df.index)
    return pd.to_numeric(df[col], errors="coerce")

def classify_risk_flag(row):
    burn = row["burnout_exposure_score_0_100"]
    coverage = row["coverage_pressure_score_0_100"]
    readiness = row["staffing_readiness_score_0_100"]
    expansion = row["capacity_expansion_readiness_score_0_100"]

    if pd.notna(burn) and burn >= 80:
        return "burnout_risk_high"
    if pd.notna(coverage) and coverage >= 80:
        return "coverage_pressure_high"
    if pd.notna(readiness) and readiness < 40:
        return "staffing_readiness_low"
    if pd.notna(expansion) and expansion < 40:
        return "capacity_expansion_not_ready"
    return "normal_watch"

def recommend_action(row):
    ext = row["external_demand_context_score_0_100"]
    coverage = row["coverage_pressure_score_0_100"]
    burn = row["burnout_exposure_score_0_100"]
    readiness = row["staffing_readiness_score_0_100"]
    expansion = row["capacity_expansion_readiness_score_0_100"]
    window = str(row.get("growth_window_vs_control_window", ""))
    posture = str(row.get("roster_action_posture", ""))

    if pd.notna(burn) and burn >= 80:
        return "protect_recovery_do_not_expand"
    if pd.notna(readiness) and readiness < 40:
        return "stabilize_roster_before_expansion"
    if pd.notna(coverage) and coverage >= 80:
        return "repair_coverage_before_growth"
    if window == "protect_team_capacity":
        return "protect_team_capacity_hold_expansion"
    if window == "hold_and_optimize":
        return "hold_capacity_optimize_controls"
    if pd.notna(ext) and ext >= 65 and pd.notna(expansion) and expansion >= 65 and pd.notna(coverage) and coverage < 60:
        return "selective_capacity_increase"
    if posture == "maintain_capacity_optimize_controls":
        return "hold_capacity_optimize_controls"
    return "maintain_capacity"

def recommend_coverage_mode(row):
    burn = row["burnout_exposure_score_0_100"]
    coverage = row["coverage_pressure_score_0_100"]
    readiness = row["staffing_readiness_score_0_100"]
    ext = row["external_demand_context_score_0_100"]

    if pd.notna(burn) and burn >= 80:
        return "protect_recovery_capacity"
    if pd.notna(coverage) and coverage >= 80:
        return "repair_core_coverage"
    if pd.notna(ext) and ext >= 65 and pd.notna(readiness) and readiness >= 65:
        return "selective_flexible_support"
    return "maintain_current_coverage"

def recommend_capacity_posture(row):
    action = row["recommended_roster_action"]
    if action in {"protect_recovery_do_not_expand", "stabilize_roster_before_expansion", "repair_coverage_before_growth", "protect_team_capacity_hold_expansion"}:
        return "hold_or_reduce_strain"
    if action == "selective_capacity_increase":
        return "selective_capacity_increase"
    if action == "hold_capacity_optimize_controls":
        return "hold_capacity_optimize_mix"
    return "maintain_capacity"

def reason_text(row):
    parts = []

    burn = row["burnout_exposure_score_0_100"]
    coverage = row["coverage_pressure_score_0_100"]
    readiness = row["staffing_readiness_score_0_100"]
    ext = row["external_demand_context_score_0_100"]
    regime = row.get("external_context_regime", "")
    window = row.get("growth_window_vs_control_window", "")
    action = row["recommended_roster_action"]

    if pd.notna(burn):
        if burn >= 80:
            parts.append("team strain and burnout exposure are too high")
        elif burn < 60:
            parts.append("burnout exposure is manageable")

    if pd.notna(coverage):
        if coverage >= 80:
            parts.append("coverage pressure is materially elevated")
        elif coverage < 60:
            parts.append("coverage pressure is still within manageable range")

    if pd.notna(readiness):
        if readiness < 40:
            parts.append("staffing readiness is below safe expansion threshold")
        elif readiness >= 65:
            parts.append("staffing readiness is supportive")

    if pd.notna(ext):
        if ext >= 65:
            parts.append("external demand context is supportive")
        elif ext < 45:
            parts.append("external demand context is soft")

    if str(regime):
        parts.append(f"external regime reads as {regime}")
    if str(window):
        parts.append(f"monthly management context reads as {window}")

    if not parts:
        parts.append("monthly roster posture should be monitored")

    return "; ".join(parts) + f"; recommended action: {action}"

def priority_bucket(x):
    if pd.isna(x):
        return "unclassified"
    if x >= 45:
        return "high"
    if x >= 35:
        return "medium"
    return "low"

mg = read_csv_safe(MGMT_FP).copy()

rename_map = {}
if "outlet_id" in mg.columns and "outlet_key" not in mg.columns:
    rename_map["outlet_id"] = "outlet_key"
mg = mg.rename(columns=rename_map)

required = [
    "outlet_key",
    "month_id",
    "external_demand_context_score_0_100",
    "coverage_pressure_score_0_100",
    "burnout_exposure_score_0_100",
    "staffing_readiness_score_0_100",
    "capacity_expansion_readiness_score_0_100",
]
missing = [c for c in required if c not in mg.columns]
if missing:
    raise KeyError(f"[STOP] Missing required management columns: {missing}")

num_cols = [
    "external_demand_context_score_0_100",
    "external_signal_confidence_score_0_100",
    "coverage_pressure_score_0_100",
    "burnout_exposure_score_0_100",
    "staffing_readiness_score_0_100",
    "capacity_expansion_readiness_score_0_100",
    "external_internal_alignment_score_0_100",
]
for c in num_cols:
    if c in mg.columns:
        mg[c] = as_num(mg, c)

signal_conf = mg["external_signal_confidence_score_0_100"] if "external_signal_confidence_score_0_100" in mg.columns else pd.Series(np.nan, index=mg.index)
if signal_conf.notna().sum() == 0:
    signal_conf = pd.Series(50.0, index=mg.index)

align = mg["external_internal_alignment_score_0_100"] if "external_internal_alignment_score_0_100" in mg.columns else pd.Series(50.0, index=mg.index)

mg["roster_risk_flag"] = mg.apply(classify_risk_flag, axis=1)
mg["recommended_roster_action"] = mg.apply(recommend_action, axis=1)
mg["recommended_coverage_mode"] = mg.apply(recommend_coverage_mode, axis=1)
mg["recommended_capacity_posture"] = mg.apply(recommend_capacity_posture, axis=1)
mg["managerial_roster_reason"] = mg.apply(reason_text, axis=1)

ext = as_num(mg, "external_demand_context_score_0_100")
coverage = as_num(mg, "coverage_pressure_score_0_100")
burn = as_num(mg, "burnout_exposure_score_0_100")
readiness = as_num(mg, "staffing_readiness_score_0_100")
expansion = as_num(mg, "capacity_expansion_readiness_score_0_100")

mg["roster_decision_priority_score_0_100"] = (
    ext * 0.20
    + coverage * 0.20
    + burn * 0.20
    + (100 - readiness) * 0.15
    + (100 - expansion) * 0.15
    + (100 - align) * 0.05
    + (100 - signal_conf) * 0.05
).clip(0, 100)

mg["roster_decision_priority_band"] = mg["roster_decision_priority_score_0_100"].apply(priority_bucket)

preferred_cols = [
    "outlet_key",
    "month_id",
    "external_context_regime",
    "growth_window_vs_control_window",
    "roster_action_posture",
    "external_demand_context_score_0_100",
    "external_signal_confidence_score_0_100",
    "external_internal_alignment_score_0_100",
    "coverage_pressure_score_0_100",
    "burnout_exposure_score_0_100",
    "staffing_readiness_score_0_100",
    "capacity_expansion_readiness_score_0_100",
    "roster_risk_flag",
    "recommended_roster_action",
    "recommended_coverage_mode",
    "recommended_capacity_posture",
    "roster_decision_priority_score_0_100",
    "roster_decision_priority_band",
    "managerial_roster_reason",
]

keep = [c for c in preferred_cols if c in mg.columns]
out = mg[keep].copy().sort_values(["outlet_key", "month_id"]).reset_index(drop=True)
out.to_csv(OUT_FP, index=False)

print(f"[OK] saved: {OUT_FP}")
print("\n=== RECOMMENDED ROSTER ACTION ===")
print(out["recommended_roster_action"].value_counts(dropna=False).to_string())
print("\n=== ROSTER DECISION PRIORITY BAND ===")
print(out["roster_decision_priority_band"].value_counts(dropna=False).to_string())
