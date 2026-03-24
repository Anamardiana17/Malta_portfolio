from pathlib import Path
import pandas as pd
import numpy as np

BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")

MGMT_CONTEXT_FP = BASE / "data_processed/management/external_internal_management_context_monthly.csv"
MGMT_SIGNAL_FP = BASE / "data_processed/internal_proxy/management_kpi_signal_layer.csv"
OUT_FP = BASE / "data_processed/management/executive_staffing_action_summary.csv"

OUT_FP.parent.mkdir(parents=True, exist_ok=True)

def read_csv_safe(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    print(f"[INFO] reading: {path}")
    return pd.read_csv(path)

def first_existing(df, candidates, required=False):
    for c in candidates:
        if c in df.columns:
            return c
    if required:
        raise KeyError(f"Missing required columns. Tried: {candidates}")
    return None

def to_month_id(series):
    s = pd.to_datetime(series, errors="coerce")
    return s.dt.to_period("M").astype("string")

def as_num(df, col):
    if col is None or col not in df.columns:
        return pd.Series(np.nan, index=df.index)
    s = pd.to_numeric(df[col], errors="coerce")
    if s.dropna().between(0, 1).mean() > 0.8:
        s = s * 100
    return s.clip(0, 100)

def weighted_mean_df(df, parts):
    num = pd.Series(0.0, index=df.index)
    den = pd.Series(0.0, index=df.index)
    for col, w, inverse in parts:
        s = as_num(df, col)
        if inverse:
            s = 100 - s
        mask = s.notna()
        num = num + s.fillna(0) * w
        den = den + mask.astype(float) * w
    out = num / den.replace(0, np.nan)
    return out.clip(0, 100)

mg = read_csv_safe(MGMT_CONTEXT_FP)
sig = read_csv_safe(MGMT_SIGNAL_FP)

mg_outlet = first_existing(mg, ["outlet_key", "outlet_id", "outlet_name"], required=True)
sig_outlet = first_existing(sig, ["outlet_id", "outlet_key", "outlet_name"], required=True)

mg = mg.rename(columns={mg_outlet: "outlet_key"})
sig = sig.rename(columns={sig_outlet: "outlet_key"})

mg_month_col = first_existing(mg, ["month_id", "period_start", "month"], required=True)
sig_month_col = first_existing(sig, ["month_id", "period_start", "month"], required=True)

mg["month_id"] = to_month_id(mg[mg_month_col])
sig["month_id"] = to_month_id(sig[sig_month_col])

mg = mg[mg["month_id"].notna()].copy()
sig = sig[sig["month_id"].notna()].copy()

mg = mg.sort_values(["outlet_key", "month_id"]).drop_duplicates(["outlet_key", "month_id"], keep="first")
sig = sig.sort_values(["outlet_key", "month_id"]).drop_duplicates(["outlet_key", "month_id"], keep="first")

sig_keep = [c for c in [
    "outlet_key",
    "month_id",
    "overall_management_signal_score_0_100",
    "management_signal",
    "recommended_manager_action",
    "avg_capacity_strain_score_0_100",
    "avg_roster_operational_health_score_0_100",
    "avg_burnout_risk_score_0_100",
    "avg_external_demand_proxy_index",
    "revenue_growth_readiness_flag",
    "leakage_control_flag",
    "qa_status",
    "audit_note",
    "status",
    "outlet_name",
] if c in sig.columns]

sig = sig[sig_keep].copy()
df = mg.merge(sig, on=["outlet_key", "month_id"], how="left", suffixes=("", "_sig"))

df["management_signal_score_0_100_final"] = as_num(df, "management_signal_score_0_100").combine_first(
    as_num(df, "overall_management_signal_score_0_100")
)
df["roster_operational_health_score_0_100_final"] = as_num(df, "roster_integrity_health_score_0_100").combine_first(
    as_num(df, "avg_roster_operational_health_score_0_100")
)
df["capacity_strain_score_0_100_final"] = as_num(df, "coverage_pressure_score_0_100").combine_first(
    as_num(df, "avg_capacity_strain_score_0_100")
)
df["burnout_risk_score_0_100_final"] = as_num(df, "burnout_exposure_score_0_100").combine_first(
    as_num(df, "avg_burnout_risk_score_0_100")
)

df["external_demand_context_score_0_100"] = as_num(df, "external_demand_context_score_0_100")
df["staffing_readiness_score_0_100"] = as_num(df, "staffing_readiness_score_0_100")
df["coverage_pressure_score_0_100"] = as_num(df, "coverage_pressure_score_0_100")
df["burnout_exposure_score_0_100"] = as_num(df, "burnout_exposure_score_0_100")
df["capacity_expansion_readiness_score_0_100"] = as_num(df, "capacity_expansion_readiness_score_0_100")
df["external_internal_alignment_score_0_100"] = as_num(df, "external_internal_alignment_score_0_100")
df["demand_support_vs_staffing_readiness_gap"] = pd.to_numeric(
    df.get("demand_support_vs_staffing_readiness_gap"), errors="coerce"
)

df["executive_staffing_confidence_score_0_100"] = weighted_mean_df(df, [
    ("staffing_readiness_score_0_100", 0.40, False),
    ("burnout_risk_score_0_100_final", 0.20, True),
    ("capacity_strain_score_0_100_final", 0.15, True),
    ("roster_operational_health_score_0_100_final", 0.15, False),
    ("management_signal_score_0_100_final", 0.10, False),
]).round(2)

def confidence_band(score):
    if pd.isna(score):
        return "review_required"
    if score >= 70:
        return "high_confidence"
    if score >= 50:
        return "moderate_confidence"
    return "low_confidence"

df["executive_action_confidence_band"] = df["executive_staffing_confidence_score_0_100"].apply(confidence_band)

def staffing_posture(row):
    ext = row["external_demand_context_score_0_100"]
    ready = row["staffing_readiness_score_0_100"]
    burn = row["burnout_risk_score_0_100_final"]
    strain = row["capacity_strain_score_0_100_final"]
    expand = row["capacity_expansion_readiness_score_0_100"]
    conf = row["executive_staffing_confidence_score_0_100"]
    gap = row["demand_support_vs_staffing_readiness_gap"]
    align = row["external_internal_alignment_score_0_100"]
    window = str(row.get("growth_window_vs_control_window", ""))

    if pd.notna(burn) and burn >= 65:
        return "protect_team_capacity"
    if pd.notna(strain) and strain >= 60:
        return "coverage_under_pressure"
    if pd.notna(ready) and ready < 50:
        return "stabilize_roster_first"
    if pd.notna(ext) and pd.notna(ready) and ext >= 55 and ready < 60:
        return "demand_supported_but_roster_not_ready"
    if pd.notna(expand) and pd.notna(conf) and expand >= 60 and conf >= 65 and (pd.isna(burn) or burn < 65):
        return "selective_growth_supported"
    if window == "control_window" and pd.notna(conf) and conf >= 60:
        return "hold_capacity_optimize_controls"
    if pd.notna(gap) and gap <= -15 and pd.notna(conf) and conf >= 55:
        return "hold_capacity_optimize_controls"
    if pd.notna(align) and align < 45:
        return "investigate_signal_misalignment"
    return "monitor_and_calibrate"

def staffing_risk_headline(row):
    burn = row["burnout_risk_score_0_100_final"]
    strain = row["capacity_strain_score_0_100_final"]
    ready = row["staffing_readiness_score_0_100"]
    gap = row["demand_support_vs_staffing_readiness_gap"]
    align = row["external_internal_alignment_score_0_100"]

    if pd.notna(burn) and burn >= 65:
        return "Burnout exposure is the main staffing risk."
    if pd.notna(strain) and strain >= 60:
        return "Coverage pressure is the main staffing risk."
    if pd.notna(ready) and ready < 50:
        return "Staffing readiness is below safe expansion level."
    if pd.notna(gap) and gap >= 10:
        return "Demand context is ahead of staffing readiness."
    if pd.notna(gap) and gap <= -15:
        return "Staffing may be ahead of current demand support."
    if pd.notna(align) and align < 45:
        return "External and internal signals are not yet well aligned."
    return "Staffing position is broadly manageable with monitoring."

def executive_action(row):
    posture = row["executive_staffing_posture"]
    mgr = row.get("recommended_manager_action", None)
    leakage_flag = row.get("leakage_control_flag", None)

    if posture == "protect_team_capacity":
        return "Protect team capacity first; avoid roster expansion until strain and burnout normalize."
    if posture == "coverage_under_pressure":
        return "Prioritize coverage stabilization, shift discipline, and service continuity before growth actions."
    if posture == "stabilize_roster_first":
        return "Stabilize roster health, adherence, and staffing readiness before adding capacity."
    if posture == "demand_supported_but_roster_not_ready":
        return "Use demand support as a watch signal, but hold fixed expansion until roster readiness improves."
    if posture == "selective_growth_supported":
        return "Use selective capacity growth in supported periods while protecting service quality."
    if posture == "hold_capacity_optimize_controls":
        return "Hold capacity steady and optimize utilization, control discipline, and staffing mix."
    if posture == "investigate_signal_misalignment":
        return "Review demand context, staffing posture, and operating controls before making expansion decisions."
    if isinstance(mgr, str) and mgr.strip():
        return mgr.strip()
    if leakage_flag == 1:
        return "Tighten leakage control and monitor staffing efficiency before growth decisions."
    return "Maintain current staffing posture and monitor for changes in demand support and team pressure."

df["executive_staffing_posture"] = df.apply(staffing_posture, axis=1)
df["staffing_risk_headline"] = df.apply(staffing_risk_headline, axis=1)
df["executive_staffing_action_summary"] = df.apply(executive_action, axis=1)

preferred_cols = [c for c in [
    "outlet_key",
    "outlet_name",
    "month_id",
    "external_demand_context_score_0_100",
    "external_signal_confidence_score_0_100",
    "staffing_readiness_score_0_100",
    "coverage_pressure_score_0_100",
    "burnout_exposure_score_0_100",
    "capacity_expansion_readiness_score_0_100",
    "demand_support_vs_staffing_readiness_gap",
    "external_internal_alignment_score_0_100",
    "growth_window_vs_control_window",
    "roster_action_posture",
    "management_signal_score_0_100_final",
    "roster_operational_health_score_0_100_final",
    "capacity_strain_score_0_100_final",
    "burnout_risk_score_0_100_final",
    "executive_staffing_confidence_score_0_100",
    "executive_action_confidence_band",
    "executive_staffing_posture",
    "staffing_risk_headline",
    "executive_staffing_action_summary",
    "management_signal",
    "recommended_manager_action",
    "revenue_growth_readiness_flag",
    "leakage_control_flag",
    "qa_status",
    "audit_note",
    "status",
] if c in df.columns]

out = df[preferred_cols].copy()
out = out.sort_values(["outlet_key", "month_id"]).reset_index(drop=True)
out.to_csv(OUT_FP, index=False)

print(f"[OK] saved: {OUT_FP}")
print(f"[OK] rows: {len(out)}")

print("\n=== EXECUTIVE STAFFING POSTURE COUNTS ===")
print(out["executive_staffing_posture"].value_counts(dropna=False).to_string())

print("\n=== ACTION CONFIDENCE BAND COUNTS ===")
print(out["executive_action_confidence_band"].value_counts(dropna=False).to_string())

print("\n=== SAMPLE ===")
sample_cols = [c for c in [
    "outlet_key",
    "month_id",
    "growth_window_vs_control_window",
    "staffing_readiness_score_0_100",
    "coverage_pressure_score_0_100",
    "burnout_risk_score_0_100_final",
    "capacity_expansion_readiness_score_0_100",
    "executive_staffing_confidence_score_0_100",
    "executive_staffing_posture",
    "staffing_risk_headline",
    "executive_staffing_action_summary",
] if c in out.columns]
print(out[sample_cols].head(25).to_string(index=False))
