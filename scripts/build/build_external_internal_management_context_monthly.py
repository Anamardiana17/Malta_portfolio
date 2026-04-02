from pathlib import Path
import pandas as pd

def derive_productive_utilization_score_0_100(df):
    import pandas as pd

    if "utilization_on_roster_ratio" in df.columns:
        s = pd.to_numeric(df["utilization_on_roster_ratio"], errors="coerce")
        return (s * 100).clip(0, 100)

    if {"booked_hours", "productive_capacity_hours"}.issubset(df.columns):
        booked = pd.to_numeric(df["booked_hours"], errors="coerce")
        cap = pd.to_numeric(df["productive_capacity_hours"], errors="coerce").replace(0, pd.NA)
        return ((booked / cap) * 100).clip(0, 100)

    if {"booked_hours", "scheduled_hours"}.issubset(df.columns):
        booked = pd.to_numeric(df["booked_hours"], errors="coerce")
        sched = pd.to_numeric(df["scheduled_hours"], errors="coerce").replace(0, pd.NA)
        return ((booked / sched) * 100).clip(0, 100)

    return pd.Series(pd.NA, index=df.index, dtype="float64")

import numpy as np

BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")

OUT_FP = BASE / "data_processed/management/external_internal_management_context_monthly.csv"
OUT_FP.parent.mkdir(parents=True, exist_ok=True)

def read_csv_safe(path: Path):
    if path.exists():
        print(f"[INFO] reading: {path}")
        return pd.read_csv(path)
    print(f"[WARN] missing: {path}")
    return None

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

def normalize_0_100(series):
    s = pd.to_numeric(series, errors="coerce")
    if s.dropna().between(0, 1).mean() > 0.8:
        s = s * 100
    return s.clip(0, 100)

def minmax_0_100(series):
    s = pd.to_numeric(series, errors="coerce")
    if s.notna().sum() == 0:
        return pd.Series(np.nan, index=series.index)
    mn, mx = s.min(), s.max()
    if pd.isna(mn) or pd.isna(mx) or mn == mx:
        return pd.Series(50.0, index=series.index)
    return ((s - mn) / (mx - mn) * 100).clip(0, 100)

def weighted_mean_df(df, parts):
    num = pd.Series(0.0, index=df.index)
    den = pd.Series(0.0, index=df.index)
    for col, weight, inverse in parts:
        if col not in df.columns:
            s = pd.Series(np.nan, index=df.index)
        else:
            s = normalize_0_100(df[col])
        if inverse:
            s = 100 - s
        mask = s.notna()
        num = num + s.fillna(0) * weight
        den = den + mask.astype(float) * weight
    return (num / den.replace(0, np.nan)).clip(0, 100)

# -----------------------------
# Load
# -----------------------------
external_fp = BASE / "data_processed/final_bundle/malta_external_proxy_monthly_2017_2025_clean.csv"
roster_integrity_fp = BASE / "data_processed/internal_proxy/internal_proxy_roster_integrity_monthly.csv"
capacity_day_fp = BASE / "data_processed/internal_proxy/internal_proxy_roster_capacity_day.csv"

burnout_fp = BASE / "data_processed/internal_proxy/internal_proxy_burnout_risk_monthly.csv"
mgmt_signal_fp = BASE / "data_processed/internal_proxy/management_kpi_signal_layer.csv"

external = read_csv_safe(external_fp)
ri = read_csv_safe(roster_integrity_fp)
cap_day = read_csv_safe(capacity_day_fp)
burn = read_csv_safe(burnout_fp)
mgmt = read_csv_safe(mgmt_signal_fp)

if any(x is None for x in [external, ri, cap_day, burn]):
    raise SystemExit("[STOP] One or more core inputs are missing.")

# -----------------------------
# External: monthly shared context
# -----------------------------
ext = external.copy()
ext_month = first_existing(ext, ["month_id", "period_month", "month", "period"], required=True)
ext["month_id"] = to_month_id(ext[ext_month])

score_col = first_existing(
    ext,
    [
        "external_demand_flow_proxy_score_0_100",
        "footfall_proxy_score_0_100",
        "external_demand_proxy_score_0_100",
        "demand_flow_proxy_score_0_100",
    ],
    required=False,
)

if score_col:
    ext["external_demand_context_score_0_100"] = normalize_0_100(ext[score_col])
else:
    proxy_parts = []
    for c in [
        "accommodation_nights_spent",
        "airport_passengers_international",
        "tourism_demand_air_passengers_international",
    ]:
        if c in ext.columns:
            proxy_parts.append(minmax_0_100(ext[c]))
    if not proxy_parts:
        raise SystemExit("[STOP] No usable external proxy columns found.")
    ext["external_demand_context_score_0_100"] = pd.concat(proxy_parts, axis=1).mean(axis=1)

quality_col = first_existing(ext, ["footfall_proxy_signal_quality", "external_signal_quality"], required=False)
regime_col = first_existing(ext, ["footfall_proxy_weight_regime", "weight_regime"], required=False)
air_flag_col = first_existing(ext, ["footfall_proxy_air_component_present_flag", "air_component_present_flag"], required=False)

def quality_to_score(x):
    x = str(x).strip().lower()
    if x == "balanced_full_blend":
        return 90
    if x == "accommodation_only":
        return 55
    if x in {"partial_blend", "limited_blend"}:
        return 70
    return np.nan

if quality_col:
    ext["external_signal_confidence_score_0_100"] = ext[quality_col].map(quality_to_score)
else:
    # Neutral placeholder when no external signal quality source is available.
    ext["external_signal_confidence_score_0_100"] = 50.0

ext["external_context_regime"] = np.select(
    [
        ext["external_demand_context_score_0_100"] >= 67,
        ext["external_demand_context_score_0_100"] >= 34,
    ],
    ["supportive", "neutral"],
    default="soft"
)
ext["external_growth_support_flag"] = (ext["external_demand_context_score_0_100"] >= 60).astype(int)

ext_keep = [
    "month_id",
    "external_demand_context_score_0_100",
    "external_signal_confidence_score_0_100",
    "external_context_regime",
    "external_growth_support_flag",
]
for c in [quality_col, regime_col, air_flag_col]:
    if c and c not in ext_keep:
        ext_keep.append(c)

ext = ext[ext_keep].dropna(subset=["month_id"]).drop_duplicates(["month_id"])

# -----------------------------
# Roster integrity monthly
# -----------------------------
ri = ri.copy()
ri_outlet = first_existing(ri, ["outlet_id", "outlet_name"], required=True)
ri_month = first_existing(ri, ["month_id", "period_start", "period_month", "month"], required=True)
ri["outlet_key"] = ri[ri_outlet].astype(str)
ri["month_id"] = to_month_id(ri[ri_month])

ri["roster_integrity_health_score_0_100"] = normalize_0_100(
    ri[first_existing(
        ri,
        [
            "roster_operational_health_score_0_100",
            "schedule_integrity_score_0_100",
            "avg_schedule_stability_score_0_100",
            "roster_integrity_score_0_100",
        ],
        required=True
    )]
)

ri = (
    ri.groupby(["outlet_key", "month_id"], as_index=False)
      .agg({"roster_integrity_health_score_0_100": "mean"})
)

# -----------------------------
# Capacity day -> outlet month
# -----------------------------
cap = cap_day.copy()
cap_outlet = first_existing(cap, ["outlet_id", "outlet_name"], required=True)
cap_date = first_existing(cap, ["period_date", "date", "service_date"], required=True)
cap["outlet_key"] = cap[cap_outlet].astype(str)
cap["month_id"] = to_month_id(cap[cap_date])

util_col = first_existing(cap, ["utilization_on_roster_ratio",
    "productive_utilization_ratio", "utilization_ratio", "capacity_utilization_ratio"], required=False)
strain_col = first_existing(cap, ["capacity_strain_score_0_100", "coverage_pressure_score_0_100"], required=False)
idle_col = first_existing(cap, ["idle_hour_ratio", "idle_ratio"], required=False)
gap_col = first_existing(cap, ["coverage_gap_day_ratio", "coverage_gap_ratio"], required=False)
ot_col = first_existing(cap, ["overtime_hour_ratio", "overtime_ratio"], required=False)

if util_col:
    cap["productive_utilization_score_0_100"] = normalize_0_100(cap[util_col])
else:
    cap["productive_utilization_score_0_100"] = np.nan

if strain_col:
    cap["coverage_pressure_score_0_100"] = normalize_0_100(cap[strain_col])
else:
    cap["coverage_pressure_score_0_100"] = np.nan

idle_score = normalize_0_100(cap[idle_col]) if idle_col else pd.Series(np.nan, index=cap.index)
gap_score = normalize_0_100(cap[gap_col]) if gap_col else pd.Series(np.nan, index=cap.index)
ot_score = normalize_0_100(cap[ot_col]) if ot_col else pd.Series(np.nan, index=cap.index)

tmp = cap[["outlet_key", "month_id", "productive_utilization_score_0_100", "coverage_pressure_score_0_100"]].copy()
tmp["idle_score"] = idle_score
tmp["gap_score"] = gap_score
tmp["ot_score"] = ot_score

cap_m = (
    tmp.groupby(["outlet_key", "month_id"], as_index=False)
       .mean(numeric_only=True)
)

cap_m["capacity_stability_score_0_100"] = weighted_mean_df(cap_m, [
    ("coverage_pressure_score_0_100", 0.50, True),
    ("idle_score", 0.20, True),
    ("gap_score", 0.15, True),
    ("ot_score", 0.15, True),
])

cap_m = cap_m[
    [
        "outlet_key",
        "month_id",
        "productive_utilization_score_0_100",
        "coverage_pressure_score_0_100",
        "capacity_stability_score_0_100",
    ]
]

# -----------------------------
# Burnout monthly
# -----------------------------
burn = burn.copy()
burn_outlet = first_existing(burn, ["outlet_id", "outlet_name"], required=True)
burn_month = first_existing(burn, ["month_id", "period_start", "period_month", "month"], required=True)
burn["outlet_key"] = burn[burn_outlet].astype(str)
burn["month_id"] = to_month_id(burn[burn_month])

burn_score_col = first_existing(
    burn,
    ["avg_burnout_risk_score_0_100", "burnout_risk_score_0_100", "burnout_exposure_score_0_100"],
    required=True
)
burn["burnout_exposure_score_0_100"] = normalize_0_100(burn[burn_score_col])

burn = (
    burn.groupby(["outlet_key", "month_id"], as_index=False)
        .agg({"burnout_exposure_score_0_100": "mean"})
)

# -----------------------------
# Optional management signal monthly
# -----------------------------
if mgmt is not None:
    mgmt = mgmt.copy()
    mgmt_outlet = first_existing(mgmt, ["outlet_id", "outlet_name"], required=True)
    mgmt_month = first_existing(mgmt, ["month_id", "period_start", "month"], required=True)
    mgmt["outlet_key"] = mgmt[mgmt_outlet].astype(str)
    mgmt["month_id"] = to_month_id(mgmt[mgmt_month])

    mgmt_score_col = first_existing(
        mgmt,
        [
            "overall_management_signal_score_0_100",
            "management_signal_score_0_100",
            "outlet_control_score_0_100",
            "control_health_score_0_100",
        ],
        required=False
    )
    if mgmt_score_col:
        mgmt["management_signal_score_0_100"] = normalize_0_100(mgmt[mgmt_score_col])
        mgmt = (
            mgmt.groupby(["outlet_key", "month_id"], as_index=False)
                .agg({"management_signal_score_0_100": "mean"})
        )
    else:
        mgmt = None

# -----------------------------
# Merge: one row per outlet-month
# -----------------------------
base = ri.merge(cap_m, on=["outlet_key", "month_id"], how="outer")
base = base.merge(burn, on=["outlet_key", "month_id"], how="outer")
base = base.merge(ext, on="month_id", how="left")

if mgmt is not None:
    base = base.merge(mgmt, on=["outlet_key", "month_id"], how="left")
else:
    base["management_signal_score_0_100"] = np.nan

# enforce unique outlet-month
base = base.sort_values(["outlet_key", "month_id"]).drop_duplicates(["outlet_key", "month_id"], keep="first")

# -----------------------------
# Composite metrics
# -----------------------------
base["staffing_readiness_score_0_100"] = weighted_mean_df(base, [
    ("roster_integrity_health_score_0_100", 0.35, False),
    ("burnout_exposure_score_0_100", 0.30, True),
    ("capacity_stability_score_0_100", 0.25, False),
    ("management_signal_score_0_100", 0.10, False),
]).round(2)

base["internal_pressure_context_score_0_100"] = weighted_mean_df(base, [
    ("coverage_pressure_score_0_100", 0.45, False),
    ("productive_utilization_score_0_100", 0.25, False),
    ("burnout_exposure_score_0_100", 0.30, False),
]).round(2)

base["demand_support_vs_staffing_readiness_gap"] = (
    base["external_demand_context_score_0_100"] - base["staffing_readiness_score_0_100"]
).round(2)

base["external_internal_alignment_score_0_100"] = (
    100 - (base["external_demand_context_score_0_100"] - base["internal_pressure_context_score_0_100"]).abs()
).clip(0, 100).round(2)

base["capacity_expansion_readiness_score_0_100"] = weighted_mean_df(base, [
    ("external_demand_context_score_0_100", 0.30, False),
    ("staffing_readiness_score_0_100", 0.40, False),
    ("burnout_exposure_score_0_100", 0.15, True),
    ("coverage_pressure_score_0_100", 0.15, True),
]).round(2)

# -----------------------------
# Decision classifications
# -----------------------------
def classify_window(row):
    ext = row["external_demand_context_score_0_100"]
    ready = row["staffing_readiness_score_0_100"]
    press = row["coverage_pressure_score_0_100"]
    burn = row["burnout_exposure_score_0_100"]
    expand = row["capacity_expansion_readiness_score_0_100"]

    if pd.notna(burn) and burn >= 65:
        return "protect_team_capacity"
    if pd.notna(press) and press >= 60:
        return "control_window"
    if pd.notna(ext) and pd.notna(ready) and ext >= 55 and ready < 60:
        return "supported_but_not_roster_ready"
    if pd.notna(expand) and expand >= 60 and (pd.isna(burn) or burn < 65):
        return "growth_window"
    if pd.notna(ready) and ready >= 60:
        return "hold_and_optimize"
    return "control_window"

def classify_action(row):
    window = row["growth_window_vs_control_window"]
    gap = row["demand_support_vs_staffing_readiness_gap"]
    align = row["external_internal_alignment_score_0_100"]

    if window == "growth_window":
        return "expand_selectively"
    if window == "supported_but_not_roster_ready":
        return "stabilize_roster_before_expansion"
    if window == "protect_team_capacity":
        return "protect_capacity_hold_expansion"
    if window == "hold_and_optimize":
        return "maintain_capacity_optimize_controls"
    if pd.notna(align) and align < 45:
        return "investigate_signal_misalignment"
    if pd.notna(gap) and gap < -15:
        return "avoid_overstaffing_recalibrate_capacity"
    return "control_and_monitor"

base["growth_window_vs_control_window"] = base.apply(classify_window, axis=1)
base["roster_action_posture"] = base.apply(classify_action, axis=1)

# -----------------------------
# Output
# -----------------------------
preferred_cols = [
    "outlet_key",
    "month_id",
    "external_demand_context_score_0_100",
    "external_signal_confidence_score_0_100",
    "external_context_regime",
    "external_growth_support_flag",
]
for c in ["footfall_proxy_signal_quality", "footfall_proxy_weight_regime", "footfall_proxy_air_component_present_flag"]:
    if c in base.columns:
        preferred_cols.append(c)

preferred_cols += [
    "roster_integrity_health_score_0_100",
    "productive_utilization_score_0_100",
    "coverage_pressure_score_0_100",
    "capacity_stability_score_0_100",
    "burnout_exposure_score_0_100",
    "management_signal_score_0_100",
    "staffing_readiness_score_0_100",
    "internal_pressure_context_score_0_100",
    "demand_support_vs_staffing_readiness_gap",
    "external_internal_alignment_score_0_100",
    "capacity_expansion_readiness_score_0_100",
    "growth_window_vs_control_window",
    "roster_action_posture",
]

preferred_cols = [c for c in preferred_cols if c in base.columns]
final = base[preferred_cols].sort_values(["outlet_key", "month_id"]).reset_index(drop=True)

final.to_csv(OUT_FP, index=False)

print(f"[OK] saved: {OUT_FP}")
print(f"[OK] rows: {len(final)}")

print("\n=== WINDOW COUNTS ===")
print(final["growth_window_vs_control_window"].value_counts(dropna=False).to_string())

print("\n=== ACTION POSTURE COUNTS ===")
print(final["roster_action_posture"].value_counts(dropna=False).to_string())

print("\n=== DESCRIBE KEY METRICS ===")
desc_cols = [c for c in [
    "external_demand_context_score_0_100",
    "roster_integrity_health_score_0_100",
    "productive_utilization_score_0_100",
    "coverage_pressure_score_0_100",
    "capacity_stability_score_0_100",
    "burnout_exposure_score_0_100",
    "management_signal_score_0_100",
    "staffing_readiness_score_0_100",
    "internal_pressure_context_score_0_100",
    "external_internal_alignment_score_0_100",
    "capacity_expansion_readiness_score_0_100",
] if c in final.columns]
print(final[desc_cols].describe().round(2).to_string())

print("\n=== SAMPLE ===")
sample_cols = [c for c in [
    "outlet_key",
    "month_id",
    "external_demand_context_score_0_100",
    "roster_integrity_health_score_0_100",
    "coverage_pressure_score_0_100",
    "burnout_exposure_score_0_100",
    "staffing_readiness_score_0_100",
    "capacity_expansion_readiness_score_0_100",
    "growth_window_vs_control_window",
    "roster_action_posture",
] if c in final.columns]
print(final[sample_cols].head(20).to_string(index=False))
