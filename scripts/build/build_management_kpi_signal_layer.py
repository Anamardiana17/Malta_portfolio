from __future__ import annotations

from pathlib import Path
import pandas as pd
import numpy as np


BASE = Path(__file__).resolve().parents[2]
DP = BASE / "data_processed"
INTERNAL_PROXY = DP / "internal_proxy"
INSIGHT_MART = DP / "insight_mart"


def first_existing(df: pd.DataFrame, candidates: list[str], required: bool = False):
    for c in candidates:
        if c in df.columns:
            return c
    if required:
        raise KeyError(f"None of these columns exist: {candidates}")
    return None


def ensure_month_id(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if "month_id" in df.columns:
        df["month_id"] = df["month_id"].astype(str)
        return df

    direct = first_existing(df, ["period_month", "snapshot_month", "report_month", "month_key"])
    if direct:
        s = df[direct].astype(str).str.strip()
        parsed = pd.to_datetime(s, errors="coerce")
        if parsed.notna().sum() > 0:
            df["month_id"] = parsed.dt.strftime("%Y-%m")
        else:
            df["month_id"] = s.str[:7]
        return df

    date_col = first_existing(df, ["period_start", "month_start", "period_date", "date", "snapshot_date"])
    if date_col:
        dt = pd.to_datetime(df[date_col], errors="coerce")
        df["month_id"] = dt.dt.strftime("%Y-%m")
        return df

    year_col = first_existing(df, ["year", "calendar_year"])
    month_col = first_existing(df, ["month", "month_num", "calendar_month"])
    if year_col and month_col:
        y = pd.to_numeric(df[year_col], errors="coerce")
        m = pd.to_numeric(df[month_col], errors="coerce")
        df["month_id"] = y.astype("Int64").astype(str).str.zfill(4) + "-" + m.astype("Int64").astype(str).str.zfill(2)
        return df

    raise ValueError(f"Cannot derive month_id from columns: {list(df.columns)}")


def ensure_outlet_id(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    col = first_existing(df, ["outlet_id", "spa_outlet_id", "branch_id", "location_id"], required=True)
    if col != "outlet_id":
        df["outlet_id"] = df[col]
    df["outlet_id"] = df["outlet_id"].astype(str)
    return df


def safe_numeric(df: pd.DataFrame, col: str, default: float = 0.0) -> pd.DataFrame:
    df = df.copy()
    if col not in df.columns:
        df[col] = default
    s = df[col]
    if isinstance(s, pd.DataFrame):
        s = s.iloc[:, 0]
    df[col] = pd.to_numeric(s, errors="coerce").fillna(default)
    return df


def pick_file(candidates: list[Path]) -> Path:
    for p in candidates:
        if p.exists():
            return p
    raise FileNotFoundError("None of the candidate files exist:\n" + "\n".join(str(p) for p in candidates))


def main():
    therapist_fp = pick_file([
        INTERNAL_PROXY / "therapist_consistency_score.csv",
        INSIGHT_MART / "therapist_consistency_score.csv",
    ])
    treatment_fp = pick_file([
        INTERNAL_PROXY / "treatment_health_score.csv",
        INSIGHT_MART / "treatment_health_score.csv",
    ])
    roster_fp = pick_file([
        INTERNAL_PROXY / "internal_proxy_roster_outlet_monthly_bridge.csv",
    ])

    therapist = pd.read_csv(therapist_fp)
    treatment = pd.read_csv(treatment_fp)
    roster = pd.read_csv(roster_fp)

    therapist = ensure_outlet_id(ensure_month_id(therapist))
    treatment = ensure_outlet_id(ensure_month_id(treatment))
    roster = ensure_outlet_id(ensure_month_id(roster))

    # therapist monthly outlet rollup
    therapist = safe_numeric(therapist, "therapist_consistency_score_0_100", 50.0)
    therapist = safe_numeric(therapist, "utilization_percent", 0.0)
    therapist = safe_numeric(therapist, "yield_eur_per_sold_hour", 0.0)
    therapist = safe_numeric(therapist, "revpath_proxy_eur_per_available_hour", 0.0)
    therapist = safe_numeric(therapist, "attendance_reliability_percent", 0.0)
    therapist = safe_numeric(therapist, "schedule_adherence_percent", 0.0)

    t_group = (
        therapist.groupby(["month_id", "outlet_id"], as_index=False)
        .agg(
            therapist_count=("therapist_id", "nunique"),
            avg_therapist_consistency_score_0_100=("therapist_consistency_score_0_100", "mean"),
            avg_therapist_utilization_percent=("utilization_percent", "mean"),
            avg_therapist_yield_eur_per_sold_hour=("yield_eur_per_sold_hour", "mean"),
            avg_therapist_revpath_proxy_eur_per_available_hour=("revpath_proxy_eur_per_available_hour", "mean"),
            avg_attendance_reliability_percent=("attendance_reliability_percent", "mean"),
            avg_schedule_adherence_percent=("schedule_adherence_percent", "mean"),
        )
    )

    # treatment monthly outlet rollup
    treatment = safe_numeric(treatment, "treatment_health_score_0_100", 50.0)
    treatment = safe_numeric(treatment, "utilization_percent", 0.0)
    treatment = safe_numeric(treatment, "yield_eur_per_sold_hour", 0.0)
    treatment = safe_numeric(treatment, "revpath_eur_per_available_hour", 0.0)
    treatment = safe_numeric(treatment, "revenue_eur", 0.0)
    treatment = safe_numeric(treatment, "external_demand_proxy_index", 0.0)

    th_group = (
        treatment.groupby(["month_id", "outlet_id"], as_index=False)
        .agg(
            treatment_count=("treatment_key", "nunique"),
            avg_treatment_health_score_0_100=("treatment_health_score_0_100", "mean"),
            avg_treatment_utilization_percent=("utilization_percent", "mean"),
            avg_treatment_yield_eur_per_sold_hour=("yield_eur_per_sold_hour", "mean"),
            avg_treatment_revpath_eur_per_available_hour=("revpath_eur_per_available_hour", "mean"),
            total_revenue_eur=("revenue_eur", "sum"),
            avg_external_demand_proxy_index=("external_demand_proxy_index", "mean"),
        )
    )

    # roster monthly outlet rollup
    roster = safe_numeric(roster, "capacity_strain_score_0_100", 0.0)
    roster = safe_numeric(roster, "roster_operational_health_score_0_100", 70.0)
    roster = safe_numeric(roster, "productive_utilization_ratio", 0.0)
    roster = safe_numeric(roster, "idle_hour_ratio", 0.0)
    roster = safe_numeric(roster, "overtime_hour_ratio", 0.0)
    roster = safe_numeric(roster, "coverage_gap_day_ratio", 0.0)
    roster = safe_numeric(roster, "burnout_exposure_day_ratio", 0.0)
    roster = safe_numeric(roster, "avg_schedule_stability_score_0_100", 70.0)
    roster = safe_numeric(roster, "avg_burnout_risk_score_0_100", 20.0)

    if "roster_management_signal" not in roster.columns:
        roster["roster_management_signal"] = "stable_controlled"

    r_group = (
        roster.groupby(["month_id", "outlet_id"], as_index=False)
        .agg(
            avg_capacity_strain_score_0_100=("capacity_strain_score_0_100", "mean"),
            avg_roster_operational_health_score_0_100=("roster_operational_health_score_0_100", "mean"),
            avg_productive_utilization_ratio=("productive_utilization_ratio", "mean"),
            avg_idle_hour_ratio=("idle_hour_ratio", "mean"),
            avg_overtime_hour_ratio=("overtime_hour_ratio", "mean"),
            avg_coverage_gap_day_ratio=("coverage_gap_day_ratio", "mean"),
            avg_burnout_exposure_day_ratio=("burnout_exposure_day_ratio", "mean"),
            avg_schedule_stability_score_0_100=("avg_schedule_stability_score_0_100", "mean"),
            avg_burnout_risk_score_0_100=("avg_burnout_risk_score_0_100", "mean"),
            roster_management_signal=("roster_management_signal", "first"),
        )
    )

    out = t_group.merge(th_group, on=["month_id", "outlet_id"], how="outer")
    out = out.merge(r_group, on=["month_id", "outlet_id"], how="outer")

    # add period fields for downstream compatibility
    out["period_type"] = "monthly"
    out["period_start"] = pd.to_datetime(out["month_id"] + "-01", errors="coerce")
    out["period_end"] = out["period_start"] + pd.offsets.MonthEnd(0)
    out["rolling_window_weeks"] = 12
    out["market_context"] = "Malta"

    # outlet_name if available from best source
    outlet_name_map = None
    for src in [therapist, treatment]:
        if "outlet_name" in src.columns:
            tmp = src[["outlet_id", "outlet_name"]].dropna().drop_duplicates("outlet_id")
            outlet_name_map = tmp if outlet_name_map is None else pd.concat([outlet_name_map, tmp], ignore_index=True).drop_duplicates("outlet_id")
    if outlet_name_map is not None:
        out = out.merge(outlet_name_map, on="outlet_id", how="left")
    if "outlet_name" not in out.columns:
        out["outlet_name"] = out["outlet_id"]

    numeric_defaults = {
        "therapist_count": 0.0,
        "treatment_count": 0.0,
        "avg_therapist_consistency_score_0_100": 50.0,
        "avg_therapist_utilization_percent": 0.0,
        "avg_therapist_yield_eur_per_sold_hour": 0.0,
        "avg_therapist_revpath_proxy_eur_per_available_hour": 0.0,
        "avg_attendance_reliability_percent": 0.0,
        "avg_schedule_adherence_percent": 0.0,
        "avg_treatment_health_score_0_100": 50.0,
        "avg_treatment_utilization_percent": 0.0,
        "avg_treatment_yield_eur_per_sold_hour": 0.0,
        "avg_treatment_revpath_eur_per_available_hour": 0.0,
        "total_revenue_eur": 0.0,
        "avg_external_demand_proxy_index": 0.0,
        "avg_capacity_strain_score_0_100": 0.0,
        "avg_roster_operational_health_score_0_100": 70.0,
        "avg_productive_utilization_ratio": 0.0,
        "avg_idle_hour_ratio": 0.0,
        "avg_overtime_hour_ratio": 0.0,
        "avg_coverage_gap_day_ratio": 0.0,
        "avg_burnout_exposure_day_ratio": 0.0,
        "avg_schedule_stability_score_0_100": 70.0,
        "avg_burnout_risk_score_0_100": 20.0,
    }
    for c, d in numeric_defaults.items():
        out = safe_numeric(out, c, d)

    out["overall_management_signal_score_0_100"] = (
        0.20 * out["avg_therapist_consistency_score_0_100"]
        + 0.20 * out["avg_treatment_health_score_0_100"]
        + 0.15 * out["avg_therapist_utilization_percent"].clip(0, 100)
        + 0.15 * (out["avg_treatment_yield_eur_per_sold_hour"].clip(0, 120) / 120.0) * 100
        + 0.10 * (1 - out["avg_capacity_strain_score_0_100"].clip(0, 100) / 100.0) * 100
        + 0.10 * (1 - out["avg_burnout_exposure_day_ratio"].clip(0, 1.0)) * 100
        + 0.10 * (1 - out["avg_coverage_gap_day_ratio"].clip(0, 1.0)) * 100
    ).clip(0, 100)

    out["management_signal"] = np.select(
        [
            (out["avg_capacity_strain_score_0_100"] >= 70) & (out["avg_treatment_utilization_percent"] >= 65),
            (out["avg_idle_hour_ratio"] >= 0.25) & (out["avg_treatment_utilization_percent"] < 55),
            (out["avg_burnout_exposure_day_ratio"] >= 0.15),
            (out["avg_coverage_gap_day_ratio"] >= 0.10),
            (out["avg_treatment_health_score_0_100"] >= 75) & (out["avg_capacity_strain_score_0_100"] < 50),
        ],
        [
            "grow_carefully_team_under_strain",
            "demand_leakage_or_scheduling_inefficiency",
            "protect_team_stability",
            "coverage_control_required",
            "commercial_growth_ready",
        ],
        default="stable_controlled_growth",
    )

    out["recommended_manager_action"] = np.select(
        [
            out["management_signal"].eq("grow_carefully_team_under_strain"),
            out["management_signal"].eq("demand_leakage_or_scheduling_inefficiency"),
            out["management_signal"].eq("protect_team_stability"),
            out["management_signal"].eq("coverage_control_required"),
            out["management_signal"].eq("commercial_growth_ready"),
        ],
        [
            "protect team capacity before pushing additional commercial growth",
            "tighten booking conversion, shift design, and channel-demand matching",
            "rebalance workload, recovery gaps, and coaching cadence",
            "repair roster coverage before service inconsistency affects revenue",
            "increase commercial push where yield and service quality can scale safely",
        ],
        default="maintain balanced commercial and operational control",
    )

    out["revenue_growth_readiness_flag"] = np.where(
        (out["avg_treatment_health_score_0_100"] >= 70)
        & (out["avg_capacity_strain_score_0_100"] < 55)
        & (out["avg_burnout_exposure_day_ratio"] < 0.12),
        1,
        0,
    )

    out["leakage_control_flag"] = np.where(
        (out["avg_idle_hour_ratio"] >= 0.20) | (out["avg_coverage_gap_day_ratio"] >= 0.10),
        1,
        0,
    )

    out["qa_status"] = "pending_qa"
    out["audit_note"] = "management signal rebuilt with roster-aware month-key tolerant joins"
    out["status"] = "scaffold_generated"

    out = out.sort_values(["outlet_id", "period_start"]).reset_index(drop=True)
    out.insert(0, "management_signal_id", [f"MKS_{i:05d}" for i in range(1, len(out) + 1)])

    INTERNAL_PROXY.mkdir(parents=True, exist_ok=True)
    INSIGHT_MART.mkdir(parents=True, exist_ok=True)

    out_internal = INTERNAL_PROXY / "management_kpi_signal_layer.csv"
    out_mart = INSIGHT_MART / "management_kpi_signal_layer.csv"

    out.to_csv(out_internal, index=False)
    out.to_csv(out_mart, index=False)

    print(f"[OK] saved: {out_internal}")
    print(f"[OK] saved: {out_mart}")
    print(f"[OK] rows: {len(out)}")
    print("\nPreview:")
    print(out.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
