from __future__ import annotations

from pathlib import Path
import pandas as pd
import numpy as np

BASE = Path(__file__).resolve().parents[2]
INTERNAL_PROXY = BASE / "data_processed" / "internal_proxy"


def read_csv(fp: Path) -> pd.DataFrame:
    if not fp.exists():
        raise FileNotFoundError(f"Missing file: {fp}")
    return pd.read_csv(fp)


def first_existing(df: pd.DataFrame, candidates: list[str], default=None):
    for c in candidates:
        if c in df.columns:
            return c
    return default


def ensure_month_key(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    direct_month_candidates = [
        "month_id",
        "period_month",
        "snapshot_month",
        "report_month",
        "month_key",
        "month_label",
    ]
    for c in direct_month_candidates:
        if c in df.columns:
            s = df[c].astype(str).str.strip()
            # already YYYY-MM
            if s.str.match(r"^\d{4}-\d{2}$", na=False).all():
                df["month_id"] = s
                return df
            # full date-like -> convert
            parsed = pd.to_datetime(s, errors="coerce")
            if parsed.notna().sum() > 0:
                df["month_id"] = parsed.dt.strftime("%Y-%m")
                return df

    date_candidates = [
        "period_date",
        "date",
        "work_date",
        "calendar_date",
        "period_start",
        "month_start",
        "start_date",
        "snapshot_date",
        "report_date",
    ]
    for c in date_candidates:
        if c in df.columns:
            parsed = pd.to_datetime(df[c], errors="coerce")
            if parsed.notna().sum() > 0:
                df["month_id"] = parsed.dt.strftime("%Y-%m")
                return df

    year_col = first_existing(df, ["year", "calendar_year", "report_year"])
    month_col = first_existing(df, ["month", "month_num", "calendar_month", "report_month_num"])
    if year_col is not None and month_col is not None:
        y = pd.to_numeric(df[year_col], errors="coerce")
        m = pd.to_numeric(df[month_col], errors="coerce")
        if y.notna().sum() > 0 and m.notna().sum() > 0:
            df["month_id"] = (
                y.astype("Int64").astype(str).str.zfill(4)
                + "-"
                + m.astype("Int64").astype(str).str.zfill(2)
            )
            return df

    raise ValueError(f"Cannot derive month_id. Available columns: {list(df.columns)}")


def ensure_outlet_key(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    outlet_col = first_existing(
        df,
        ["outlet_id", "spa_outlet_id", "branch_id", "location_id", "outlet_code", "site_id"]
    )
    if outlet_col is None:
        raise ValueError(f"Cannot derive outlet_id. Available columns: {list(df.columns)}")
    if outlet_col != "outlet_id":
        df["outlet_id"] = df[outlet_col]
    df["outlet_id"] = df["outlet_id"].astype(str)
    return df


def ensure_therapist_key(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    therapist_col = first_existing(df, ["therapist_id", "staff_id", "employee_id", "provider_id"])
    if therapist_col is not None and therapist_col != "therapist_id":
        df["therapist_id"] = df[therapist_col]
    if "therapist_id" in df.columns:
        df["therapist_id"] = df["therapist_id"].astype(str)
    return df


def add_missing_numeric(df: pd.DataFrame, cols: list[str], fill_value=0.0) -> pd.DataFrame:
    df = df.copy()
    for c in cols:
        if c not in df.columns:
            df[c] = fill_value
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(fill_value)
    return df


def add_missing_flag(df: pd.DataFrame, cols: list[str], fill_value=0) -> pd.DataFrame:
    df = df.copy()
    for c in cols:
        if c not in df.columns:
            df[c] = fill_value
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(fill_value)
    return df


def main():
    schedule_fp = INTERNAL_PROXY / "internal_proxy_roster_schedule.csv"
    capacity_fp = INTERNAL_PROXY / "internal_proxy_roster_capacity_day.csv"
    integrity_fp = INTERNAL_PROXY / "internal_proxy_roster_integrity_monthly.csv"
    burnout_fp = INTERNAL_PROXY / "internal_proxy_burnout_risk_monthly.csv"

    schedule = read_csv(schedule_fp)
    capacity = read_csv(capacity_fp)
    integrity = read_csv(integrity_fp)
    burnout = read_csv(burnout_fp)

    schedule = ensure_month_key(ensure_therapist_key(ensure_outlet_key(schedule)))
    capacity = ensure_month_key(ensure_therapist_key(ensure_outlet_key(capacity)))
    integrity = ensure_month_key(ensure_therapist_key(ensure_outlet_key(integrity)))
    burnout = ensure_month_key(ensure_therapist_key(ensure_outlet_key(burnout)))

    schedule = add_missing_numeric(
        schedule,
        [
            "scheduled_hours",
            "booked_hours",
            "productive_capacity_hours",
            "idle_hours",
            "break_hours",
            "recovery_gap_hours",
            "overtime_hours",
            "workload_density_ratio",
            "schedule_stability_score_0_100",
            "external_demand_proxy_index",
        ],
    )
    schedule = add_missing_flag(
        schedule,
        [
            "split_shift_flag",
            "coverage_gap_flag",
            "day_off_flag",
            "worked_flag",
            "burnout_exposure_flag",
        ],
    )

    capacity = add_missing_numeric(
        capacity,
        [
            "scheduled_hours",
            "booked_hours",
            "productive_capacity_hours",
            "idle_hours",
            "overtime_hours",
            "external_demand_proxy_index",
        ],
    )
    capacity = add_missing_flag(capacity, ["coverage_gap_flag"])

    integrity = add_missing_numeric(
        integrity,
        [
            "schedule_stability_score_0_100",
            "workload_density_ratio",
        ],
    )
    if "integrity_band" not in integrity.columns:
        if "integrity_status_band" in integrity.columns:
            integrity["integrity_band"] = integrity["integrity_status_band"]
        elif "integrity_health_band" in integrity.columns:
            integrity["integrity_band"] = integrity["integrity_health_band"]
        else:
            integrity["integrity_band"] = "healthy"

    burnout = add_missing_numeric(
        burnout,
        [
            "burnout_risk_score_0_100",
            "consecutive_workdays",
            "overtime_hours",
            "recovery_gap_hours",
            "workload_density_ratio",
        ],
    )
    burnout = add_missing_flag(burnout, ["burnout_exposure_flag"])
    if "burnout_band" not in burnout.columns:
        burnout["burnout_band"] = np.where(
            burnout["burnout_exposure_flag"].fillna(0).astype(float) > 0,
            "elevated",
            "guarded",
        )

    therapist_month = (
        schedule.groupby(["month_id", "outlet_id", "therapist_id"], as_index=False)
        .agg(
            scheduled_hours=("scheduled_hours", "sum"),
            booked_hours=("booked_hours", "sum"),
            productive_capacity_hours=("productive_capacity_hours", "sum"),
            idle_hours=("idle_hours", "sum"),
            break_hours=("break_hours", "sum"),
            recovery_gap_hours=("recovery_gap_hours", "sum"),
            overtime_hours=("overtime_hours", "sum"),
            split_shift_days=("split_shift_flag", "sum"),
            coverage_gap_days=("coverage_gap_flag", "sum"),
            day_off_days=("day_off_flag", "sum"),
            worked_days=("worked_flag", "sum"),
            avg_schedule_stability_score_0_100=("schedule_stability_score_0_100", "mean"),
            avg_workload_density_ratio=("workload_density_ratio", "mean"),
            burnout_exposure_days=("burnout_exposure_flag", "sum"),
            avg_external_demand_proxy_index=("external_demand_proxy_index", "mean"),
        )
    )

    therapist_month["productive_utilization_ratio"] = np.where(
        therapist_month["productive_capacity_hours"] > 0,
        therapist_month["booked_hours"] / therapist_month["productive_capacity_hours"],
        0.0,
    )
    therapist_month["idle_hour_ratio"] = np.where(
        therapist_month["scheduled_hours"] > 0,
        therapist_month["idle_hours"] / therapist_month["scheduled_hours"],
        0.0,
    )
    therapist_month["overtime_hour_ratio"] = np.where(
        therapist_month["scheduled_hours"] > 0,
        therapist_month["overtime_hours"] / therapist_month["scheduled_hours"],
        0.0,
    )
    therapist_month["coverage_gap_day_ratio"] = np.where(
        therapist_month["worked_days"] > 0,
        therapist_month["coverage_gap_days"] / therapist_month["worked_days"],
        0.0,
    )
    therapist_month["burnout_exposure_day_ratio"] = np.where(
        therapist_month["worked_days"] > 0,
        therapist_month["burnout_exposure_days"] / therapist_month["worked_days"],
        0.0,
    )

    integrity_keep = [
        c for c in [
            "month_id", "outlet_id", "therapist_id",
            "schedule_stability_score_0_100",
            "workload_density_ratio",
            "integrity_band",
        ] if c in integrity.columns
    ]
    burnout_keep = [
        c for c in [
            "month_id", "outlet_id", "therapist_id",
            "burnout_risk_score_0_100",
            "burnout_exposure_flag",
            "burnout_band",
            "consecutive_workdays",
        ] if c in burnout.columns
    ]

    if integrity_keep:
        therapist_month = therapist_month.merge(
            integrity[integrity_keep].drop_duplicates(["month_id", "outlet_id", "therapist_id"]),
            on=["month_id", "outlet_id", "therapist_id"],
            how="left",
            suffixes=("", "_integrity"),
        )

    if burnout_keep:
        therapist_month = therapist_month.merge(
            burnout[burnout_keep].drop_duplicates(["month_id", "outlet_id", "therapist_id"]),
            on=["month_id", "outlet_id", "therapist_id"],
            how="left",
            suffixes=("", "_burnout"),
        )

    if "integrity_band" not in therapist_month.columns:
        therapist_month["integrity_band"] = "healthy"
    therapist_month["integrity_band"] = therapist_month["integrity_band"].fillna("healthy")

    if "burnout_band" not in therapist_month.columns:
        therapist_month["burnout_band"] = "guarded"
    therapist_month["burnout_band"] = therapist_month["burnout_band"].fillna("guarded")

    if "burnout_exposure_flag" not in therapist_month.columns:
        therapist_month["burnout_exposure_flag"] = 0
    therapist_month["burnout_exposure_flag"] = pd.to_numeric(
        therapist_month["burnout_exposure_flag"], errors="coerce"
    ).fillna(0)

    if "burnout_risk_score_0_100" not in therapist_month.columns:
        therapist_month["burnout_risk_score_0_100"] = 20.0
    therapist_month["burnout_risk_score_0_100"] = pd.to_numeric(
        therapist_month["burnout_risk_score_0_100"], errors="coerce"
    ).fillna(20.0)

    outlet_month = (
        therapist_month.groupby(["month_id", "outlet_id"], as_index=False)
        .agg(
            therapist_count=("therapist_id", "nunique"),
            scheduled_hours=("scheduled_hours", "sum"),
            booked_hours=("booked_hours", "sum"),
            productive_capacity_hours=("productive_capacity_hours", "sum"),
            idle_hours=("idle_hours", "sum"),
            break_hours=("break_hours", "sum"),
            recovery_gap_hours=("recovery_gap_hours", "sum"),
            overtime_hours=("overtime_hours", "sum"),
            coverage_gap_days=("coverage_gap_days", "sum"),
            burnout_exposure_days=("burnout_exposure_days", "sum"),
            worked_days=("worked_days", "sum"),
            avg_schedule_stability_score_0_100=("avg_schedule_stability_score_0_100", "mean"),
            avg_workload_density_ratio=("avg_workload_density_ratio", "mean"),
            avg_external_demand_proxy_index=("avg_external_demand_proxy_index", "mean"),
            avg_burnout_risk_score_0_100=("burnout_risk_score_0_100", "mean"),
        )
    )

    outlet_month["productive_utilization_ratio"] = np.where(
        outlet_month["productive_capacity_hours"] > 0,
        outlet_month["booked_hours"] / outlet_month["productive_capacity_hours"],
        0.0,
    )
    outlet_month["idle_hour_ratio"] = np.where(
        outlet_month["scheduled_hours"] > 0,
        outlet_month["idle_hours"] / outlet_month["scheduled_hours"],
        0.0,
    )
    outlet_month["overtime_hour_ratio"] = np.where(
        outlet_month["scheduled_hours"] > 0,
        outlet_month["overtime_hours"] / outlet_month["scheduled_hours"],
        0.0,
    )
    outlet_month["coverage_gap_day_ratio"] = np.where(
        outlet_month["worked_days"] > 0,
        outlet_month["coverage_gap_days"] / outlet_month["worked_days"],
        0.0,
    )
    outlet_month["burnout_exposure_day_ratio"] = np.where(
        outlet_month["worked_days"] > 0,
        outlet_month["burnout_exposure_days"] / outlet_month["worked_days"],
        0.0,
    )

    outlet_month["capacity_strain_score_0_100"] = (
        35 * outlet_month["productive_utilization_ratio"].clip(0, 1.3) / 1.3
        + 25 * outlet_month["coverage_gap_day_ratio"].clip(0, 1.0)
        + 20 * outlet_month["overtime_hour_ratio"].clip(0, 1.0)
        + 20 * outlet_month["burnout_exposure_day_ratio"].clip(0, 1.0)
    ).clip(0, 100)

    outlet_month["roster_operational_health_score_0_100"] = (
        40 * (outlet_month["avg_schedule_stability_score_0_100"].fillna(70) / 100.0)
        + 25 * (1 - outlet_month["coverage_gap_day_ratio"].clip(0, 1.0))
        + 20 * (1 - outlet_month["overtime_hour_ratio"].clip(0, 1.0))
        + 15 * (1 - outlet_month["burnout_exposure_day_ratio"].clip(0, 1.0))
    ) * 100 / 100.0

    outlet_month["roster_management_signal"] = np.select(
        [
            outlet_month["capacity_strain_score_0_100"] >= 70,
            outlet_month["capacity_strain_score_0_100"] >= 50,
            outlet_month["roster_operational_health_score_0_100"] < 60,
        ],
        [
            "urgent_capacity_risk",
            "watch_capacity_risk",
            "operational_watchlist",
        ],
        default="stable_controlled",
    )

    therapist_out = INTERNAL_PROXY / "internal_proxy_roster_therapist_monthly_bridge.csv"
    outlet_out = INTERNAL_PROXY / "internal_proxy_roster_outlet_monthly_bridge.csv"

    therapist_month.to_csv(therapist_out, index=False)
    outlet_month.to_csv(outlet_out, index=False)

    print(f"[OK] saved: {therapist_out}")
    print(f"[OK] rows: {len(therapist_month)}")
    print(f"[OK] saved: {outlet_out}")
    print(f"[OK] rows: {len(outlet_month)}")


if __name__ == "__main__":
    main()
