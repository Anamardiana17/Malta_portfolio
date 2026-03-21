from __future__ import annotations
import numpy as np
import pandas as pd
from pathlib import Path

BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")
INTERNAL = BASE / "data_processed" / "internal_proxy"
OUTFILE = INTERNAL / "internal_proxy_roster_capacity_day.csv"

def safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required file: {path}")
    return pd.read_csv(path)

def main():
    roster = safe_read_csv(INTERNAL / "internal_proxy_roster_schedule.csv")
    roster["period_date"] = pd.to_datetime(roster["period_date"])

    for c in [
        "scheduled_hours","booked_hours","productive_capacity_hours","idle_hours",
        "break_hours","overtime_hours","coverage_gap_flag","worked_flag",
        "split_shift_flag","burnout_exposure_flag","workload_density_ratio",
        "schedule_stability_score_0_100"
    ]:
        roster[c] = pd.to_numeric(roster[c], errors="coerce").fillna(0)

    grp = roster.groupby(["period_date", "outlet_id"], as_index=False).agg(
        therapist_count=("therapist_id", "nunique"),
        therapist_working_count=("worked_flag", "sum"),
        scheduled_hours=("scheduled_hours", "sum"),
        booked_hours=("booked_hours", "sum"),
        productive_capacity_hours=("productive_capacity_hours", "sum"),
        idle_hours=("idle_hours", "sum"),
        break_hours=("break_hours", "sum"),
        overtime_hours=("overtime_hours", "sum"),
        split_shift_count=("split_shift_flag", "sum"),
        coverage_gap_count=("coverage_gap_flag", "sum"),
        burnout_exposure_count=("burnout_exposure_flag", "sum"),
        avg_workload_density_ratio=("workload_density_ratio", "mean"),
        avg_schedule_stability_score_0_100=("schedule_stability_score_0_100", "mean"),
    )

    grp["utilization_on_roster_ratio"] = (
        grp["booked_hours"] / grp["productive_capacity_hours"].replace(0, np.nan)
    ).fillna(0).clip(lower=0, upper=1.25)

    grp["capacity_coverage_ratio"] = (
        grp["productive_capacity_hours"] / grp["booked_hours"].replace(0, np.nan)
    ).fillna(1.25).clip(lower=0, upper=1.80)

    grp["coverage_gap_threshold_count"] = np.maximum(1, np.ceil(grp["therapist_count"] * 0.50)).astype(int)

    grp["staffing_pressure_flag"] = np.where(
        (grp["utilization_on_roster_ratio"] >= 0.94) |
        (grp["overtime_hours"] >= 10) |
        (grp["coverage_gap_count"] >= grp["coverage_gap_threshold_count"]),
        1, 0
    )

    grp["capacity_strain_score_0_100"] = (
        grp["utilization_on_roster_ratio"] * 46
        + np.minimum(20, grp["overtime_hours"] * 0.9)
        + np.minimum(12, grp["split_shift_count"] * 2.5)
        + np.minimum(14, grp["burnout_exposure_count"] * 2.8)
        + np.maximum(0, 80 - grp["avg_schedule_stability_score_0_100"]) * 0.28
    ).clip(lower=5, upper=100)

    grp["coverage_status"] = np.select(
        [
            grp["capacity_coverage_ratio"] < 0.95,
            grp["capacity_coverage_ratio"] < 1.03,
            grp["capacity_coverage_ratio"] < 1.15,
        ],
        [
            "under_covered",
            "tight_covered",
            "balanced",
        ],
        default="over_buffered"
    )

    grp = grp.drop(columns=["coverage_gap_threshold_count"])
    grp = grp.sort_values(["period_date", "outlet_id"]).reset_index(drop=True)
    grp.to_csv(OUTFILE, index=False)

    print(f"[OK] saved: {OUTFILE}")
    print(f"[OK] rows: {len(grp)}")
    print(grp.head(10).to_string(index=False))

if __name__ == "__main__":
    main()
