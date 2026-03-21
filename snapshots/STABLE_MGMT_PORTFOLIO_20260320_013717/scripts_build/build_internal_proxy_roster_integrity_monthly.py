from __future__ import annotations
import numpy as np
import pandas as pd
from pathlib import Path

BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")
INTERNAL = BASE / "data_processed" / "internal_proxy"
OUTFILE = INTERNAL / "internal_proxy_roster_integrity_monthly.csv"

def safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required file: {path}")
    return pd.read_csv(path)

def main():
    roster = safe_read_csv(INTERNAL / "internal_proxy_roster_schedule.csv")
    roster["period_date"] = pd.to_datetime(roster["period_date"])
    roster["period_month"] = roster["period_date"].dt.to_period("M").dt.to_timestamp()

    numeric_cols = [
        "scheduled_hours","booked_hours","idle_hours","break_hours","recovery_gap_hours",
        "overtime_hours","split_shift_flag","coverage_gap_flag","worked_days","day_off_days",
        "schedule_stability_score_0_100","workload_density_ratio","consecutive_workdays",
        "burnout_exposure_flag"
    ]
    for c in numeric_cols:
        roster[c] = pd.to_numeric(roster[c], errors="coerce").fillna(0)

    grp = roster.groupby(["period_month", "outlet_id", "therapist_id"], as_index=False).agg(
        scheduled_hours=("scheduled_hours", "sum"),
        booked_hours=("booked_hours", "sum"),
        idle_hours=("idle_hours", "sum"),
        break_hours=("break_hours", "sum"),
        recovery_gap_hours=("recovery_gap_hours", "mean"),
        overtime_hours=("overtime_hours", "sum"),
        split_shift_days=("split_shift_flag", "sum"),
        coverage_gap_days=("coverage_gap_flag", "sum"),
        worked_days=("worked_days", "max"),
        day_off_days=("day_off_days", "max"),
        schedule_stability_score_0_100=("schedule_stability_score_0_100", "mean"),
        workload_density_ratio=("workload_density_ratio", "mean"),
        max_consecutive_workdays=("consecutive_workdays", "max"),
        burnout_exposure_days=("burnout_exposure_flag", "sum"),
    )

    grp["day_off_regularity_ratio"] = (grp["day_off_days"] / 8.0).clip(lower=0, upper=1)
    grp["recovery_adequacy_ratio"] = (grp["recovery_gap_hours"] / 0.75).clip(lower=0, upper=1.10)
    grp["overtime_pressure_ratio"] = (grp["overtime_hours"] / 12.0).clip(lower=0, upper=2.0)
    grp["coverage_gap_pressure_ratio"] = (grp["coverage_gap_days"] / 3.0).clip(lower=0, upper=1.5)
    grp["burnout_exposure_ratio"] = (grp["burnout_exposure_days"] / 3.0).clip(lower=0, upper=1.5)

    grp["roster_integrity_score_0_100"] = (
        grp["schedule_stability_score_0_100"] * 0.32
        + grp["day_off_regularity_ratio"] * 100 * 0.14
        + grp["recovery_adequacy_ratio"].clip(upper=1.0) * 100 * 0.12
        + (1 - grp["overtime_pressure_ratio"].clip(upper=1.0)) * 100 * 0.14
        + (1 - (grp["split_shift_days"] / 5.0).clip(upper=1.0)) * 100 * 0.08
        + (1 - grp["coverage_gap_pressure_ratio"].clip(upper=1.0)) * 100 * 0.10
        + (1 - grp["burnout_exposure_ratio"].clip(upper=1.0)) * 100 * 0.10
    ).clip(lower=18, upper=100)

    grp["roster_integrity_band"] = pd.cut(
        grp["roster_integrity_score_0_100"],
        bins=[-np.inf, 40, 55, 70, 85, np.inf],
        labels=["critical","fragile","watchlist","healthy","strong"]
    ).astype(str)

    grp["roster_integrity_note"] = np.select(
        [
            grp["roster_integrity_score_0_100"] < 40,
            grp["overtime_hours"] >= 12,
            grp["coverage_gap_days"] >= 2,
            grp["max_consecutive_workdays"] >= 7,
            grp["workload_density_ratio"] >= 0.94,
            grp["split_shift_days"] >= 2,
        ],
        [
            "Roster design is unstable and likely to weaken service consistency.",
            "Overtime accumulation is eroding recovery protection and needs intervention.",
            "Coverage gaps suggest staffing design is too tight for current demand pattern.",
            "Consecutive workday pattern is too aggressive for sustainable spa operations.",
            "Therapist load is being carried too close to capacity and may not be sustainable.",
            "Shift fragmentation is hurting roster cleanliness and team recovery quality.",
        ],
        default="Roster integrity is acceptable, but should still be governed against avoidable strain."
    )

    grp = grp.sort_values(["period_month", "outlet_id", "therapist_id"]).reset_index(drop=True)
    grp.to_csv(OUTFILE, index=False)

    print(f"[OK] saved: {OUTFILE}")
    print(f"[OK] rows: {len(grp)}")
    print(grp.head(10).to_string(index=False))

if __name__ == "__main__":
    main()
