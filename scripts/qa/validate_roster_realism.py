from __future__ import annotations
import pandas as pd
from pathlib import Path

BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")
INTERNAL = BASE / "data_processed" / "internal_proxy"

def assert_true(cond: bool, msg: str):
    if not cond:
        raise AssertionError(msg)
    print(f"[OK] {msg}")

def main():
    schedule = pd.read_csv(INTERNAL / "internal_proxy_roster_schedule.csv")
    capacity = pd.read_csv(INTERNAL / "internal_proxy_roster_capacity_day.csv")
    integrity = pd.read_csv(INTERNAL / "internal_proxy_roster_integrity_monthly.csv")
    burnout = pd.read_csv(INTERNAL / "internal_proxy_burnout_risk_monthly.csv")

    assert_true(schedule["workload_density_ratio"].nunique() > 50, "schedule workload density has meaningful variation")
    assert_true(schedule["schedule_stability_score_0_100"].nunique() > 50, "schedule stability score has meaningful variation")
    assert_true(schedule["idle_hours"].gt(0).mean() > 0.20, "idle hours appear in a meaningful share of records")
    assert_true(schedule["coverage_gap_flag"].mean() < 0.70, "coverage gap is not firing on most rows")
    assert_true(schedule["coverage_gap_flag"].mean() > 0.01, "coverage gap appears in at least a small strained subset")
    assert_true(schedule["overtime_hours"].gt(0).mean() > 0.03, "overtime appears in a meaningful minority of rows")
    assert_true(schedule["burnout_exposure_flag"].mean() > 0.02, "burnout exposure appears in strained cases")
    assert_true(schedule["burnout_exposure_flag"].mean() < 0.60, "burnout exposure is not universal")

    assert_true(capacity["coverage_status"].nunique() >= 3, "capacity day shows multiple coverage states")
    assert_true(capacity["capacity_strain_score_0_100"].nunique() > 30, "capacity strain score varies meaningfully")

    assert_true(integrity["roster_integrity_band"].nunique() >= 3, "roster integrity bands are not overly collapsed")
    assert_true(burnout["burnout_risk_band"].nunique() >= 3, "burnout bands are not overly collapsed")

    print("\n[PASS] roster realism QA passed")

if __name__ == "__main__":
    main()
