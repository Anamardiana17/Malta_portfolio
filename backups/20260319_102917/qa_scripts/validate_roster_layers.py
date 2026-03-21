from __future__ import annotations
import pandas as pd
from pathlib import Path

BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")
INTERNAL = BASE / "data_processed" / "internal_proxy"

def must_exist(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"[FAIL] missing file: {path}")
    print(f"[OK] exists: {path.name}")

def check_columns(df: pd.DataFrame, required: list[str], name: str):
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise AssertionError(f"[FAIL] {name} missing columns: {missing}")
    print(f"[OK] {name} required columns present")

def check_nonempty(df: pd.DataFrame, name: str):
    if df.empty:
        raise AssertionError(f"[FAIL] {name} is empty")
    print(f"[OK] {name} rows={len(df)}")

def main():
    files = {
        "schedule": INTERNAL / "internal_proxy_roster_schedule.csv",
        "capacity_day": INTERNAL / "internal_proxy_roster_capacity_day.csv",
        "integrity_monthly": INTERNAL / "internal_proxy_roster_integrity_monthly.csv",
        "burnout_monthly": INTERNAL / "internal_proxy_burnout_risk_monthly.csv",
    }

    for p in files.values():
        must_exist(p)

    schedule = pd.read_csv(files["schedule"])
    capacity_day = pd.read_csv(files["capacity_day"])
    integrity = pd.read_csv(files["integrity_monthly"])
    burnout = pd.read_csv(files["burnout_monthly"])

    check_nonempty(schedule, "schedule")
    check_nonempty(capacity_day, "capacity_day")
    check_nonempty(integrity, "integrity_monthly")
    check_nonempty(burnout, "burnout_monthly")

    check_columns(schedule, [
        "period_date","outlet_id","therapist_id","scheduled_hours","booked_hours",
        "idle_hours","break_hours","recovery_gap_hours","overtime_hours",
        "split_shift_flag","coverage_gap_flag","schedule_stability_score_0_100",
        "workload_density_ratio","consecutive_workdays","burnout_exposure_flag"
    ], "schedule")

    check_columns(capacity_day, [
        "period_date","outlet_id","therapist_count","scheduled_hours","booked_hours",
        "productive_capacity_hours","capacity_coverage_ratio","capacity_strain_score_0_100",
        "coverage_status","staffing_pressure_flag"
    ], "capacity_day")

    check_columns(integrity, [
        "period_month","outlet_id","therapist_id","roster_integrity_score_0_100",
        "roster_integrity_band","roster_integrity_note"
    ], "integrity_monthly")

    check_columns(burnout, [
        "period_month","outlet_id","therapist_id","burnout_risk_score_0_100",
        "burnout_risk_band","burnout_primary_driver","managerial_action_hint"
    ], "burnout_monthly")

    # Numeric sanity
    for c in ["scheduled_hours","booked_hours","idle_hours","break_hours","overtime_hours"]:
        if (pd.to_numeric(schedule[c], errors="coerce").fillna(0) < 0).any():
            raise AssertionError(f"[FAIL] schedule has negative values in {c}")
    print("[OK] schedule numeric sanity passed")

    for c in ["schedule_stability_score_0_100"]:
        s = pd.to_numeric(schedule[c], errors="coerce").fillna(0)
        if ((s < 0) | (s > 100)).any():
            raise AssertionError(f"[FAIL] schedule score out of bounds: {c}")
    print("[OK] schedule score bounds passed")

    for c in ["capacity_strain_score_0_100"]:
        s = pd.to_numeric(capacity_day[c], errors="coerce").fillna(0)
        if ((s < 0) | (s > 100)).any():
            raise AssertionError(f"[FAIL] capacity_day score out of bounds: {c}")
    print("[OK] capacity day score bounds passed")

    for c in ["roster_integrity_score_0_100"]:
        s = pd.to_numeric(integrity[c], errors="coerce").fillna(0)
        if ((s < 0) | (s > 100)).any():
            raise AssertionError(f"[FAIL] integrity score out of bounds: {c}")
    print("[OK] integrity score bounds passed")

    for c in ["burnout_risk_score_0_100"]:
        s = pd.to_numeric(burnout[c], errors="coerce").fillna(0)
        if ((s < 0) | (s > 100)).any():
            raise AssertionError(f"[FAIL] burnout score out of bounds: {c}")
    print("[OK] burnout score bounds passed")

    print("\n[PASS] roster layer QA passed")

if __name__ == "__main__":
    main()
