from __future__ import annotations
import pandas as pd
import numpy as np
from pathlib import Path

BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")
INTERNAL = BASE / "data_processed" / "internal_proxy"

CONFLICT_FILE = INTERNAL / "conflict_resolution_layer.csv"
CAPACITY_FILE = INTERNAL / "internal_proxy_roster_capacity_day.csv"
ROSTER_FILE = INTERNAL / "internal_proxy_roster_integrity_monthly.csv"
BURNOUT_FILE = INTERNAL / "internal_proxy_burnout_risk_monthly.csv"

def safe_read(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    return pd.read_csv(path)

def find_col(df: pd.DataFrame, candidates: list[str], required: bool = True):
    for c in candidates:
        if c in df.columns:
            return c
    if required:
        raise KeyError(f"Missing required column. Tried: {candidates}")
    return None

def main():
    conflict = safe_read(CONFLICT_FILE)
    capacity = safe_read(CAPACITY_FILE)
    roster = safe_read(ROSTER_FILE)
    burnout = safe_read(BURNOUT_FILE)

    c_period = find_col(conflict, ["period_month", "period_start", "period_date"])
    c_outlet = find_col(conflict, ["outlet_id"])

    conflict[c_period] = pd.to_datetime(conflict[c_period]).dt.to_period("M").dt.to_timestamp()
    capacity["period_month"] = pd.to_datetime(capacity["period_date"]).dt.to_period("M").dt.to_timestamp()
    roster["period_month"] = pd.to_datetime(roster["period_month"]).dt.to_period("M").dt.to_timestamp()
    burnout["period_month"] = pd.to_datetime(burnout["period_month"]).dt.to_period("M").dt.to_timestamp()

    cap_month = capacity.groupby(["period_month", "outlet_id"], as_index=False).agg(
        avg_capacity_strain_score_0_100=("capacity_strain_score_0_100", "mean"),
        staffing_pressure_days=("staffing_pressure_flag", "sum"),
        under_covered_days=("coverage_status", lambda s: int((s == "under_covered").sum())),
        tight_covered_days=("coverage_status", lambda s: int((s == "tight_covered").sum())),
    )

    roster_month = roster.groupby(["period_month", "outlet_id"], as_index=False).agg(
        avg_roster_integrity_score_0_100=("roster_integrity_score_0_100", "mean"),
        watchlist_or_lower_count=("roster_integrity_band", lambda s: int((s.isin(["watchlist","fragile","critical"])).sum())),
        coverage_gap_days_total=("coverage_gap_days", "sum"),
    )

    burnout_month = burnout.groupby(["period_month", "outlet_id"], as_index=False).agg(
        avg_burnout_risk_score_0_100=("burnout_risk_score_0_100", "mean"),
        elevated_or_higher_count=("burnout_risk_band", lambda s: int((s.isin(["elevated","high","critical"])).sum())),
    )

    df = conflict.rename(columns={c_period: "period_month", c_outlet: "outlet_id"}).copy()
    df = df.merge(cap_month, on=["period_month", "outlet_id"], how="left")
    df = df.merge(roster_month, on=["period_month", "outlet_id"], how="left")
    df = df.merge(burnout_month, on=["period_month", "outlet_id"], how="left")

    for c in [
        "avg_capacity_strain_score_0_100","staffing_pressure_days","under_covered_days",
        "tight_covered_days","avg_roster_integrity_score_0_100","watchlist_or_lower_count",
        "coverage_gap_days_total","avg_burnout_risk_score_0_100","elevated_or_higher_count"
    ]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    df["roster_conflict_type"] = np.select(
        [
            (df["under_covered_days"] >= 1) & (df["avg_burnout_risk_score_0_100"] >= 45),
            (df["staffing_pressure_days"] >= 1) & (df["avg_capacity_strain_score_0_100"] >= 45),
            (df["watchlist_or_lower_count"] >= 1) & (df["avg_roster_integrity_score_0_100"] < 70),
            (df["coverage_gap_days_total"] >= 2),
        ],
        [
            "high_demand_understaffed",
            "roster_strain",
            "staffing_mismatch",
            "coverage_gap_pressure",
        ],
        default="stable_staffing_context"
    )

    df["roster_conflict_severity"] = np.select(
        [
            (df["avg_burnout_risk_score_0_100"] >= 55) | (df["avg_capacity_strain_score_0_100"] >= 60),
            (df["avg_burnout_risk_score_0_100"] >= 42) | (df["avg_capacity_strain_score_0_100"] >= 45),
        ],
        ["high", "medium"],
        default="low"
    )

    df["roster_conflict_note"] = np.select(
        [
            df["roster_conflict_type"].eq("high_demand_understaffed"),
            df["roster_conflict_type"].eq("roster_strain"),
            df["roster_conflict_type"].eq("staffing_mismatch"),
            df["roster_conflict_type"].eq("coverage_gap_pressure"),
        ],
        [
            "Demand is being carried with insufficient staffing headroom; revenue risk may convert into service strain.",
            "Commercial performance is meeting meaningful roster strain and should be manager-governed closely.",
            "Staffing pattern is not well matched to actual service demand and sustainability needs.",
            "Coverage gaps are accumulating and may weaken consistency if not addressed.",
        ],
        default="No material roster-based conflict override detected."
    )

    df = df.sort_values(["period_month", "outlet_id"]).reset_index(drop=True)
    df.to_csv(CONFLICT_FILE, index=False)

    print(f"[OK] patched: {CONFLICT_FILE}")
    print(df.head(10).to_string(index=False))

if __name__ == "__main__":
    main()
