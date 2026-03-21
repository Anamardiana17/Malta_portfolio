from __future__ import annotations
import pandas as pd
import numpy as np
from pathlib import Path

BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")
INTERNAL = BASE / "data_processed" / "internal_proxy"

SIGNAL_FILE = INTERNAL / "management_kpi_signal_layer.csv"
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
    signal = safe_read(SIGNAL_FILE)
    capacity = safe_read(CAPACITY_FILE)
    roster = safe_read(ROSTER_FILE)
    burnout = safe_read(BURNOUT_FILE)

    s_period = find_col(signal, ["period_month", "period_start", "period_date"])
    s_outlet = find_col(signal, ["outlet_id"])

    signal[s_period] = pd.to_datetime(signal[s_period]).dt.to_period("M").dt.to_timestamp()
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
    )

    burnout_month = burnout.groupby(["period_month", "outlet_id"], as_index=False).agg(
        avg_burnout_risk_score_0_100=("burnout_risk_score_0_100", "mean"),
        elevated_or_higher_count=("burnout_risk_band", lambda s: int((s.isin(["elevated","high","critical"])).sum())),
    )

    df = signal.rename(columns={s_period: "period_month", s_outlet: "outlet_id"}).copy()
    df = df.merge(cap_month, on=["period_month", "outlet_id"], how="left")
    df = df.merge(roster_month, on=["period_month", "outlet_id"], how="left")
    df = df.merge(burnout_month, on=["period_month", "outlet_id"], how="left")

    for c in [
        "avg_capacity_strain_score_0_100","staffing_pressure_days","under_covered_days","tight_covered_days",
        "avg_roster_integrity_score_0_100","watchlist_or_lower_count",
        "avg_burnout_risk_score_0_100","elevated_or_higher_count"
    ]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    df["operating_sustainability_signal"] = np.select(
        [
            (df["avg_burnout_risk_score_0_100"] >= 50) | (df["avg_roster_integrity_score_0_100"] < 60),
            (df["avg_capacity_strain_score_0_100"] >= 45) | (df["staffing_pressure_days"] >= 1),
            (df["avg_burnout_risk_score_0_100"] >= 40) | (df["watchlist_or_lower_count"] >= 1),
        ],
        [
            "growth_at_burnout_risk",
            "commercially_strong_but_operationally_tight",
            "watch_sustainability",
        ],
        default="sustainable_operating_context"
    )

    df["sustainability_reality_flag"] = np.where(
        df["operating_sustainability_signal"].ne("sustainable_operating_context"), 1, 0
    )

    df["managerial_interpretation_roster"] = np.select(
        [
            df["operating_sustainability_signal"].eq("growth_at_burnout_risk"),
            df["operating_sustainability_signal"].eq("commercially_strong_but_operationally_tight"),
            df["operating_sustainability_signal"].eq("watch_sustainability"),
        ],
        [
            "Outlet performance should not be interpreted as fully healthy because team sustainability is under pressure.",
            "Commercial delivery is holding, but staffing design is tight and needs manager attention.",
            "Operational sustainability should be watched before a performance issue becomes a people issue.",
        ],
        default="Current performance appears broadly compatible with sustainable roster conditions."
    )

    df = df.sort_values(["period_month", "outlet_id"]).reset_index(drop=True)
    df.to_csv(SIGNAL_FILE, index=False)

    print(f"[OK] patched: {SIGNAL_FILE}")
    print(df.head(10).to_string(index=False))

if __name__ == "__main__":
    main()
