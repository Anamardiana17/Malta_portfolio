from __future__ import annotations
import pandas as pd
import numpy as np
from pathlib import Path

BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")
INTERNAL = BASE / "data_processed" / "internal_proxy"

TREATMENT_FILE = INTERNAL / "treatment_health_score.csv"
CAPACITY_FILE = INTERNAL / "internal_proxy_roster_capacity_day.csv"

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
    treatment = safe_read(TREATMENT_FILE)
    capacity = safe_read(CAPACITY_FILE)

    t_period = find_col(treatment, ["period_month", "period_start", "period_date"])
    t_outlet = find_col(treatment, ["outlet_id"])
    t_score = find_col(treatment, ["treatment_health_score_0_100", "health_score_0_100"])

    treatment[t_period] = pd.to_datetime(treatment[t_period]).dt.to_period("M").dt.to_timestamp()
    capacity["period_month"] = pd.to_datetime(capacity["period_date"]).dt.to_period("M").dt.to_timestamp()

    cap_month = capacity.groupby(["period_month", "outlet_id"], as_index=False).agg(
        avg_capacity_strain_score_0_100=("capacity_strain_score_0_100", "mean"),
        staffing_pressure_days=("staffing_pressure_flag", "sum"),
    )

    df = treatment.rename(columns={
        t_period: "period_month",
        t_outlet: "outlet_id",
        t_score: "treatment_health_score_0_100_original"
    }).copy()

    df = df.merge(cap_month, on=["period_month", "outlet_id"], how="left")
    df["avg_capacity_strain_score_0_100"] = pd.to_numeric(df["avg_capacity_strain_score_0_100"], errors="coerce").fillna(0)
    df["staffing_pressure_days"] = pd.to_numeric(df["staffing_pressure_days"], errors="coerce").fillna(0)
    df["treatment_health_score_0_100_original"] = pd.to_numeric(df["treatment_health_score_0_100_original"], errors="coerce").fillna(0)

    df["capacity_strain_penalty_points"] = (
        np.maximum(0, df["avg_capacity_strain_score_0_100"] - 45) * 0.12
        + np.minimum(4, df["staffing_pressure_days"] * 1.20)
    )

    df["treatment_health_score_0_100"] = (
        df["treatment_health_score_0_100_original"] - df["capacity_strain_penalty_points"]
    ).clip(lower=0, upper=100)

    df["treatment_health_capacity_note"] = np.where(
        df["capacity_strain_penalty_points"] >= 3,
        "Treatment health is partially discounted by outlet staffing strain.",
        "Treatment health remains broadly supportable by current outlet capacity conditions."
    )

    df = df.sort_values(["period_month", "outlet_id"]).reset_index(drop=True)
    df.to_csv(TREATMENT_FILE, index=False)

    print(f"[OK] patched: {TREATMENT_FILE}")
    print(df.head(10).to_string(index=False))

if __name__ == "__main__":
    main()
