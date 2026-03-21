from __future__ import annotations
import numpy as np
import pandas as pd
from pathlib import Path

BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")
INTERNAL = BASE / "data_processed" / "internal_proxy"
OUTFILE = INTERNAL / "internal_proxy_burnout_risk_monthly.csv"

def safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required file: {path}")
    return pd.read_csv(path)

def main():
    df = safe_read_csv(INTERNAL / "internal_proxy_roster_integrity_monthly.csv")

    num_cols = [
        "overtime_hours","workload_density_ratio","max_consecutive_workdays",
        "day_off_regularity_ratio","schedule_stability_score_0_100",
        "burnout_exposure_days","roster_integrity_score_0_100"
    ]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    df["burnout_risk_score_0_100"] = (
        (df["overtime_hours"].clip(upper=18) / 18.0 * 24)
        + (df["workload_density_ratio"].clip(upper=1.02) / 1.02 * 24)
        + (df["max_consecutive_workdays"].clip(upper=8) / 8.0 * 16)
        + ((1 - df["day_off_regularity_ratio"].clip(upper=1.0)) * 10)
        + ((1 - (df["schedule_stability_score_0_100"] / 100.0).clip(lower=0, upper=1)) * 12)
        + ((df["burnout_exposure_days"].clip(upper=3) / 3.0) * 14)
    ).clip(lower=0, upper=100)

    df["burnout_risk_band"] = pd.cut(
        df["burnout_risk_score_0_100"],
        bins=[-np.inf, 25, 45, 65, 80, np.inf],
        labels=["low","guarded","elevated","high","critical"]
    ).astype(str)

    df["burnout_primary_driver"] = np.select(
        [
            df["overtime_hours"] >= 10,
            df["workload_density_ratio"] >= 0.95,
            df["max_consecutive_workdays"] >= 7,
            df["day_off_regularity_ratio"] < 0.70,
            df["schedule_stability_score_0_100"] < 68,
        ],
        [
            "overtime_accumulation",
            "dense_booking_load",
            "consecutive_workdays",
            "dayoff_instability",
            "schedule_instability",
        ],
        default="mixed_moderate_pressure"
    )

    df["managerial_action_hint"] = np.select(
        [
            df["burnout_risk_score_0_100"] >= 80,
            df["burnout_risk_score_0_100"] >= 65,
            df["burnout_risk_score_0_100"] >= 45,
        ],
        [
            "Immediate roster reset, reduce overload concentration, and protect recovery windows.",
            "Rebalance appointment density, cap overtime, and stabilize therapist scheduling.",
            "Monitor therapist load concentration and preserve day-off discipline before escalation.",
        ],
        default="Routine monitoring sufficient; current workload is broadly sustainable."
    )

    keep = [
        "period_month",
        "outlet_id",
        "therapist_id",
        "roster_integrity_score_0_100",
        "roster_integrity_band",
        "burnout_risk_score_0_100",
        "burnout_risk_band",
        "burnout_primary_driver",
        "managerial_action_hint",
        "overtime_hours",
        "workload_density_ratio",
        "max_consecutive_workdays",
        "day_off_regularity_ratio",
        "schedule_stability_score_0_100",
        "burnout_exposure_days",
    ]
    out = df[keep].sort_values(["period_month", "outlet_id", "therapist_id"]).reset_index(drop=True)
    out.to_csv(OUTFILE, index=False)

    print(f"[OK] saved: {OUTFILE}")
    print(f"[OK] rows: {len(out)}")
    print(out.head(10).to_string(index=False))

if __name__ == "__main__":
    main()
