from __future__ import annotations
import pandas as pd
import numpy as np
from pathlib import Path

BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")
INTERNAL = BASE / "data_processed" / "internal_proxy"

THERAPIST_FILE = INTERNAL / "therapist_consistency_score.csv"
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
    base = safe_read(THERAPIST_FILE)
    roster = safe_read(ROSTER_FILE)
    burnout = safe_read(BURNOUT_FILE)

    base_period = find_col(base, ["period_month", "period_start", "period_date", "month"])
    base_outlet = find_col(base, ["outlet_id"])
    base_therapist = find_col(base, ["therapist_id"])
    base_score = find_col(base, [
        "therapist_consistency_score_0_100",
        "consistency_score_0_100",
        "therapist_consistency_score"
    ])

    roster_period = find_col(roster, ["period_month"])
    roster_outlet = find_col(roster, ["outlet_id"])
    roster_therapist = find_col(roster, ["therapist_id"])

    burnout_period = find_col(burnout, ["period_month"])
    burnout_outlet = find_col(burnout, ["outlet_id"])
    burnout_therapist = find_col(burnout, ["therapist_id"])

    base[base_period] = pd.to_datetime(base[base_period]).dt.to_period("M").dt.to_timestamp()
    roster[roster_period] = pd.to_datetime(roster[roster_period]).dt.to_period("M").dt.to_timestamp()
    burnout[burnout_period] = pd.to_datetime(burnout[burnout_period]).dt.to_period("M").dt.to_timestamp()

    roster_small = roster[[
        roster_period, roster_outlet, roster_therapist,
        "roster_integrity_score_0_100", "roster_integrity_band",
        "overtime_hours", "coverage_gap_days", "burnout_exposure_days"
    ]].copy()

    burnout_small = burnout[[
        burnout_period, burnout_outlet, burnout_therapist,
        "burnout_risk_score_0_100", "burnout_risk_band",
        "burnout_primary_driver", "max_consecutive_workdays",
        "schedule_stability_score_0_100"
    ]].copy()

    roster_small.columns = [
        "period_month", "outlet_id", "therapist_id",
        "roster_integrity_score_0_100", "roster_integrity_band",
        "overtime_hours", "coverage_gap_days", "burnout_exposure_days"
    ]
    burnout_small.columns = [
        "period_month", "outlet_id", "therapist_id",
        "burnout_risk_score_0_100", "burnout_risk_band",
        "burnout_primary_driver", "max_consecutive_workdays",
        "schedule_stability_score_0_100"
    ]

    base = base.rename(columns={
        base_period: "period_month",
        base_outlet: "outlet_id",
        base_therapist: "therapist_id",
        base_score: "therapist_consistency_score_0_100_original"
    })

    df = base.merge(roster_small, on=["period_month", "outlet_id", "therapist_id"], how="left")
    df = df.merge(burnout_small, on=["period_month", "outlet_id", "therapist_id"], how="left")

    for c in [
        "therapist_consistency_score_0_100_original",
        "roster_integrity_score_0_100",
        "burnout_risk_score_0_100",
        "overtime_hours",
        "coverage_gap_days",
        "burnout_exposure_days",
        "max_consecutive_workdays",
        "schedule_stability_score_0_100",
    ]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    df["sustainability_penalty_points"] = (
        np.maximum(0, 75 - df["roster_integrity_score_0_100"]) * 0.18
        + np.maximum(0, df["burnout_risk_score_0_100"] - 35) * 0.20
        + np.minimum(8, df["overtime_hours"] * 0.45)
        + np.minimum(5, df["coverage_gap_days"] * 1.20)
        + np.minimum(5, df["burnout_exposure_days"] * 1.40)
        + np.maximum(0, df["max_consecutive_workdays"] - 6) * 2.20
    )

    df["therapist_consistency_score_0_100"] = (
        df["therapist_consistency_score_0_100_original"] - df["sustainability_penalty_points"]
    ).clip(lower=0, upper=100)

    df["sustainability_adjustment_flag"] = np.where(
        df["sustainability_penalty_points"] >= 5, 1, 0
    )

    df["therapist_consistency_note"] = np.select(
        [
            df["burnout_risk_score_0_100"] >= 65,
            df["roster_integrity_score_0_100"] < 55,
            df["coverage_gap_days"] >= 2,
            df["overtime_hours"] >= 10,
            df["sustainability_penalty_points"] >= 5,
        ],
        [
            "Consistency is being achieved under material burnout risk; score should be interpreted cautiously.",
            "Consistency is weakened by fragile roster integrity.",
            "Consistency is exposed to staffing coverage gaps.",
            "Consistency is being supported by overtime rather than clean roster design.",
            "Consistency remains commercially useful but is partially discounted by sustainability pressure.",
        ],
        default="Consistency remains credible under current roster conditions."
    )

    # preserve old score too
    out_cols = list(base.columns)
    extra_cols = [
        "roster_integrity_score_0_100", "roster_integrity_band",
        "burnout_risk_score_0_100", "burnout_risk_band",
        "burnout_primary_driver", "sustainability_penalty_points",
        "sustainability_adjustment_flag", "therapist_consistency_note"
    ]
    for c in extra_cols:
        if c not in df.columns:
            df[c] = np.nan

    desired = [c for c in out_cols if c != "therapist_consistency_score_0_100_original"] + [
        "therapist_consistency_score_0_100_original",
        "therapist_consistency_score_0_100",
        "roster_integrity_score_0_100", "roster_integrity_band",
        "burnout_risk_score_0_100", "burnout_risk_band",
        "burnout_primary_driver", "sustainability_penalty_points",
        "sustainability_adjustment_flag", "therapist_consistency_note"
    ]
    desired = [c for c in desired if c in df.columns]

    df = df[desired].sort_values(["period_month", "outlet_id", "therapist_id"]).reset_index(drop=True)
    df.to_csv(THERAPIST_FILE, index=False)

    print(f"[OK] patched: {THERAPIST_FILE}")
    print(df.head(10).to_string(index=False))

if __name__ == "__main__":
    main()
