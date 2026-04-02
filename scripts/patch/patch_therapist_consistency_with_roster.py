from __future__ import annotations

from pathlib import Path
import pandas as pd
import numpy as np

BASE = Path(__file__).resolve().parents[2]

THERAPIST_FP = BASE / "data_processed" / "management" / "therapist_consistency_score.csv"
ROSTER_FP = BASE / "data_processed" / "internal_proxy" / "internal_proxy_roster_therapist_monthly_bridge.csv"


def dedupe_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if df.columns.duplicated().any():
        df = df.loc[:, ~df.columns.duplicated()]
    return df


def ensure_key(df: pd.DataFrame, target: str, candidates: list[str]) -> pd.DataFrame:
    df = df.copy()
    if target in df.columns:
        return df
    for c in candidates:
        if c in df.columns:
            df[target] = df[c]
            return df
    raise KeyError(f"Cannot derive required key: {target}")


def safe_numeric(df: pd.DataFrame, col: str, default: float = 0.0) -> pd.DataFrame:
    df = df.copy()
    if col not in df.columns:
        df[col] = default
    series = df[col]
    if isinstance(series, pd.DataFrame):
        series = series.iloc[:, 0]
    df[col] = pd.to_numeric(series, errors="coerce").fillna(default)
    return df


def main():
    if not THERAPIST_FP.exists():
        raise FileNotFoundError(f"Missing file: {THERAPIST_FP}")
    if not ROSTER_FP.exists():
        raise FileNotFoundError(f"Missing file: {ROSTER_FP}")

    df = pd.read_csv(THERAPIST_FP)
    roster = pd.read_csv(ROSTER_FP)

    df = dedupe_columns(df)
    roster = dedupe_columns(roster)

    df = ensure_key(df, "month_id", ["period_month", "month"])
    df = ensure_key(df, "outlet_id", ["spa_outlet_id", "branch_id", "location_id"])
    df = ensure_key(df, "therapist_id", ["staff_id", "employee_id"])

    roster = ensure_key(roster, "month_id", ["period_month", "month"])
    roster = ensure_key(roster, "outlet_id", ["spa_outlet_id", "branch_id", "location_id"])
    roster = ensure_key(roster, "therapist_id", ["staff_id", "employee_id"])

    for c in ["month_id", "outlet_id", "therapist_id"]:
        df[c] = df[c].astype(str)
        roster[c] = roster[c].astype(str)

    roster_keep = [
        c for c in [
            "month_id",
            "outlet_id",
            "therapist_id",
            "avg_schedule_stability_score_0_100",
            "avg_workload_density_ratio",
            "productive_utilization_ratio",
            "idle_hour_ratio",
            "overtime_hour_ratio",
            "coverage_gap_day_ratio",
            "burnout_exposure_day_ratio",
            "burnout_risk_score_0_100",
            "burnout_band",
            "integrity_band",
        ] if c in roster.columns
    ]

    roster_patch = roster[roster_keep].drop_duplicates(["month_id", "outlet_id", "therapist_id"])

    df = df.merge(
        roster_patch,
        on=["month_id", "outlet_id", "therapist_id"],
        how="left",
        suffixes=("", "_roster"),
    )

    numeric_defaults = {
        "avg_schedule_stability_score_0_100": 70.0,
        "avg_workload_density_ratio": 0.0,
        "productive_utilization_ratio": 0.0,
        "idle_hour_ratio": 0.0,
        "overtime_hour_ratio": 0.0,
        "coverage_gap_day_ratio": 0.0,
        "burnout_exposure_day_ratio": 0.0,
        "burnout_risk_score_0_100": 20.0,
    }

    for c, d in numeric_defaults.items():
        df = safe_numeric(df, c, d)

    if "burnout_band" not in df.columns:
        df["burnout_band"] = "guarded"
    df["burnout_band"] = df["burnout_band"].fillna("guarded").astype(str)

    if "integrity_band" not in df.columns:
        df["integrity_band"] = "healthy"
    df["integrity_band"] = df["integrity_band"].fillna("healthy").astype(str)

    base_col = None
    for c in ["therapist_consistency_score_0_100", "consistency_score_0_100", "consistency_score"]:
        if c in df.columns:
            base_col = c
            break

    if base_col is None:
        df["therapist_consistency_score_0_100"] = 50.0
        base_col = "therapist_consistency_score_0_100"

    df = safe_numeric(df, base_col, 50.0)

    df["therapist_consistency_score_0_100"] = (
        0.55 * df[base_col]
        + 0.20 * df["avg_schedule_stability_score_0_100"]
        + 0.10 * (1 - df["coverage_gap_day_ratio"].clip(0, 1)) * 100
        + 0.10 * (1 - df["overtime_hour_ratio"].clip(0, 1)) * 100
        + 0.05 * (1 - df["burnout_exposure_day_ratio"].clip(0, 1)) * 100
    ).clip(0, 100)

    df["coaching_priority_band"] = np.select(
        [
            (df["therapist_consistency_score_0_100"] < 45) | (df["burnout_risk_score_0_100"] >= 70),
            (df["therapist_consistency_score_0_100"] < 60) | (df["burnout_risk_score_0_100"] >= 50),
        ],
        [
            "urgent",
            "watchlist",
        ],
        default="stable",
    )

    df["managerial_story"] = np.select(
        [
            (df["burnout_risk_score_0_100"] >= 70) & (df["productive_utilization_ratio"] >= 0.75),
            (df["avg_schedule_stability_score_0_100"] < 60) & (df["coverage_gap_day_ratio"] >= 0.10),
            (df["idle_hour_ratio"] >= 0.25),
        ],
        [
            "high performer under strain; protect revenue without accelerating burnout",
            "unstable schedule pattern is weakening service consistency and outlet coverage",
            "underutilized capacity; review demand matching, shift design, and booking conversion",
        ],
        default="stable therapist operating pattern with manageable coaching needs",
    )

    df.to_csv(THERAPIST_FP, index=False)
    print(f"[OK] patched: {THERAPIST_FP}")
    print(f"[OK] rows: {len(df)}")


if __name__ == "__main__":
    main()
