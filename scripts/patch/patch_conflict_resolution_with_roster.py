from __future__ import annotations

from pathlib import Path
import pandas as pd
import numpy as np

BASE = Path(__file__).resolve().parents[2]

TARGET_FP = BASE / "data_processed" / "management" / "conflict_resolution_layer.csv"
ROSTER_OUTLET_FP = BASE / "data_processed" / "internal_proxy" / "internal_proxy_roster_outlet_monthly_bridge.csv"


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
    s = df[col]
    if isinstance(s, pd.DataFrame):
        s = s.iloc[:, 0]
    df[col] = pd.to_numeric(s, errors="coerce").fillna(default)
    return df


def main():
    if not TARGET_FP.exists():
        raise FileNotFoundError(f"Missing file: {TARGET_FP}")
    if not ROSTER_OUTLET_FP.exists():
        raise FileNotFoundError(f"Missing file: {ROSTER_OUTLET_FP}")

    df = pd.read_csv(TARGET_FP)
    roster = pd.read_csv(ROSTER_OUTLET_FP)

    df = dedupe_columns(df)
    roster = dedupe_columns(roster)

    df = ensure_key(df, "month_id", ["period_month", "month"])
    df = ensure_key(df, "outlet_id", ["spa_outlet_id", "branch_id", "location_id"])

    roster = ensure_key(roster, "month_id", ["period_month", "month"])
    roster = ensure_key(roster, "outlet_id", ["spa_outlet_id", "branch_id", "location_id"])

    df["month_id"] = df["month_id"].astype(str)
    df["outlet_id"] = df["outlet_id"].astype(str)
    roster["month_id"] = roster["month_id"].astype(str)
    roster["outlet_id"] = roster["outlet_id"].astype(str)

    roster_keep = [
        c for c in [
            "month_id",
            "outlet_id",
            "capacity_strain_score_0_100",
            "roster_operational_health_score_0_100",
            "productive_utilization_ratio",
            "coverage_gap_day_ratio",
            "overtime_hour_ratio",
            "burnout_exposure_day_ratio",
            "roster_management_signal",
        ] if c in roster.columns
    ]

    roster_patch = roster[roster_keep].drop_duplicates(["month_id", "outlet_id"])

    df = df.merge(
        roster_patch,
        on=["month_id", "outlet_id"],
        how="left",
        suffixes=("", "_roster"),
    )

    if "avg_capacity_strain_score_0_100" not in df.columns:
        if "capacity_strain_score_0_100" in df.columns:
            df["avg_capacity_strain_score_0_100"] = df["capacity_strain_score_0_100"]
        else:
            df["avg_capacity_strain_score_0_100"] = 0.0

    for c, d in {
        "avg_capacity_strain_score_0_100": 0.0,
        "capacity_strain_score_0_100": 0.0,
        "roster_operational_health_score_0_100": 70.0,
        "productive_utilization_ratio": 0.0,
        "coverage_gap_day_ratio": 0.0,
        "overtime_hour_ratio": 0.0,
        "burnout_exposure_day_ratio": 0.0,
    }.items():
        df = safe_numeric(df, c, d)

    if "roster_management_signal" not in df.columns:
        df["roster_management_signal"] = "stable_controlled"
    df["roster_management_signal"] = df["roster_management_signal"].fillna("stable_controlled").astype(str)

    base_col = None
    for c in ["conflict_resolution_score_0_100", "conflict_score_0_100", "conflict_score"]:
        if c in df.columns:
            base_col = c
            break
    if base_col is None:
        df["conflict_resolution_score_0_100"] = 50.0
        base_col = "conflict_resolution_score_0_100"

    df = safe_numeric(df, base_col, 50.0)

    df["conflict_resolution_score_0_100"] = (
        0.60 * df[base_col]
        + 0.20 * (1 - df["avg_capacity_strain_score_0_100"].clip(0, 100) / 100.0) * 100
        + 0.10 * (1 - df["coverage_gap_day_ratio"].clip(0, 1.0)) * 100
        + 0.10 * (1 - df["burnout_exposure_day_ratio"].clip(0, 1.0)) * 100
    ).clip(0, 100)

    df["conflict_resolution_priority"] = np.select(
        [
            (df["avg_capacity_strain_score_0_100"] >= 70) | (df["burnout_exposure_day_ratio"] >= 0.20),
            (df["avg_capacity_strain_score_0_100"] >= 50) | (df["coverage_gap_day_ratio"] >= 0.10),
        ],
        [
            "urgent",
            "watchlist",
        ],
        default="controlled",
    )

    df["conflict_story"] = np.select(
        [
            df["avg_capacity_strain_score_0_100"] >= 70,
            df["coverage_gap_day_ratio"] >= 0.10,
            df["burnout_exposure_day_ratio"] >= 0.15,
        ],
        [
            "capacity strain is likely escalating service friction and manager intervention needs",
            "coverage instability is increasing operational conflict risk between demand and roster execution",
            "burnout exposure is raising coaching and conflict containment requirements",
        ],
        default="conflict environment remains manageable under current operating rhythm",
    )

    df.to_csv(TARGET_FP, index=False)
    print(f"[OK] patched: {TARGET_FP}")
    print(f"[OK] rows: {len(df)}")


if __name__ == "__main__":
    main()
