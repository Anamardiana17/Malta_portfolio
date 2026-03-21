from __future__ import annotations

from pathlib import Path
import pandas as pd
import numpy as np

BASE = Path(__file__).resolve().parents[2]

TARGET_FP = BASE / "data_processed" / "insight_mart" / "management_kpi_signal_layer.csv"
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
            "idle_hour_ratio",
            "overtime_hour_ratio",
            "coverage_gap_day_ratio",
            "burnout_exposure_day_ratio",
            "roster_management_signal",
        ] if c in roster.columns
    ]

    df = df.merge(
        roster[roster_keep].drop_duplicates(["month_id", "outlet_id"]),
        on=["month_id", "outlet_id"],
        how="left",
        suffixes=("", "_roster"),
    )

    if "avg_capacity_strain_score_0_100" not in df.columns:
        if "capacity_strain_score_0_100" in df.columns:
            df["avg_capacity_strain_score_0_100"] = df["capacity_strain_score_0_100"]
        else:
            df["avg_capacity_strain_score_0_100"] = 0.0

    defaults = {
        "avg_capacity_strain_score_0_100": 0.0,
        "capacity_strain_score_0_100": 0.0,
        "roster_operational_health_score_0_100": 70.0,
        "productive_utilization_ratio": 0.0,
        "idle_hour_ratio": 0.0,
        "overtime_hour_ratio": 0.0,
        "coverage_gap_day_ratio": 0.0,
        "burnout_exposure_day_ratio": 0.0,
    }
    for c, d in defaults.items():
        df = safe_numeric(df, c, d)

    if "roster_management_signal" not in df.columns:
        df["roster_management_signal"] = "stable_controlled"
    df["roster_management_signal"] = df["roster_management_signal"].fillna("stable_controlled").astype(str)

    base_col = None
    for c in ["overall_management_signal_score_0_100", "management_signal_score_0_100"]:
        if c in df.columns:
            base_col = c
            break
    if base_col is None:
        df["overall_management_signal_score_0_100"] = 50.0
        base_col = "overall_management_signal_score_0_100"

    df = safe_numeric(df, base_col, 50.0)

    df["overall_management_signal_score_0_100"] = (
        0.60 * df[base_col]
        + 0.15 * (1 - df["avg_capacity_strain_score_0_100"].clip(0, 100) / 100.0) * 100
        + 0.10 * (1 - df["coverage_gap_day_ratio"].clip(0, 1.0)) * 100
        + 0.10 * (1 - df["burnout_exposure_day_ratio"].clip(0, 1.0)) * 100
        + 0.05 * (1 - df["idle_hour_ratio"].clip(0, 1.0)) * 100
    ).clip(0, 100)

    df["management_signal"] = np.select(
        [
            (df["avg_capacity_strain_score_0_100"] >= 70) & (df["productive_utilization_ratio"] >= 0.70),
            (df["idle_hour_ratio"] >= 0.25) & (df["productive_utilization_ratio"] < 0.55),
            (df["burnout_exposure_day_ratio"] >= 0.15),
            (df["coverage_gap_day_ratio"] >= 0.10),
        ],
        [
            "grow_carefully_team_under_strain",
            "demand_leakage_or_scheduling_inefficiency",
            "protect_team_stability",
            "coverage_control_required",
        ],
        default="stable_controlled_growth",
    )

    df["recommended_manager_action"] = np.select(
        [
            df["management_signal"].eq("grow_carefully_team_under_strain"),
            df["management_signal"].eq("demand_leakage_or_scheduling_inefficiency"),
            df["management_signal"].eq("protect_team_stability"),
            df["management_signal"].eq("coverage_control_required"),
        ],
        [
            "protect team capacity before pushing additional commercial growth",
            "tighten booking conversion, shift design, and demand-channel alignment",
            "rebalance workload, recovery gaps, and coaching cadence",
            "repair roster coverage before service inconsistency impacts revenue",
        ],
        default="maintain balanced commercial and operational control",
    )

    df.to_csv(TARGET_FP, index=False)
    print(f"[OK] patched: {TARGET_FP}")
    print(f"[OK] rows: {len(df)}")


if __name__ == "__main__":
    main()
