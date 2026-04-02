from __future__ import annotations

from pathlib import Path
import pandas as pd
import numpy as np


BASE = Path(__file__).resolve().parents[2]
DP = BASE / "data_processed"
MGMT = DP / "management"


def safe_numeric(df: pd.DataFrame, col: str, default: float = 0.0) -> pd.DataFrame:
    df = df.copy()
    if col not in df.columns:
        df[col] = default
    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(default)
    return df


def ensure_text(df: pd.DataFrame, col: str, default: str = "") -> pd.DataFrame:
    df = df.copy()
    if col not in df.columns:
        df[col] = default
    df[col] = df[col].fillna(default).astype(str)
    return df


def main() -> None:
    outlet_fp = MGMT / "outlet_management_summary.csv"
    if not outlet_fp.exists():
        raise FileNotFoundError(f"Missing required file: {outlet_fp}")

    df = pd.read_csv(outlet_fp)

    df = ensure_text(df, "month_id")
    df = ensure_text(df, "outlet_id")
    df = ensure_text(df, "outlet_name")
    df = ensure_text(df, "management_signal", "stable_controlled_growth")
    df = ensure_text(df, "recommended_manager_action", "maintain balanced commercial and operational control")
    df = ensure_text(df, "managerial_story", "")
    df = ensure_text(df, "commercial_story", "")

    for c, d in {
        "overall_management_signal_score_0_100": 50.0,
        "avg_roster_operational_health_score_0_100": 70.0,
        "avg_capacity_strain_score_0_100": 0.0,
        "avg_burnout_exposure_day_ratio": 0.0,
        "avg_coverage_gap_day_ratio": 0.0,
        "avg_idle_hour_ratio": 0.0,
        "avg_staff_retail_selling_score_0_100": 0.0,
        "avg_therapist_upsell_score_0_100": 0.0,
        "avg_therapist_total_commercial_score_0_100": 0.0,
        "retail_reward_eligible_staff_count": 0.0,
        "therapist_bonus_reward_eligible_count": 0.0,
        "therapist_refresh_training_required_count": 0.0,
        "therapist_top_group_count": 0.0,
        "therapist_bottom_group_count": 0.0,
        "commercial_reward_attention_flag": 0.0,
        "refresh_training_attention_flag": 0.0,
        "total_staff_retail_revenue_eur": 0.0,
        "total_therapist_treatment_upsell_revenue_eur": 0.0,
    }.items():
        df = safe_numeric(df, c, d)

    df["people_readiness_score_0_100"] = (
        0.35 * df["avg_roster_operational_health_score_0_100"].clip(0, 100)
        + 0.25 * (100 - df["avg_capacity_strain_score_0_100"].clip(0, 100))
        + 0.20 * (1 - df["avg_burnout_exposure_day_ratio"].clip(0, 1.0)) * 100
        + 0.20 * (1 - df["avg_coverage_gap_day_ratio"].clip(0, 1.0)) * 100
    ).clip(0, 100)

    df["commercial_execution_score_0_100"] = (
        0.30 * df["avg_staff_retail_selling_score_0_100"].clip(0, 100)
        + 0.30 * df["avg_therapist_upsell_score_0_100"].clip(0, 100)
        + 0.20 * df["avg_therapist_total_commercial_score_0_100"].clip(0, 100)
        + 0.20 * df["overall_management_signal_score_0_100"].clip(0, 100)
    ).clip(0, 100)

    df["training_pressure_score_0_100"] = (
        0.40 * np.minimum(df["therapist_refresh_training_required_count"] * 20, 100)
        + 0.20 * df["avg_capacity_strain_score_0_100"].clip(0, 100)
        + 0.20 * df["avg_burnout_exposure_day_ratio"].clip(0, 1.0) * 100
        + 0.20 * df["refresh_training_attention_flag"] * 100
    ).clip(0, 100)

    df["reward_readiness_score_0_100"] = (
        0.35 * np.minimum(df["retail_reward_eligible_staff_count"] * 15, 100)
        + 0.35 * np.minimum(df["therapist_bonus_reward_eligible_count"] * 20, 100)
        + 0.15 * df["avg_staff_retail_selling_score_0_100"].clip(0, 100)
        + 0.15 * df["avg_therapist_upsell_score_0_100"].clip(0, 100)
    ).clip(0, 100)

    df["executive_priority_score_0_100"] = (
        0.30 * (100 - df["people_readiness_score_0_100"])
        + 0.20 * (100 - df["commercial_execution_score_0_100"])
        + 0.35 * df["training_pressure_score_0_100"]
        + 0.15 * (100 - df["overall_management_signal_score_0_100"].clip(0, 100))
    ).clip(0, 100)

    df["executive_priority_band"] = np.select(
        [
            df["executive_priority_score_0_100"] >= 75,
            df["executive_priority_score_0_100"] >= 55,
            df["executive_priority_score_0_100"] >= 35,
        ],
        [
            "critical",
            "high",
            "watchlist",
        ],
        default="stable",
    )

    df["executive_management_recommendation"] = np.select(
        [
            df["executive_priority_band"].eq("critical"),
            df["executive_priority_band"].eq("high"),
            df["executive_priority_band"].eq("watchlist"),
            df["reward_readiness_score_0_100"] >= 60,
        ],
        [
            "Immediate management intervention required across staffing control, training refresh, and execution discipline.",
            "Prioritize management follow-through on training pressure, commercial leakage, and people-readiness gaps.",
            "Monitor outlet closely and run selective coaching, reward calibration, and operating-control review.",
            "Outlet shows usable reward-recognition potential; protect and replicate strong commercial behavior.",
        ],
        default="Maintain current operating rhythm and monitor month-over-month movement.",
    )

    df["executive_rank_within_month"] = (
        df.groupby("month_id")["executive_priority_score_0_100"]
        .rank(method="first", ascending=False)
        .astype(int)
    )

    out = df[
        [
            "month_id",
            "period_start",
            "period_end",
            "outlet_id",
            "outlet_name",
            "management_signal",
            "recommended_manager_action",
            "managerial_story",
            "commercial_story",
            "people_readiness_score_0_100",
            "commercial_execution_score_0_100",
            "training_pressure_score_0_100",
            "reward_readiness_score_0_100",
            "executive_priority_score_0_100",
            "executive_priority_band",
            "executive_rank_within_month",
            "retail_reward_eligible_staff_count",
            "therapist_bonus_reward_eligible_count",
            "therapist_refresh_training_required_count",
            "therapist_top_group_count",
            "therapist_bottom_group_count",
            "executive_management_recommendation",
        ]
    ].copy()

    out = out.sort_values(["month_id", "executive_rank_within_month"]).reset_index(drop=True)
    out.insert(0, "executive_ranking_id", [f"EXR_{i:05d}" for i in range(1, len(out) + 1)])

    out_fp = MGMT / "outlet_executive_ranking_layer.csv"
    out.to_csv(out_fp, index=False)

    print(f"[OK] saved: {out_fp} | rows={len(out)}")
    print("\nPreview:")
    print(out.head(12).to_string(index=False))


if __name__ == "__main__":
    main()
