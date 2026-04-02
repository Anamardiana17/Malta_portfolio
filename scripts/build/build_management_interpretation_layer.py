from pathlib import Path
import pandas as pd
import numpy as np

BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")

INPUT_OUTLET = BASE / "data_processed/management/outlet_management_summary.csv"
INPUT_ROSTER = BASE / "data_processed/internal_proxy/internal_proxy_roster_outlet_monthly_bridge.csv"

OUT_DIR = BASE / "data_processed/management_interpretation"
OUT_MAIN = OUT_DIR / "management_interpretation_layer.csv"
OUT_STORY = OUT_DIR / "outlet_story_summary.csv"


def safe_numeric(df, col, default=np.nan):
    if col in df.columns:
        return pd.to_numeric(df[col], errors="coerce")
    return pd.Series(default, index=df.index, dtype="float64")


def clipped_score(series, fallback=50):
    s = pd.to_numeric(series, errors="coerce").fillna(fallback)
    return s.clip(lower=0, upper=100)


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    outlet = pd.read_csv(INPUT_OUTLET)
    roster = pd.read_csv(INPUT_ROSTER)

    # normalize month_id if needed
    if "month_id" not in roster.columns:
        raise ValueError("INPUT_ROSTER missing month_id")
    if "month_id" not in outlet.columns:
        raise ValueError("INPUT_OUTLET missing month_id")

    # aggregate roster to outlet-month level
    numeric_roster_cols = [
        c for c in roster.columns
        if c not in ["outlet_id", "month_id", "therapist_id", "period_date"]
    ]
    numeric_roster_cols = [
        c for c in numeric_roster_cols
        if pd.api.types.is_numeric_dtype(roster[c]) or roster[c].dtype == object
    ]

    roster_agg_dict = {}
    for c in numeric_roster_cols:
        try:
            roster[c] = pd.to_numeric(roster[c], errors="coerce")
            roster_agg_dict[c] = "mean"
        except Exception:
            pass

    roster_agg = roster.groupby(["outlet_id", "month_id"], as_index=False).agg(roster_agg_dict)

    df = outlet.merge(roster_agg, on=["outlet_id", "month_id"], how="left")

    overall = clipped_score(safe_numeric(df, "overall_management_signal_score_0_100", 50), fallback=50)

    # Temporary pillar logic
    # Will be refined after inspecting actual KPI columns in outlet_management_summary
    df["commercial_strength_score_0_100"] = overall.copy()
    df["operational_control_score_0_100"] = overall.copy()

    burnout = clipped_score(safe_numeric(df, "avg_burnout_risk_score_0_100", 50), fallback=50)
    stability = clipped_score(safe_numeric(df, "avg_schedule_stability_score_0_100", 50), fallback=50)
    idle_ratio = safe_numeric(df, "idle_hour_ratio", np.nan)
    overtime_ratio = safe_numeric(df, "overtime_hour_ratio", np.nan)
    coverage_gap_ratio = safe_numeric(df, "coverage_gap_day_ratio", np.nan)

    idle_penalty = (idle_ratio.fillna(0) * 100).clip(lower=0, upper=100)
    overtime_penalty = (overtime_ratio.fillna(0) * 100).clip(lower=0, upper=100)
    coverage_penalty = (coverage_gap_ratio.fillna(0) * 100).clip(lower=0, upper=100)

    df["team_health_score_0_100"] = (
        stability * 0.40
        + (100 - burnout) * 0.35
        + (100 - overtime_penalty) * 0.15
        + (100 - coverage_penalty) * 0.10
    ).clip(lower=0, upper=100)

    df["overall_management_signal_score_0_100"] = overall

    df["growth_ready_flag"] = (
        (df["overall_management_signal_score_0_100"] >= 70) &
        (df["commercial_strength_score_0_100"] >= 65) &
        (df["operational_control_score_0_100"] >= 60) &
        (df["team_health_score_0_100"] >= 60)
    ).astype(int)

    df["leakage_risk_flag"] = (
        (df["commercial_strength_score_0_100"] < 55) &
        (df["operational_control_score_0_100"] >= 45) &
        (df["team_health_score_0_100"] >= 45)
    ).astype(int)

    df["team_strain_risk_flag"] = (
        (df["team_health_score_0_100"] < 50) |
        (burnout >= 65) |
        (overtime_penalty >= 20) |
        (coverage_penalty >= 20)
    ).astype(int)

    def assign_story(row):
        if row["team_strain_risk_flag"] == 1:
            return "team-strain / burnout-risk"
        if row["leakage_risk_flag"] == 1:
            return "leakage-risk"
        if row["growth_ready_flag"] == 1:
            return "growth-ready"
        return "mixed / monitor"

    def action_map(story):
        if story == "team-strain / burnout-risk":
            return "Stabilize roster, reduce overtime strain, rebalance coverage, and protect service reliability before pushing growth."
        if story == "leakage-risk":
            return "Tighten conversion discipline, improve treatment mix, reduce monetization leakage, and strengthen price capture."
        if story == "growth-ready":
            return "Push selective pricing optimization, premium mix, upsell execution, and targeted commercial activation."
        return "Monitor the outlet for 2-3 months and validate direction before major intervention."

    def headline_map(story):
        if story == "team-strain / burnout-risk":
            return "Outlet requires team stabilization before further commercial pressure."
        if story == "leakage-risk":
            return "Outlet shows workable business base but is leaking commercial value."
        if story == "growth-ready":
            return "Outlet is positioned for selective commercial expansion."
        return "Outlet remains active but needs directional monitoring before firm repositioning."

    def issue_map(story):
        if story == "team-strain / burnout-risk":
            return "workforce strain"
        if story == "leakage-risk":
            return "commercial leakage"
        if story == "growth-ready":
            return "selective growth execution"
        return "mixed signal monitoring"

    def priority_map(story):
        if story == "team-strain / burnout-risk":
            return 1
        if story == "leakage-risk":
            return 2
        if story == "growth-ready":
            return 3
        return 4

    df["story_label"] = df.apply(assign_story, axis=1)
    df["recommended_manager_action"] = df["story_label"].map(action_map)
    df["portfolio_headline"] = df["story_label"].map(headline_map)
    df["primary_management_issue"] = df["story_label"].map(issue_map)
    df["management_priority_rank"] = df["story_label"].map(priority_map)

    df["executive_summary_1line"] = (
        df["outlet_id"].astype(str)
        + " | "
        + df["story_label"].astype(str)
        + " | "
        + df["portfolio_headline"].astype(str)
    )

    keep_cols = [
        "outlet_id",
        "month_id",
        "overall_management_signal_score_0_100",
        "commercial_strength_score_0_100",
        "operational_control_score_0_100",
        "team_health_score_0_100",
        "growth_ready_flag",
        "leakage_risk_flag",
        "team_strain_risk_flag",
        "story_label",
        "management_priority_rank",
        "primary_management_issue",
        "recommended_manager_action",
        "portfolio_headline",
        "executive_summary_1line",
    ]

    final_df = df[keep_cols].copy()
    final_df = final_df.sort_values(["outlet_id", "month_id"]).reset_index(drop=True)
    final_df.to_csv(OUT_MAIN, index=False)

    story_summary = (
        final_df.sort_values(["outlet_id", "month_id"])
        .groupby("outlet_id", as_index=False)
        .tail(1)
        .sort_values(["management_priority_rank", "outlet_id"])
        .reset_index(drop=True)
    )
    story_summary.to_csv(OUT_STORY, index=False)

    print(f"[OK] saved: {OUT_MAIN}")
    print(f"[OK] rows: {len(final_df)}")
    print(f"[OK] saved: {OUT_STORY}")
    print(f"[OK] outlet rows: {len(story_summary)}")

    print("\n=== story distribution ===")
    print(final_df["story_label"].value_counts(dropna=False).to_string())


if __name__ == "__main__":
    main()
