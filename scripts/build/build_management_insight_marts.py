from __future__ import annotations

from pathlib import Path
import pandas as pd
import numpy as np

BASE = Path(__file__).resolve().parents[2]
DP = BASE / "data_processed"
INTERNAL_PROXY = DP / "internal_proxy"
INSIGHT_MART = DP / "management"


def pick_file(candidates: list[Path]) -> Path:
    for p in candidates:
        if p.exists():
            return p
    raise FileNotFoundError("None of the candidate files exist:\n" + "\n".join(str(p) for p in candidates))


def safe_numeric(df: pd.DataFrame, col: str, default: float = 0.0) -> pd.DataFrame:
    df = df.copy()
    if col not in df.columns:
        df[col] = default
    s = df[col]
    if isinstance(s, pd.DataFrame):
        s = s.iloc[:, 0]
    df[col] = pd.to_numeric(s, errors="coerce").fillna(default)
    return df


def ensure_col(df: pd.DataFrame, col: str, default):
    df = df.copy()
    if col not in df.columns:
        df[col] = default
    return df


def ensure_month_id(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "month_id" in df.columns:
        df["month_id"] = df["month_id"].astype(str)
        return df

    for c in ["period_start", "month_start", "period_date", "date", "snapshot_date"]:
        if c in df.columns:
            dt = pd.to_datetime(df[c], errors="coerce")
            if dt.notna().sum() > 0:
                df["month_id"] = dt.dt.strftime("%Y-%m")
                return df

    for c in ["period_month", "snapshot_month", "report_month", "month_key"]:
        if c in df.columns:
            s = df[c].astype(str).str.strip()
            dt = pd.to_datetime(s, errors="coerce")
            if dt.notna().sum() > 0:
                df["month_id"] = dt.dt.strftime("%Y-%m")
            else:
                df["month_id"] = s.str[:7]
            return df

    if "year" in df.columns and ("month" in df.columns or "month_num" in df.columns):
        month_col = "month" if "month" in df.columns else "month_num"
        y = pd.to_numeric(df["year"], errors="coerce")
        m = pd.to_numeric(df[month_col], errors="coerce")
        df["month_id"] = y.astype("Int64").astype(str).str.zfill(4) + "-" + m.astype("Int64").astype(str).str.zfill(2)
        return df

    raise KeyError(f"Cannot derive month_id from columns: {list(df.columns)}")


def ensure_outlet_id(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "outlet_id" in df.columns:
        df["outlet_id"] = df["outlet_id"].astype(str)
        return df
    for c in ["spa_outlet_id", "branch_id", "location_id"]:
        if c in df.columns:
            df["outlet_id"] = df[c].astype(str)
            return df
    raise KeyError(f"Cannot derive outlet_id from columns: {list(df.columns)}")


def signal_band_from_score(score: pd.Series) -> pd.Series:
    score = pd.to_numeric(score, errors="coerce").fillna(0)
    return np.select(
        [
            score >= 75,
            score >= 60,
            score >= 45,
        ],
        [
            "stable",
            "watchlist",
            "at_risk",
        ],
        default="critical",
    )


def main():
    INSIGHT_MART.mkdir(parents=True, exist_ok=True)

    mgmt_fp = pick_file([
        INSIGHT_MART / "management_kpi_signal_layer.csv",
        INTERNAL_PROXY / "management_kpi_signal_layer.csv",
    ])
    therapist_fp = pick_file([
        INTERNAL_PROXY / "therapist_consistency_score.csv",
        INSIGHT_MART / "therapist_consistency_score.csv",
    ])
    treatment_fp = pick_file([
        INTERNAL_PROXY / "treatment_health_score.csv",
        INSIGHT_MART / "treatment_health_score.csv",
    ])
    roster_outlet_fp = pick_file([
        INTERNAL_PROXY / "internal_proxy_roster_outlet_monthly_bridge.csv",
    ])
    roster_therapist_fp = pick_file([
        INTERNAL_PROXY / "internal_proxy_roster_therapist_monthly_bridge.csv",
    ])

    # New layers
    staff_commercial_fp = pick_file([
        INSIGHT_MART / "staff_commercial_scoring_layer.csv",
    ])
    therapist_perf_fp = pick_file([
        INSIGHT_MART / "therapist_top_bottom_performance_layer.csv",
    ])

    mgmt = pd.read_csv(mgmt_fp)
    therapist = pd.read_csv(therapist_fp)
    treatment = pd.read_csv(treatment_fp)
    roster_outlet = pd.read_csv(roster_outlet_fp)
    roster_therapist = pd.read_csv(roster_therapist_fp)
    staff_commercial = pd.read_csv(staff_commercial_fp)
    therapist_perf = pd.read_csv(therapist_perf_fp)

    mgmt = ensure_outlet_id(ensure_month_id(mgmt))
    therapist = ensure_outlet_id(ensure_month_id(therapist))
    treatment = ensure_outlet_id(ensure_month_id(treatment))
    roster_outlet = ensure_outlet_id(ensure_month_id(roster_outlet))
    roster_therapist = ensure_outlet_id(ensure_month_id(roster_therapist))
    staff_commercial = ensure_outlet_id(ensure_month_id(staff_commercial))
    therapist_perf = ensure_outlet_id(ensure_month_id(therapist_perf))

    # ------------------------------------------------------------
    # Commercial summaries by outlet-month
    # ------------------------------------------------------------
    staff_commercial = ensure_col(staff_commercial, "outlet_name", "")
    for c, d in {
        "retail_selling_score_0_100": 0.0,
        "retail_reward_eligibility_flag": 0.0,
        "retail_revenue_eur": 0.0,
        "retail_units_sold": 0.0,
    }.items():
        staff_commercial = safe_numeric(staff_commercial, c, d)

    staff_commercial_summary = (
        staff_commercial.groupby(["month_id", "outlet_id", "outlet_name"], as_index=False)
        .agg(
            total_staff_retail_revenue_eur=("retail_revenue_eur", "sum"),
            total_staff_retail_units_sold=("retail_units_sold", "sum"),
            avg_staff_retail_selling_score_0_100=("retail_selling_score_0_100", "mean"),
            retail_reward_eligible_staff_count=("retail_reward_eligibility_flag", "sum"),
        )
    )

    therapist_perf = ensure_col(therapist_perf, "outlet_name", "")
    for c, d in {
        "upsell_score_0_100": 0.0,
        "total_commercial_score_0_100": 0.0,
        "bonus_reward_eligibility_flag": 0.0,
        "refresh_training_required_flag": 0.0,
        "top3_therapist_flag": 0.0,
        "bottom3_therapist_flag": 0.0,
        "treatment_upsell_revenue_eur": 0.0,
        "retail_revenue_eur": 0.0,
    }.items():
        therapist_perf = safe_numeric(therapist_perf, c, d)

    therapist_perf_summary = (
        therapist_perf.groupby(["month_id", "outlet_id", "outlet_name"], as_index=False)
        .agg(
            avg_therapist_upsell_score_0_100=("upsell_score_0_100", "mean"),
            avg_therapist_total_commercial_score_0_100=("total_commercial_score_0_100", "mean"),
            therapist_bonus_reward_eligible_count=("bonus_reward_eligibility_flag", "sum"),
            therapist_refresh_training_required_count=("refresh_training_required_flag", "sum"),
            therapist_top_group_count=("top3_therapist_flag", "sum"),
            therapist_bottom_group_count=("bottom3_therapist_flag", "sum"),
            total_therapist_treatment_upsell_revenue_eur=("treatment_upsell_revenue_eur", "sum"),
            total_therapist_retail_revenue_eur=("retail_revenue_eur", "sum"),
        )
    )

    # outlet_management_summary
    outlet = mgmt.copy()

    for c, d in {
        "overall_management_signal_score_0_100": 50.0,
        "avg_capacity_strain_score_0_100": 0.0,
        "avg_treatment_utilization_percent": 0.0,
        "avg_treatment_yield_eur_per_sold_hour": 0.0,
        "avg_treatment_revpath_eur_per_available_hour": 0.0,
        "avg_burnout_exposure_day_ratio": 0.0,
        "avg_coverage_gap_day_ratio": 0.0,
        "avg_idle_hour_ratio": 0.0,
        "avg_roster_operational_health_score_0_100": 70.0,
        "total_revenue_eur": 0.0,
        "avg_external_demand_proxy_index": 0.0,
    }.items():
        outlet = safe_numeric(outlet, c, d)

    outlet = ensure_col(outlet, "management_signal", "stable_controlled_growth")
    outlet = ensure_col(outlet, "recommended_manager_action", "maintain balanced commercial and operational control")

    if "overall_management_signal_band" not in outlet.columns:
        outlet["overall_management_signal_band"] = signal_band_from_score(
            outlet["overall_management_signal_score_0_100"]
        )

    outlet["revenue_growth_without_team_burnout_flag"] = np.where(
        (outlet["overall_management_signal_score_0_100"] >= 70)
        & (outlet["avg_capacity_strain_score_0_100"] < 55)
        & (outlet["avg_burnout_exposure_day_ratio"] < 0.12),
        1,
        0,
    )

    outlet["leakage_control_priority_flag"] = np.where(
        (outlet["avg_idle_hour_ratio"] >= 0.20)
        | (outlet["avg_coverage_gap_day_ratio"] >= 0.10),
        1,
        0,
    )

    outlet["managerial_story"] = np.select(
        [
            outlet["management_signal"].eq("grow_carefully_team_under_strain"),
            outlet["management_signal"].eq("demand_leakage_or_scheduling_inefficiency"),
            outlet["management_signal"].eq("protect_team_stability"),
            outlet["management_signal"].eq("coverage_control_required"),
            outlet["management_signal"].eq("commercial_growth_ready"),
        ],
        [
            "revenue momentum exists, but team strain must be protected before pushing further growth",
            "commercial demand is leaking through scheduling inefficiency or weak booking conversion discipline",
            "team stability protection is needed to sustain service quality and avoid burnout-driven revenue loss",
            "coverage control is required before service inconsistency damages guest experience and repeat demand",
            "commercial growth can be pushed with controlled operational risk",
        ],
        default="balanced commercial and operational conditions with manageable managerial control needs",
    )

    outlet = outlet.merge(
        staff_commercial_summary,
        on=["month_id", "outlet_id", "outlet_name"],
        how="left",
    ).merge(
        therapist_perf_summary,
        on=["month_id", "outlet_id", "outlet_name"],
        how="left",
    )

    for c, d in {
        "total_staff_retail_revenue_eur": 0.0,
        "total_staff_retail_units_sold": 0.0,
        "avg_staff_retail_selling_score_0_100": 0.0,
        "retail_reward_eligible_staff_count": 0.0,
        "avg_therapist_upsell_score_0_100": 0.0,
        "avg_therapist_total_commercial_score_0_100": 0.0,
        "therapist_bonus_reward_eligible_count": 0.0,
        "therapist_refresh_training_required_count": 0.0,
        "therapist_top_group_count": 0.0,
        "therapist_bottom_group_count": 0.0,
        "total_therapist_treatment_upsell_revenue_eur": 0.0,
        "total_therapist_retail_revenue_eur": 0.0,
    }.items():
        outlet = safe_numeric(outlet, c, d)

    outlet["commercial_reward_attention_flag"] = np.where(
        (outlet["retail_reward_eligible_staff_count"] > 0)
        | (outlet["therapist_bonus_reward_eligible_count"] > 0),
        1,
        0,
    )

    outlet["refresh_training_attention_flag"] = np.where(
        outlet["therapist_refresh_training_required_count"] > 0,
        1,
        0,
    )

    outlet["commercial_story"] = np.select(
        [
            (outlet["retail_reward_eligible_staff_count"] > 0) & (outlet["therapist_bonus_reward_eligible_count"] > 0),
            (outlet["retail_reward_eligible_staff_count"] > 0),
            (outlet["therapist_bonus_reward_eligible_count"] > 0),
            (outlet["therapist_refresh_training_required_count"] > 0),
        ],
        [
            "Outlet shows both staff retail-selling reward potential and therapist upsell reward potential.",
            "Outlet shows staff retail-selling reward potential that can be recognized and replicated.",
            "Outlet shows therapist upsell reward potential with commercial coaching upside.",
            "Outlet shows limited reward readiness and therapist refresh-training need.",
        ],
        default="Outlet commercial reward and training signals remain moderate under current modeled thresholds.",
    )

    outlet_management_summary = outlet[
        [
            "management_signal_id",
            "month_id",
            "period_type",
            "period_start",
            "period_end",
            "rolling_window_weeks",
            "market_context",
            "outlet_id",
            "outlet_name",
            "therapist_count",
            "treatment_count",
            "total_revenue_eur",
            "avg_treatment_utilization_percent",
            "avg_treatment_yield_eur_per_sold_hour",
            "avg_treatment_revpath_eur_per_available_hour",
            "avg_external_demand_proxy_index",
            "avg_capacity_strain_score_0_100",
            "avg_roster_operational_health_score_0_100",
            "avg_burnout_exposure_day_ratio",
            "avg_coverage_gap_day_ratio",
            "avg_idle_hour_ratio",
            "overall_management_signal_score_0_100",
            "overall_management_signal_band",
            "management_signal",
            "recommended_manager_action",
            "revenue_growth_without_team_burnout_flag",
            "leakage_control_priority_flag",
            "total_staff_retail_revenue_eur",
            "total_staff_retail_units_sold",
            "avg_staff_retail_selling_score_0_100",
            "retail_reward_eligible_staff_count",
            "avg_therapist_upsell_score_0_100",
            "avg_therapist_total_commercial_score_0_100",
            "therapist_bonus_reward_eligible_count",
            "therapist_refresh_training_required_count",
            "therapist_top_group_count",
            "therapist_bottom_group_count",
            "total_therapist_treatment_upsell_revenue_eur",
            "total_therapist_retail_revenue_eur",
            "commercial_reward_attention_flag",
            "refresh_training_attention_flag",
            "managerial_story",
            "commercial_story",
            "qa_status",
            "audit_note",
            "status",
        ]
    ].copy()

    # treatment_opportunity_summary
    th = treatment.copy()

    for c, d in {
        "treatment_health_score_0_100": 50.0,
        "utilization_percent": 0.0,
        "yield_eur_per_sold_hour": 0.0,
        "revpath_eur_per_available_hour": 0.0,
        "revenue_eur": 0.0,
        "external_demand_proxy_index": 0.0,
    }.items():
        th = safe_numeric(th, c, d)

    join_cols = [
        "month_id",
        "outlet_id",
        "outlet_name",
        "overall_management_signal_score_0_100",
        "overall_management_signal_band",
        "management_signal",
        "recommended_manager_action",
        "avg_capacity_strain_score_0_100",
        "avg_burnout_exposure_day_ratio",
        "avg_coverage_gap_day_ratio",
        "avg_idle_hour_ratio",
        "avg_external_demand_proxy_index",
        "commercial_reward_attention_flag",
        "refresh_training_attention_flag",
    ]
    th = th.merge(
        outlet_management_summary[join_cols].drop_duplicates(["month_id", "outlet_id"]),
        on=["month_id", "outlet_id"],
        how="left",
        suffixes=("", "_outlet"),
    )

    th["expansion_readiness_flag"] = np.where(
        (th["treatment_health_score_0_100"] >= 72)
        & (th["avg_capacity_strain_score_0_100"] < 55)
        & (th["avg_burnout_exposure_day_ratio"] < 0.12),
        1,
        0,
    )

    th["utilization_pressure_flag"] = np.where(
        (th["utilization_percent"] >= 65)
        & (
            (th["avg_capacity_strain_score_0_100"] >= 60)
            | (th["avg_burnout_exposure_day_ratio"] >= 0.12)
        ),
        1,
        0,
    )

    th["yield_improvement_opportunity"] = np.where(
        (th["utilization_percent"] < 60)
        & (th["avg_capacity_strain_score_0_100"] < 50),
        "pricing_or_mix_optimization",
        "capacity_or_execution_first",
    )

    th["opportunity_managerial_story"] = np.select(
        [
            th["expansion_readiness_flag"].eq(1),
            th["utilization_pressure_flag"].eq(1),
            th["yield_improvement_opportunity"].eq("pricing_or_mix_optimization"),
        ],
        [
            "treatment line can support commercial push without materially raising team strain",
            "treatment line is selling into a tighter capacity environment and should be scaled carefully",
            "yield can be improved through pricing, package design, or sales mix without immediate capacity pressure",
        ],
        default="maintain and monitor treatment line performance within current operating rhythm",
    )

    treatment_opportunity_summary = th.copy()

    # therapist_coaching_summary
    tc = therapist.copy()

    tc["therapist_id"] = tc["therapist_id"].astype(str)
    tc["outlet_id"] = tc["outlet_id"].astype(str)
    tc["month_id"] = tc["month_id"].astype(str)

    roster_therapist["therapist_id"] = roster_therapist["therapist_id"].astype(str)
    roster_therapist["outlet_id"] = roster_therapist["outlet_id"].astype(str)
    roster_therapist["month_id"] = roster_therapist["month_id"].astype(str)

    tc = tc.merge(
        roster_therapist[
            [
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
                ] if c in roster_therapist.columns
            ]
        ].drop_duplicates(["month_id", "outlet_id", "therapist_id"]),
        on=["month_id", "outlet_id", "therapist_id"],
        how="left",
    )

    for c, d in {
        "therapist_consistency_score_0_100": 50.0,
        "utilization_percent": 0.0,
        "yield_eur_per_sold_hour": 0.0,
        "revpath_proxy_eur_per_available_hour": 0.0,
        "attendance_reliability_percent": 0.0,
        "schedule_adherence_percent": 0.0,
        "avg_schedule_stability_score_0_100": 70.0,
        "avg_workload_density_ratio": 0.0,
        "productive_utilization_ratio": 0.0,
        "idle_hour_ratio": 0.0,
        "overtime_hour_ratio": 0.0,
        "coverage_gap_day_ratio": 0.0,
        "burnout_exposure_day_ratio": 0.0,
        "burnout_risk_score_0_100": 20.0,
    }.items():
        tc = safe_numeric(tc, c, d)

    tc = ensure_col(tc, "burnout_band", "guarded")
    tc = ensure_col(tc, "integrity_band", "healthy")

    tc["coaching_priority_band"] = np.select(
        [
            (tc["therapist_consistency_score_0_100"] < 45) | (tc["burnout_risk_score_0_100"] >= 70),
            (tc["therapist_consistency_score_0_100"] < 60) | (tc["burnout_risk_score_0_100"] >= 50),
        ],
        [
            "urgent",
            "watchlist",
        ],
        default="stable",
    )

    tc["managerial_story"] = np.select(
        [
            (tc["burnout_risk_score_0_100"] >= 70) & (tc["productive_utilization_ratio"] >= 0.75),
            (tc["avg_schedule_stability_score_0_100"] < 60) & (tc["coverage_gap_day_ratio"] >= 0.10),
            (tc["idle_hour_ratio"] >= 0.25),
        ],
        [
            "high performer under strain; protect revenue contribution without accelerating burnout",
            "schedule instability is weakening service consistency and operational reliability",
            "underutilized therapist capacity; review shift design, demand matching, and booking conversion",
        ],
        default="therapist performance pattern is stable with manageable coaching need",
    )

    therapist_perf_join_cols = [
        "month_id",
        "outlet_id",
        "therapist_id",
        "therapist_name",
        "upsell_score_0_100",
        "total_commercial_score_0_100",
        "bonus_reward_eligibility_flag",
        "commercial_reward_reason",
        "top3_therapist_flag",
        "bottom3_therapist_flag",
        "refresh_training_required_flag",
        "refresh_training_reason",
        "coaching_action_recommendation",
    ]
    tc = tc.merge(
        therapist_perf[therapist_perf_join_cols].drop_duplicates(["month_id", "outlet_id", "therapist_id"]),
        on=["month_id", "outlet_id", "therapist_id"],
        how="left",
        suffixes=("", "_perf"),
    )

    for c, d in {
        "upsell_score_0_100": 0.0,
        "total_commercial_score_0_100": 0.0,
        "bonus_reward_eligibility_flag": 0.0,
        "top3_therapist_flag": 0.0,
        "bottom3_therapist_flag": 0.0,
        "refresh_training_required_flag": 0.0,
    }.items():
        tc = safe_numeric(tc, c, d)
    tc = ensure_col(tc, "commercial_reward_reason", "Therapist did not yet meet full commercial reward threshold or quality guardrail threshold.")
    tc = ensure_col(tc, "refresh_training_reason", "No immediate refresh training trigger under current modeled threshold.")
    tc = ensure_col(tc, "coaching_action_recommendation", "maintain routine coaching cadence and monitor month-over-month movement")

    therapist_coaching_summary = tc.copy()

    # manager_action_queue
    outlet_actions = outlet_management_summary.copy()
    outlet_actions["action_type"] = "outlet_management"
    outlet_actions["action_theme"] = np.select(
        [
            outlet_actions["management_signal"].eq("grow_carefully_team_under_strain"),
            outlet_actions["management_signal"].eq("demand_leakage_or_scheduling_inefficiency"),
            outlet_actions["management_signal"].eq("protect_team_stability"),
            outlet_actions["management_signal"].eq("coverage_control_required"),
            outlet_actions["management_signal"].eq("commercial_growth_ready"),
        ],
        [
            "capacity_protection",
            "leakage_control",
            "team_stability",
            "coverage_control",
            "growth_acceleration",
        ],
        default="balanced_control",
    )
    outlet_actions["recommended_action"] = outlet_actions["recommended_manager_action"]
    outlet_actions["revenue_impact_direction"] = np.select(
        [
            outlet_actions["action_theme"].eq("growth_acceleration"),
            outlet_actions["action_theme"].eq("leakage_control"),
        ],
        ["upside", "recoverable_upside"],
        default="protective",
    )
    outlet_actions["team_impact_direction"] = np.select(
        [
            outlet_actions["action_theme"].isin(["capacity_protection", "team_stability", "coverage_control"]),
        ],
        ["stabilize"],
        default="neutral",
    )
    outlet_actions["execution_priority_score"] = (
        0.35 * (100 - outlet_actions["overall_management_signal_score_0_100"])
        + 0.20 * outlet_actions["avg_capacity_strain_score_0_100"]
        + 0.15 * (outlet_actions["leakage_control_priority_flag"] * 100)
        + 0.10 * ((1 - outlet_actions["revenue_growth_without_team_burnout_flag"]) * 100)
        + 0.10 * (outlet_actions["refresh_training_attention_flag"] * 100)
        + 0.10 * (outlet_actions["commercial_reward_attention_flag"] * 100)
    ).clip(0, 100)
    outlet_actions["manager_note"] = outlet_actions["managerial_story"] + " | " + outlet_actions["commercial_story"]
    outlet_actions = outlet_actions[
        [
            "month_id",
            "period_start",
            "period_end",
            "outlet_id",
            "outlet_name",
            "action_type",
            "action_theme",
            "management_signal",
            "recommended_action",
            "revenue_impact_direction",
            "team_impact_direction",
            "execution_priority_score",
            "manager_note",
        ]
    ].copy()

    therapist_actions = therapist_coaching_summary.copy()
    therapist_actions["action_type"] = "therapist_coaching"
    therapist_actions["action_theme"] = np.select(
        [
            therapist_actions["bonus_reward_eligibility_flag"].eq(1),
            therapist_actions["refresh_training_required_flag"].eq(1),
            therapist_actions["coaching_priority_band"].eq("urgent"),
            therapist_actions["coaching_priority_band"].eq("watchlist"),
        ],
        [
            "reward_recognition",
            "refresh_training",
            "urgent_coaching",
            "watchlist_coaching",
        ],
        default="routine_coaching",
    )
    therapist_actions["management_signal"] = therapist_actions["action_theme"]
    therapist_actions["recommended_action"] = np.select(
        [
            therapist_actions["bonus_reward_eligibility_flag"].eq(1),
            therapist_actions["refresh_training_required_flag"].eq(1),
            therapist_actions["coaching_priority_band"].eq("urgent"),
            therapist_actions["coaching_priority_band"].eq("watchlist"),
        ],
        [
            "recognize bonus-eligible therapist and capture best-practice upsell behavior",
            therapist_actions["coaching_action_recommendation"],
            "immediate coaching and workload protection review",
            "targeted coaching with schedule and recovery monitoring",
        ],
        default="maintain routine coaching cadence",
    )
    therapist_actions["revenue_impact_direction"] = np.select(
        [
            therapist_actions["bonus_reward_eligibility_flag"].eq(1),
        ],
        ["upside"],
        default="protective",
    )
    therapist_actions["team_impact_direction"] = np.select(
        [
            therapist_actions["refresh_training_required_flag"].eq(1),
        ],
        ["stabilize"],
        default="neutral",
    )
    therapist_actions["execution_priority_score"] = (
        0.35 * (100 - therapist_actions["therapist_consistency_score_0_100"])
        + 0.20 * therapist_actions["burnout_risk_score_0_100"]
        + 0.15 * therapist_actions["coverage_gap_day_ratio"].clip(0, 1) * 100
        + 0.15 * therapist_actions["refresh_training_required_flag"] * 100
        + 0.15 * therapist_actions["bonus_reward_eligibility_flag"] * 100
    ).clip(0, 100)
    therapist_actions["manager_note"] = np.where(
        therapist_actions["bonus_reward_eligibility_flag"].eq(1),
        therapist_actions["commercial_reward_reason"],
        therapist_actions["refresh_training_reason"],
    )
    therapist_actions["outlet_name"] = therapist_actions.get("outlet_name", therapist_actions["outlet_id"])
    therapist_actions = therapist_actions[
        [
            "month_id",
            "period_start",
            "period_end",
            "outlet_id",
            "outlet_name",
            "action_type",
            "action_theme",
            "management_signal",
            "recommended_action",
            "revenue_impact_direction",
            "team_impact_direction",
            "execution_priority_score",
            "manager_note",
        ]
    ].copy()

    treatment_actions = treatment_opportunity_summary.copy()
    treatment_actions["action_type"] = "treatment_opportunity"
    treatment_actions["action_theme"] = np.select(
        [
            treatment_actions["expansion_readiness_flag"].eq(1),
            treatment_actions["utilization_pressure_flag"].eq(1),
            treatment_actions["yield_improvement_opportunity"].eq("pricing_or_mix_optimization"),
        ],
        [
            "growth_acceleration",
            "capacity_protection",
            "yield_optimization",
        ],
        default="maintain_monitor",
    )
    treatment_actions["recommended_action"] = np.select(
        [
            treatment_actions["action_theme"].eq("growth_acceleration"),
            treatment_actions["action_theme"].eq("capacity_protection"),
            treatment_actions["action_theme"].eq("yield_optimization"),
        ],
        [
            "expand promotion and conversion support for scalable treatments",
            "protect capacity before increasing treatment demand",
            "review pricing, package design, and treatment mix for yield uplift",
        ],
        default="maintain and monitor treatment line performance",
    )
    treatment_actions["management_signal"] = treatment_actions["action_theme"]
    treatment_actions["revenue_impact_direction"] = np.select(
        [
            treatment_actions["action_theme"].isin(["growth_acceleration", "yield_optimization"]),
        ],
        ["upside"],
        default="protective",
    )
    treatment_actions["team_impact_direction"] = np.select(
        [
            treatment_actions["action_theme"].eq("capacity_protection"),
        ],
        ["stabilize"],
        default="neutral",
    )
    treatment_actions["execution_priority_score"] = (
        0.45 * (100 - treatment_actions["treatment_health_score_0_100"])
        + 0.30 * treatment_actions["avg_capacity_strain_score_0_100"]
        + 0.25 * treatment_actions["utilization_pressure_flag"] * 100
    ).clip(0, 100)
    treatment_actions["manager_note"] = treatment_actions["opportunity_managerial_story"]
    treatment_actions["outlet_name"] = treatment_actions.get("outlet_name", treatment_actions["outlet_id"])
    treatment_actions = treatment_actions[
        [
            "month_id",
            "period_start",
            "period_end",
            "outlet_id",
            "outlet_name",
            "action_type",
            "action_theme",
            "management_signal",
            "recommended_action",
            "revenue_impact_direction",
            "team_impact_direction",
            "execution_priority_score",
            "manager_note",
        ]
    ].copy()

    # Staff commercial actions
    staff_actions = staff_commercial.copy()
    staff_actions["action_type"] = "staff_commercial"
    staff_actions["action_theme"] = np.select(
        [
            staff_actions["retail_reward_eligibility_flag"].eq(1),
            staff_actions["retail_selling_score_0_100"] < 45,
            staff_actions["retail_selling_score_0_100"] < 60,
        ],
        [
            "reward_recognition",
            "refresh_training",
            "watchlist_commercial_coaching",
        ],
        default="maintain_monitor",
    )
    staff_actions["recommended_action"] = np.select(
        [
            staff_actions["retail_reward_eligibility_flag"].eq(1),
            staff_actions["retail_selling_score_0_100"] < 45,
            staff_actions["retail_selling_score_0_100"] < 60,
        ],
        [
            "recognize retail-selling reward candidate and capture replicable selling behavior",
            "run refresh training on retail recommendation, attach behavior, and guest-conversation conversion",
            "run targeted commercial coaching on retail recommendation and attach-rate improvement",
        ],
        default="maintain current commercial coaching cadence",
    )
    staff_actions["management_signal"] = staff_actions["action_theme"]
    staff_actions["revenue_impact_direction"] = np.select(
        [
            staff_actions["retail_reward_eligibility_flag"].eq(1),
        ],
        ["upside"],
        default="recoverable_upside",
    )
    staff_actions["team_impact_direction"] = "neutral"
    staff_actions["execution_priority_score"] = (
        0.55 * (100 - staff_actions["retail_selling_score_0_100"])
        + 0.45 * staff_actions["retail_reward_eligibility_flag"] * 100
    ).clip(0, 100)
    staff_actions["manager_note"] = staff_actions["reward_bonus_reason"]
    if "period_start" not in staff_actions.columns:
        staff_actions["period_start"] = pd.to_datetime(staff_actions["month_id"] + "-01", errors="coerce")
    if "period_end" not in staff_actions.columns:
        staff_actions["period_end"] = staff_actions["period_start"] + pd.offsets.MonthEnd(0)

    staff_actions = staff_actions[
        [
            "month_id",
            "period_start",
            "period_end",
            "outlet_id",
            "outlet_name",
            "action_type",
            "action_theme",
            "management_signal",
            "recommended_action",
            "revenue_impact_direction",
            "team_impact_direction",
            "execution_priority_score",
            "manager_note",
        ]
    ].copy()

    manager_action_queue = pd.concat(
        [outlet_actions, therapist_actions, treatment_actions, staff_actions],
        ignore_index=True,
    )

    manager_action_queue = manager_action_queue.sort_values(
        ["month_id", "execution_priority_score"],
        ascending=[True, False],
    ).reset_index(drop=True)

    manager_action_queue.insert(
        0, "action_queue_id", [f"MAQ_{i:05d}" for i in range(1, len(manager_action_queue) + 1)]
    )
    manager_action_queue["action_priority_rank"] = (
        manager_action_queue.groupby("month_id")["execution_priority_score"]
        .rank(method="first", ascending=False)
        .astype(int)
    )
    manager_action_queue["execution_priority"] = np.select(
        [
            manager_action_queue["execution_priority_score"] >= 75,
            manager_action_queue["execution_priority_score"] >= 55,
        ],
        [
            "urgent",
            "high",
        ],
        default="normal",
    )

    outlet_fp = INSIGHT_MART / "outlet_management_summary.csv"
    treatment_fp_out = INSIGHT_MART / "treatment_opportunity_summary.csv"
    therapist_fp_out = INSIGHT_MART / "therapist_coaching_summary.csv"
    action_fp = INSIGHT_MART / "manager_action_queue.csv"

    outlet_management_summary.to_csv(outlet_fp, index=False)
    treatment_opportunity_summary.to_csv(treatment_fp_out, index=False)
    therapist_coaching_summary.to_csv(therapist_fp_out, index=False)
    manager_action_queue.to_csv(action_fp, index=False)

    print(f"[OK] saved: {outlet_fp} | rows={len(outlet_management_summary)}")
    print(f"[OK] saved: {treatment_fp_out} | rows={len(treatment_opportunity_summary)}")
    print(f"[OK] saved: {therapist_fp_out} | rows={len(therapist_coaching_summary)}")
    print(f"[OK] saved: {action_fp} | rows={len(manager_action_queue)}")


if __name__ == "__main__":
    main()
