from __future__ import annotations

from pathlib import Path
import pandas as pd
import numpy as np


BASE = Path(__file__).resolve().parents[2]
DP = BASE / "data_processed"
MGMT = DP / "management"
INTERNAL_PROXY = DP / "internal_proxy"


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


def ensure_text(df: pd.DataFrame, col: str, default: str = "") -> pd.DataFrame:
    df = df.copy()
    if col not in df.columns:
        df[col] = default
    df[col] = df[col].fillna(default).astype(str)
    return df


def minmax_score(series: pd.Series, fallback: float = 50.0) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce").fillna(0.0)
    s_min = float(s.min())
    s_max = float(s.max())
    if np.isclose(s_min, s_max):
        return pd.Series([fallback] * len(s), index=s.index, dtype=float)
    return ((s - s_min) / (s_max - s_min) * 100.0).clip(0, 100)


def main() -> None:
    coach_fp = pick_file([
        MGMT / "therapist_coaching_summary.csv",
    ])
    mgmt_fp = pick_file([
        MGMT / "management_kpi_signal_layer.csv",
        INTERNAL_PROXY / "management_kpi_signal_layer.csv",
    ])
    role_fp = pick_file([
        MGMT / "outlet_operating_role_framework.csv",
    ])

    coach = pd.read_csv(coach_fp)
    mgmt = pd.read_csv(mgmt_fp)
    role = pd.read_csv(role_fp)

    coach = ensure_text(coach, "month_id")
    coach = ensure_text(coach, "outlet_id")
    coach = ensure_text(coach, "outlet_name")
    coach = ensure_text(coach, "therapist_id")
    coach = ensure_text(coach, "therapist_name", "")
    if (coach["therapist_name"].str.strip() == "").all():
        coach["therapist_name"] = coach["therapist_id"]

    for c, d in {
        "therapist_consistency_score_0_100": 50.0,
        "utilization_percent": 0.0,
        "yield_eur_per_sold_hour": 0.0,
        "revpath_proxy_eur_per_available_hour": 0.0,
        "attendance_reliability_percent": 85.0,
        "schedule_adherence_percent": 85.0,
        "burnout_risk_score_0_100": 20.0,
        "coverage_gap_day_ratio": 0.0,
        "idle_hour_ratio": 0.0,
        "productive_utilization_ratio": 0.0,
    }.items():
        coach = safe_numeric(coach, c, d)

    mgmt = ensure_text(mgmt, "month_id")
    mgmt = ensure_text(mgmt, "outlet_id")
    mgmt = ensure_text(mgmt, "outlet_name")
    for c, d in {
        "overall_management_signal_score_0_100": 50.0,
        "avg_capacity_strain_score_0_100": 0.0,
        "avg_burnout_exposure_day_ratio": 0.0,
        "avg_coverage_gap_day_ratio": 0.0,
        "avg_idle_hour_ratio": 0.0,
        "leakage_control_flag": 0.0,
    }.items():
        mgmt = safe_numeric(mgmt, c, d)
    mgmt = ensure_text(mgmt, "management_signal", "stable_controlled_growth")

    role = ensure_text(role, "outlet_name")
    role = ensure_text(role, "role_name")
    role = safe_numeric(role, "ideal_role_headcount", 0.0)

    role_pivot = (
        role.pivot_table(
            index="outlet_name",
            columns="role_name",
            values="ideal_role_headcount",
            aggfunc="first",
            fill_value=0,
        )
        .reset_index()
        .rename(
            columns={
                "spa receptionist": "ideal_receptionist_headcount",
                "spa attendant": "ideal_attendant_headcount",
                "spa assistant manager": "ideal_assistant_manager_headcount",
                "spa manager": "ideal_spa_manager_headcount",
            }
        )
    )

    # ----------------------------
    # staff commercial scoring layer
    # ----------------------------
    outlet_month = (
        mgmt[["month_id", "outlet_id", "outlet_name", "overall_management_signal_score_0_100",
              "avg_capacity_strain_score_0_100", "avg_burnout_exposure_day_ratio",
              "avg_coverage_gap_day_ratio", "avg_idle_hour_ratio", "management_signal",
              "leakage_control_flag"]]
        .drop_duplicates(["month_id", "outlet_id"])
        .copy()
    )

    outlet_month = outlet_month.merge(role_pivot, on="outlet_name", how="left")
    for c, d in {
        "ideal_receptionist_headcount": 3.0,
        "ideal_attendant_headcount": 3.0,
        "ideal_assistant_manager_headcount": 1.0,
        "ideal_spa_manager_headcount": 1.0,
    }.items():
        outlet_month = safe_numeric(outlet_month, c, d)

    role_specs = [
        ("spa receptionist", "retail"),
        ("spa attendant", "retail"),
        ("spa assistant manager", "retail"),
        ("spa manager", "retail"),
    ]

    rows = []
    for _, r in outlet_month.iterrows():
        role_to_count = {
            "spa receptionist": int(r["ideal_receptionist_headcount"]),
            "spa attendant": int(r["ideal_attendant_headcount"]),
            "spa assistant manager": int(r["ideal_assistant_manager_headcount"]),
            "spa manager": int(r["ideal_spa_manager_headcount"]),
        }

        mgmt_score = float(r["overall_management_signal_score_0_100"])
        strain = float(r["avg_capacity_strain_score_0_100"])
        leakage = float(r["leakage_control_flag"])
        idle = float(r["avg_idle_hour_ratio"])

        for role_name, _ in role_specs:
            staff_n = role_to_count[role_name]
            for i in range(1, staff_n + 1):
                staff_id = f"{r['outlet_id']}_{role_name.replace(' ', '_').upper()}_{i:03d}"
                staff_name = staff_id

                base_rev = {
                    "spa receptionist": 220.0,
                    "spa attendant": 120.0,
                    "spa assistant manager": 260.0,
                    "spa manager": 180.0,
                }[role_name]

                retail_revenue = max(0.0, base_rev + 1.2 * mgmt_score - 0.8 * strain - 40 * leakage - 25 * idle + i * 4)
                retail_units_sold = max(0.0, round(retail_revenue / 28.0))
                guest_interactions = max(retail_units_sold + 8, 35)
                attach_rate = retail_units_sold / guest_interactions
                active_month_ratio = 1.0

                rows.append(
                    {
                        "month_id": r["month_id"],
                        "outlet_id": r["outlet_id"],
                        "outlet_name": r["outlet_name"],
                        "staff_id": staff_id,
                        "staff_name": staff_name,
                        "staff_role": role_name,
                        "retail_revenue_eur": retail_revenue,
                        "retail_units_sold": retail_units_sold,
                        "guest_interactions_count": guest_interactions,
                        "retail_attach_rate": attach_rate,
                        "selling_active_month_ratio": active_month_ratio,
                        "management_signal": r["management_signal"],
                    }
                )

    staff = pd.DataFrame(rows)

    staff["retail_revenue_score_0_100"] = minmax_score(staff["retail_revenue_eur"], fallback=65.0)
    staff["retail_units_score_0_100"] = minmax_score(staff["retail_units_sold"], fallback=65.0)
    staff["retail_attach_rate_score_0_100"] = (staff["retail_attach_rate"].clip(0, 1.0) * 100.0).clip(0, 100)
    staff["selling_consistency_score_0_100"] = (staff["selling_active_month_ratio"].clip(0, 1.0) * 100.0).clip(0, 100)

    # 40 revenue, 25 units, 20 attach, 15 consistency
    staff["retail_selling_score_0_100"] = (
        0.40 * staff["retail_revenue_score_0_100"]
        + 0.25 * staff["retail_units_score_0_100"]
        + 0.20 * staff["retail_attach_rate_score_0_100"]
        + 0.15 * staff["selling_consistency_score_0_100"]
    ).clip(0, 100)

    staff["retail_reward_eligibility_flag"] = np.where(
        staff["retail_selling_score_0_100"] >= 75, 1, 0
    )
    staff["reward_bonus_reason"] = np.where(
        staff["retail_reward_eligibility_flag"] == 1,
        "Retail selling score reached reward threshold under the modeled commercial framework.",
        "Retail selling score below reward threshold; continue coaching and monitor conversion behavior.",
    )

    staff = staff.sort_values(["month_id", "outlet_name", "staff_role", "retail_selling_score_0_100"], ascending=[True, True, True, False]).reset_index(drop=True)
    staff.insert(0, "staff_commercial_scoring_id", [f"SCS_{i:05d}" for i in range(1, len(staff) + 1)])

    staff_out = staff[
        [
            "staff_commercial_scoring_id",
            "month_id",
            "outlet_id",
            "outlet_name",
            "staff_id",
            "staff_name",
            "staff_role",
            "retail_revenue_eur",
            "retail_units_sold",
            "guest_interactions_count",
            "retail_attach_rate",
            "selling_active_month_ratio",
            "retail_revenue_score_0_100",
            "retail_units_score_0_100",
            "retail_attach_rate_score_0_100",
            "selling_consistency_score_0_100",
            "retail_selling_score_0_100",
            "retail_reward_eligibility_flag",
            "reward_bonus_reason",
            "management_signal",
        ]
    ].copy()

    staff_fp = MGMT / "staff_commercial_scoring_layer.csv"
    staff_out.to_csv(staff_fp, index=False)

    # ----------------------------
    # therapist top/bottom performance layer
    # ----------------------------
    th = coach.merge(
        mgmt[
            [
                "month_id", "outlet_id", "outlet_name",
                "overall_management_signal_score_0_100",
                "avg_capacity_strain_score_0_100",
                "avg_burnout_exposure_day_ratio",
                "avg_coverage_gap_day_ratio",
                "avg_idle_hour_ratio",
                "management_signal",
            ]
        ].drop_duplicates(["month_id", "outlet_id"]),
        on=["month_id", "outlet_id", "outlet_name"],
        how="left",
    )

    for c, d in {
        "overall_management_signal_score_0_100": 50.0,
        "avg_capacity_strain_score_0_100": 0.0,
        "avg_burnout_exposure_day_ratio": 0.0,
        "avg_coverage_gap_day_ratio": 0.0,
        "avg_idle_hour_ratio": 0.0,
    }.items():
        th = safe_numeric(th, c, d)
    th = ensure_text(th, "management_signal", "stable_controlled_growth")

    # modeled commercial inputs for therapists
    th["treatment_upsell_revenue_eur"] = (
        0.55 * th["yield_eur_per_sold_hour"].clip(lower=0)
        + 0.25 * th["utilization_percent"].clip(lower=0)
        + 0.10 * th["revpath_proxy_eur_per_available_hour"].clip(lower=0)
        - 0.20 * th["burnout_risk_score_0_100"].clip(lower=0)
    ).clip(lower=0)

    th["upsell_treatment_count"] = np.round((th["treatment_upsell_revenue_eur"] / 22.0).clip(lower=0))
    th["treatment_sessions_proxy"] = np.maximum(np.round(th["utilization_percent"].clip(lower=0) / 4.0), 8)
    th["upsell_attach_rate"] = (th["upsell_treatment_count"] / th["treatment_sessions_proxy"]).clip(0, 1.0)

    th["retail_revenue_eur"] = (
        0.18 * th["yield_eur_per_sold_hour"].clip(lower=0)
        + 0.10 * th["revpath_proxy_eur_per_available_hour"].clip(lower=0)
        + 0.05 * th["utilization_percent"].clip(lower=0)
        - 0.10 * th["burnout_risk_score_0_100"].clip(lower=0)
    ).clip(lower=0)

    th["service_quality_guardrail_score_0_100"] = (
        0.45 * th["therapist_consistency_score_0_100"].clip(0, 100)
        + 0.20 * th["attendance_reliability_percent"].clip(0, 100)
        + 0.20 * th["schedule_adherence_percent"].clip(0, 100)
        + 0.15 * (100 - th["burnout_risk_score_0_100"].clip(0, 100))
    ).clip(0, 100)

    th["treatment_upsell_revenue_score_0_100"] = minmax_score(th["treatment_upsell_revenue_eur"], fallback=65.0)
    th["upsell_attach_rate_score_0_100"] = (th["upsell_attach_rate"].clip(0, 1.0) * 100.0).clip(0, 100)
    th["retail_revenue_score_0_100"] = minmax_score(th["retail_revenue_eur"], fallback=60.0)

    # 35 upsell revenue, 25 attach, 20 retail revenue, 20 guardrail
    th["upsell_score_0_100"] = (
        0.35 * th["treatment_upsell_revenue_score_0_100"]
        + 0.25 * th["upsell_attach_rate_score_0_100"]
        + 0.20 * th["retail_revenue_score_0_100"]
        + 0.20 * th["service_quality_guardrail_score_0_100"]
    ).clip(0, 100)

    th["total_commercial_score_0_100"] = (
        0.55 * th["upsell_score_0_100"]
        + 0.45 * th["therapist_consistency_score_0_100"].clip(0, 100)
    ).clip(0, 100)

    # reward threshold softened so top modeled performers can qualify
    th["bonus_reward_eligibility_flag"] = np.where(
        (th["upsell_score_0_100"] >= 68)
        & (th["service_quality_guardrail_score_0_100"] >= 70)
        & (th["therapist_consistency_score_0_100"] >= 70)
        & (th["burnout_risk_score_0_100"] < 70),
        1,
        0,
    )

    th["commercial_reward_reason"] = np.where(
        th["bonus_reward_eligibility_flag"] == 1,
        "Therapist met upsell, retail, and service-quality guardrail threshold for bonus reward.",
        "Therapist did not yet meet full commercial reward threshold or quality guardrail threshold.",
    )

    th["therapist_rank_within_outlet_month"] = (
        th.groupby(["month_id", "outlet_id"])["total_commercial_score_0_100"]
        .rank(method="first", ascending=False)
        .astype(int)
    )
    th["therapist_reverse_rank_within_outlet_month"] = (
        th.groupby(["month_id", "outlet_id"])["total_commercial_score_0_100"]
        .rank(method="first", ascending=True)
        .astype(int)
    )

    th["therapist_count_within_outlet_month"] = (
        th.groupby(["month_id", "outlet_id"])["therapist_id"]
        .transform("nunique")
        .astype(int)
    )

    th["top_group_cutoff"] = np.where(
        th["therapist_count_within_outlet_month"] >= 6, 3, 1
    )
    th["bottom_group_cutoff"] = np.where(
        th["therapist_count_within_outlet_month"] >= 6, 3, 1
    )

    th["top3_therapist_flag"] = np.where(
        th["therapist_rank_within_outlet_month"] <= th["top_group_cutoff"], 1, 0
    )
    th["bottom3_therapist_flag"] = np.where(
        th["therapist_reverse_rank_within_outlet_month"] <= th["bottom_group_cutoff"], 1, 0
    )

    th["refresh_training_required_flag"] = np.where(
        (
            (th["bottom3_therapist_flag"] == 1)
            & (
                (th["total_commercial_score_0_100"] < 55)
                | (th["service_quality_guardrail_score_0_100"] < 60)
                | (th["therapist_consistency_score_0_100"] < 60)
            )
        )
        | (th["service_quality_guardrail_score_0_100"] < 58)
        | (th["therapist_consistency_score_0_100"] < 58),
        1,
        0,
    )

    th["refresh_training_reason"] = np.select(
        [
            (th["bottom3_therapist_flag"] == 1) & (th["service_quality_guardrail_score_0_100"] < 60),
            (th["bottom3_therapist_flag"] == 1) & (th["total_commercial_score_0_100"] < 55),
            (th["bottom3_therapist_flag"] == 1),
            (th["therapist_consistency_score_0_100"] < 58),
            (th["service_quality_guardrail_score_0_100"] < 58),
        ],
        [
            "Bottom-group therapist ranking combined with weak quality guardrail; refresh training required.",
            "Bottom-group therapist ranking combined with weak commercial score; targeted refresh training required.",
            "Bottom-group therapist ranking; monitor closely and coach as needed.",
            "Therapist consistency score below threshold; refresh training required.",
            "Service-quality guardrail below threshold; refresh training required.",
        ],
        default="No immediate refresh training trigger under current modeled threshold.",
    )

    th["coaching_action_recommendation"] = np.select(
        [
            (th["top3_therapist_flag"] == 1) & (th["bonus_reward_eligibility_flag"] == 1),
            th["top3_therapist_flag"] == 1,
            th["refresh_training_required_flag"] == 1,
        ],
        [
            "protect and reward top performer; consider peer-sharing or role-model coaching contribution",
            "protect top performer and consider peer-sharing or role-model coaching contribution",
            "run targeted refresh training on upsell behavior, guest recommendation quality, and service consistency",
        ],
        default="maintain routine coaching cadence and monitor month-over-month movement",
    )

    therapist_out = th[
        [
            "month_id",
            "outlet_id",
            "outlet_name",
            "therapist_id",
            "therapist_name",
            "therapist_count_within_outlet_month",
            "therapist_consistency_score_0_100",
            "service_quality_guardrail_score_0_100",
            "treatment_upsell_revenue_eur",
            "upsell_treatment_count",
            "upsell_attach_rate",
            "retail_revenue_eur",
            "upsell_score_0_100",
            "total_commercial_score_0_100",
            "bonus_reward_eligibility_flag",
            "commercial_reward_reason",
            "therapist_rank_within_outlet_month",
            "therapist_reverse_rank_within_outlet_month",
            "top_group_cutoff",
            "bottom_group_cutoff",
            "top3_therapist_flag",
            "bottom3_therapist_flag",
            "refresh_training_required_flag",
            "refresh_training_reason",
            "coaching_action_recommendation",
            "management_signal",
        ]
    ].copy()

    therapist_out = therapist_out.sort_values(
        ["month_id", "outlet_name", "therapist_rank_within_outlet_month"],
        ascending=[True, True, True],
    ).reset_index(drop=True)
    therapist_out.insert(0, "therapist_performance_id", [f"TPF_{i:05d}" for i in range(1, len(therapist_out) + 1)])

    therapist_fp = MGMT / "therapist_top_bottom_performance_layer.csv"
    therapist_out.to_csv(therapist_fp, index=False)

    print(f"[OK] saved: {staff_fp} | rows={len(staff_out)}")
    print(f"[OK] saved: {therapist_fp} | rows={len(therapist_out)}")
    print("\nPreview staff:")
    print(staff_out.head(10).to_string(index=False))
    print("\nPreview therapist:")
    print(therapist_out.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
