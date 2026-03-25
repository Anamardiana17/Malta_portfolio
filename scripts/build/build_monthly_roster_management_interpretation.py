from pathlib import Path
import pandas as pd
import numpy as np

BASE = Path(__file__).resolve().parents[2]
IN_FP = BASE / "data_processed" / "management" / "monthly_roster_deployment_recommendation.csv"
OUT_FP = BASE / "data_processed" / "management" / "monthly_roster_management_interpretation.csv"

KEY_COLS = ["outlet_key", "month_id"]

def require_columns(df: pd.DataFrame, cols: list[str]) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise SystemExit(f"[FAIL] Missing required columns: {missing}")

def classify_priority_read(score: float, band: str) -> str:
    if pd.isna(score):
        return "Priority score unavailable; review supporting signals before action."
    if band == "high":
        return "High deployment priority; review staffing deployment and operating pressure immediately."
    if band == "medium":
        return "Moderate deployment priority; monitor pressure and adjust staffing discipline selectively."
    return "Low deployment priority; maintain current deployment stance and monitor for drift."

def classify_action_posture(band: str, staffing_readiness: float, capacity_ready: float, coverage_pressure: float, burnout: float) -> str:
    staffing_readiness = 0.0 if pd.isna(staffing_readiness) else staffing_readiness
    capacity_ready = 0.0 if pd.isna(capacity_ready) else capacity_ready
    coverage_pressure = 0.0 if pd.isna(coverage_pressure) else coverage_pressure
    burnout = 0.0 if pd.isna(burnout) else burnout

    if band == "high":
        if coverage_pressure >= 50 or burnout >= 45:
            return "Stabilize coverage first before expansion-oriented actions."
        if staffing_readiness >= 70 and capacity_ready >= 65:
            return "Prioritize near-term deployment adjustment with controlled expansion readiness."
        return "Prioritize near-term staffing review and deployment discipline."
    if band == "medium":
        if coverage_pressure >= 45 or burnout >= 40:
            return "Maintain selective intervention posture with close monitoring of team pressure."
        return "Use targeted staffing adjustment rather than broad deployment changes."
    return "Maintain current staffing posture and review only if internal signals deteriorate."

def classify_external_context_read(regime: str, ext_score: float, align_score: float) -> str:
    regime = "" if pd.isna(regime) else str(regime).strip().lower()
    ext_score = np.nan if pd.isna(ext_score) else float(ext_score)
    align_score = np.nan if pd.isna(align_score) else float(align_score)

    if regime == "supportive":
        if not pd.isna(align_score) and align_score >= 80:
            return "External context is supportive and broadly aligned with internal operating signals."
        return "External context is supportive, but deployment action should still follow internal operating evidence."
    if regime == "soft":
        return "External context is soft; use it as cautionary context rather than as a direct staffing trigger."
    return "External context is neutral; rely mainly on internal operating signals for deployment decisions."

def classify_staffing_risk_read(coverage_pressure: float, burnout: float, staffing_readiness: float) -> str:
    coverage_pressure = 0.0 if pd.isna(coverage_pressure) else coverage_pressure
    burnout = 0.0 if pd.isna(burnout) else burnout
    staffing_readiness = 0.0 if pd.isna(staffing_readiness) else staffing_readiness

    if coverage_pressure >= 55 or burnout >= 50:
        return "Staffing risk is elevated due to coverage pressure and/or burnout exposure."
    if staffing_readiness < 60:
        return "Staffing readiness is constrained; avoid aggressive deployment changes."
    if coverage_pressure >= 40 or burnout >= 35:
        return "Staffing risk is watchlist-level; adjust selectively and monitor execution stability."
    return "Staffing risk appears manageable under the current monthly operating pattern."

def classify_management_focus(band: str, coverage_pressure: float, burnout: float, staffing_readiness: float, regime: str) -> str:
    coverage_pressure = 0.0 if pd.isna(coverage_pressure) else coverage_pressure
    burnout = 0.0 if pd.isna(burnout) else burnout
    staffing_readiness = 0.0 if pd.isna(staffing_readiness) else staffing_readiness
    regime = "" if pd.isna(regime) else str(regime).strip().lower()

    if band == "high":
        if coverage_pressure >= 50:
            return "Coverage stabilization and deployment rebalancing"
        if burnout >= 45:
            return "Burnout-risk containment and schedule discipline"
        if staffing_readiness >= 70 and regime == "supportive":
            return "Controlled deployment lift under supportive context"
        return "Near-term staffing review and management attention"
    if band == "medium":
        if burnout >= 40:
            return "Selective pressure management and roster fine-tuning"
        return "Targeted deployment adjustment and monitoring"
    return "Maintain baseline staffing discipline"

def build_boundary_note() -> str:
    return (
        "Monthly management interpretation only; external context is supportive regime context, "
        "not direct hourly spa demand or observed daypart traffic. Actionability should remain anchored "
        "to internal operating proxies."
    )

def build_summary(priority_read: str, posture: str, ext_read: str, risk_read: str, focus: str) -> str:
    return " | ".join([priority_read, posture, ext_read, risk_read, f"Focus: {focus}."])

def main() -> None:
    if not IN_FP.exists():
        raise SystemExit(f"[FAIL] Missing input file: {IN_FP}")

    df = pd.read_csv(IN_FP)
    require_columns(df, KEY_COLS + [
        "external_context_regime",
        "roster_decision_priority_score_0_100",
        "roster_decision_priority_band",
        "external_demand_context_score_0_100",
        "external_internal_alignment_score_0_100",
        "coverage_pressure_score_0_100",
        "burnout_exposure_score_0_100",
        "staffing_readiness_score_0_100",
        "capacity_expansion_readiness_score_0_100",
    ])

    out = df.copy()

    out["management_priority_read"] = out.apply(
        lambda r: classify_priority_read(
            r["roster_decision_priority_score_0_100"],
            str(r["roster_decision_priority_band"]).strip().lower()
        ),
        axis=1
    )

    out["management_action_posture"] = out.apply(
        lambda r: classify_action_posture(
            str(r["roster_decision_priority_band"]).strip().lower(),
            r["staffing_readiness_score_0_100"],
            r["capacity_expansion_readiness_score_0_100"],
            r["coverage_pressure_score_0_100"],
            r["burnout_exposure_score_0_100"],
        ),
        axis=1
    )

    out["external_context_read"] = out.apply(
        lambda r: classify_external_context_read(
            r["external_context_regime"],
            r["external_demand_context_score_0_100"],
            r["external_internal_alignment_score_0_100"],
        ),
        axis=1
    )

    out["staffing_risk_read"] = out.apply(
        lambda r: classify_staffing_risk_read(
            r["coverage_pressure_score_0_100"],
            r["burnout_exposure_score_0_100"],
            r["staffing_readiness_score_0_100"],
        ),
        axis=1
    )

    out["recommended_management_focus"] = out.apply(
        lambda r: classify_management_focus(
            str(r["roster_decision_priority_band"]).strip().lower(),
            r["coverage_pressure_score_0_100"],
            r["burnout_exposure_score_0_100"],
            r["staffing_readiness_score_0_100"],
            r["external_context_regime"],
        ),
        axis=1
    )

    out["evidence_boundary_note"] = build_boundary_note()

    out["management_interpretation_summary"] = out.apply(
        lambda r: build_summary(
            r["management_priority_read"],
            r["management_action_posture"],
            r["external_context_read"],
            r["staffing_risk_read"],
            r["recommended_management_focus"],
        ),
        axis=1
    )

    preferred_cols = [
        "outlet_key",
        "month_id",
        "external_context_regime",
        "roster_decision_priority_score_0_100",
        "roster_decision_priority_band",
        "management_priority_read",
        "management_action_posture",
        "external_context_read",
        "staffing_risk_read",
        "recommended_management_focus",
        "evidence_boundary_note",
        "management_interpretation_summary",
    ]

    remaining = [c for c in out.columns if c not in preferred_cols]
    out = out[preferred_cols + remaining]

    OUT_FP.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_FP, index=False)

    print(f"[OK] saved: {OUT_FP}")
    print(f"[INFO] shape={out.shape}")

    for c in [
        "roster_decision_priority_band",
        "external_context_regime",
        "recommended_management_focus",
    ]:
        if c in out.columns:
            print(f"\n=== {c} ===")
            print(out[c].value_counts(dropna=False).to_string())

    print("\n=== SAMPLE OUTPUT ===")
    sample_cols = [
        c for c in [
            "outlet_key",
            "month_id",
            "roster_decision_priority_band",
            "external_context_regime",
            "recommended_management_focus",
            "management_interpretation_summary",
        ] if c in out.columns
    ]
    print(out[sample_cols].head(10).to_string(index=False))

if __name__ == "__main__":
    main()
