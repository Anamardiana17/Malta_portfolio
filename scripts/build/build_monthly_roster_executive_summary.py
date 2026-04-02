from __future__ import annotations

from pathlib import Path
import pandas as pd


BASE_DIR = Path("/Users/ambakinanti/Desktop/Malta_portfolio")
DEPLOY_FP = BASE_DIR / "data_processed/management/monthly_roster_deployment_recommendation.csv"
INTERPRET_FP = BASE_DIR / "data_processed/management/monthly_roster_management_interpretation.csv"
OUT_FP = BASE_DIR / "data_processed/management/monthly_roster_executive_summary.csv"


def classify_action_urgency(priority_band: str) -> str:
    pb = str(priority_band).strip().lower()
    if pb == "high":
        return "high"
    if pb == "medium":
        return "moderate"
    return "baseline"


def classify_posture(priority_band: str, focus: str, regime: str) -> str:
    pb = str(priority_band).strip().lower()
    focus_raw = str(focus).strip()
    fc = focus_raw.lower()
    rg = str(regime).strip().lower()

    exact_focus_map = {
        "Maintain baseline staffing discipline": "Maintain baseline staffing discipline",
        "Targeted deployment adjustment and monitoring": "Targeted deployment adjustment and monitoring",
        "Coverage stabilization and deployment rebalancing": "Coverage stabilization and deployment rebalancing",
        "Near-term staffing review and management attention": "Near-term staffing review and management attention",
        "Selective pressure management and roster fine-tuning": "Selective pressure management and roster fine-tuning",
        "Controlled deployment lift under supportive context": "Controlled deployment lift under supportive context",
    }

    if focus_raw in exact_focus_map:
        return exact_focus_map[focus_raw]

    if pb == "high":
        return "Coverage stabilization and deployment rebalancing"

    if "coverage stabilization" in fc:
        return "Coverage stabilization and deployment rebalancing"

    if "staffing review" in fc:
        return "Near-term staffing review and management attention"

    if "targeted deployment adjustment" in fc:
        return "Targeted deployment adjustment and monitoring"

    if "selective pressure management" in fc:
        return "Selective pressure management and roster fine-tuning"

    if "controlled deployment lift" in fc and rg == "supportive":
        return "Controlled deployment lift under supportive context"

    return "Maintain baseline staffing discipline"


def build_headline(outlet_key: str, urgency: str, posture: str) -> str:
    outlet = str(outlet_key).strip()

    if urgency == "high":
        return f"{outlet}: immediate staffing attention required to stabilize coverage and deployment discipline."

    if posture == "Controlled deployment lift under supportive context":
        return f"{outlet}: stable staffing base with selective room for controlled deployment lift."

    if posture == "Targeted deployment adjustment and monitoring":
        return f"{outlet}: selective staffing adjustment is appropriate without broad deployment expansion."

    if posture == "Selective pressure management and roster fine-tuning":
        return f"{outlet}: localized staffing pressure should be managed through disciplined roster fine-tuning."

    if posture == "Near-term staffing review and management attention":
        return f"{outlet}: near-term staffing review is warranted before broader operating moves."

    return f"{outlet}: maintain baseline staffing discipline and continue monthly monitoring."


def build_detail(priority_band: str, focus: str, regime: str, confidence) -> str:
    pb = str(priority_band).strip().lower()
    fc = str(focus).strip()
    rg = str(regime).strip().lower()

    try:
        conf_txt = f"{float(confidence):.1f}/100"
    except Exception:
        conf_txt = "n/a"

    if pb == "high":
        return (
            f"Priority band is high, so staffing coverage and deployment balance need immediate management attention. "
            f"Recommended focus: {fc}. External context is {rg} with confidence {conf_txt}, but actionability remains anchored to internal operating proxies."
        )

    if pb == "medium":
        return (
            f"Priority band is medium, so targeted staffing adjustment is more appropriate than structural overreaction. "
            f"Recommended focus: {fc}. External context is {rg} with confidence {conf_txt}, and should be used as contextual framing rather than direct demand truth."
        )

    return (
        f"Priority band is low, so baseline staffing discipline remains appropriate. "
        f"Recommended focus: {fc}. External context is {rg} with confidence {conf_txt}; continue monitoring without forcing unnecessary deployment change."
    )


BOUNDARY_NOTE = (
    "Monthly executive staffing summary only; external proxies function as contextual regime and market-pressure signals, "
    "while internal operating proxies remain the primary decision anchor. This layer does not represent direct hourly spa demand, "
    "observed daypart traffic, or roster-by-hour truth."
)


def main() -> None:
    if not DEPLOY_FP.exists():
        raise FileNotFoundError(f"Missing input file: {DEPLOY_FP}")
    if not INTERPRET_FP.exists():
        raise FileNotFoundError(f"Missing input file: {INTERPRET_FP}")

    deploy = pd.read_csv(DEPLOY_FP)
    interpret = pd.read_csv(INTERPRET_FP)

    join_keys = ["outlet_key", "month_id"]

    keep_cols = [
        "outlet_key",
        "month_id",
        "external_context_regime",
        "roster_decision_priority_score_0_100",
        "roster_decision_priority_band",
        "recommended_management_focus",
        "management_priority_read",
        "management_action_posture",
        "external_context_read",
        "staffing_risk_read",
        "external_signal_confidence_score_0_100",
        "management_interpretation_summary",
    ]
    keep_cols = [c for c in keep_cols if c in interpret.columns]

    df = interpret[keep_cols].copy()

    if "recommended_management_focus" not in df.columns:
        raise ValueError("required column missing: recommended_management_focus")
    if "roster_decision_priority_band" not in df.columns:
        raise ValueError("required column missing: roster_decision_priority_band")
    if "external_context_regime" not in df.columns:
        raise ValueError("required column missing: external_context_regime")

    df["management_action_urgency"] = df["roster_decision_priority_band"].apply(classify_action_urgency)

    df["executive_staffing_posture"] = df.apply(
        lambda r: classify_posture(
            r["roster_decision_priority_band"],
            r["recommended_management_focus"],
            r["external_context_regime"],
        ),
        axis=1,
    )

    df["executive_summary_headline"] = df.apply(
        lambda r: build_headline(
            r["outlet_key"],
            r["management_action_urgency"],
            r["executive_staffing_posture"],
        ),
        axis=1,
    )

    df["executive_summary_detail"] = df.apply(
        lambda r: build_detail(
            r["roster_decision_priority_band"],
            r["recommended_management_focus"],
            r["external_context_regime"],
            r.get("external_signal_confidence_score_0_100"),
        ),
        axis=1,
    )

    df["executive_boundary_note"] = BOUNDARY_NOTE

    preferred_order = [
        "outlet_key",
        "month_id",
        "roster_decision_priority_score_0_100",
        "roster_decision_priority_band",
        "management_action_urgency",
        "recommended_management_focus",
        "executive_staffing_posture",
        "external_context_regime",
        "external_signal_confidence_score_0_100",
        "management_priority_read",
        "management_action_posture",
        "external_context_read",
        "staffing_risk_read",
        "executive_summary_headline",
        "executive_summary_detail",
        "management_interpretation_summary",
        "executive_boundary_note",
    ]
    preferred_order = [c for c in preferred_order if c in df.columns]
    df = df[preferred_order].copy()

    OUT_FP.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_FP, index=False)

    print(f"[OK] saved: {OUT_FP}")
    print(f"[INFO] shape={df.shape}")

    print("\n=== PRIORITY BAND DISTRIBUTION ===")
    print(df["roster_decision_priority_band"].value_counts(dropna=False).to_string())

    print("\n=== ACTION URGENCY DISTRIBUTION ===")
    print(df["management_action_urgency"].value_counts(dropna=False).to_string())

    print("\n=== EXECUTIVE STAFFING POSTURE DISTRIBUTION ===")
    print(df["executive_staffing_posture"].value_counts(dropna=False).to_string())

    print("\n=== SAMPLE OUTPUT ===")
    print(df.head(12).to_string(index=False))


if __name__ == "__main__":
    main()
