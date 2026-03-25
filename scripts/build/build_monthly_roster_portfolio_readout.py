from __future__ import annotations

from pathlib import Path
import pandas as pd


BASE_DIR = Path("/Users/ambakinanti/Desktop/Malta_portfolio")
IN_FP = BASE_DIR / "data_processed/management/monthly_roster_executive_summary.csv"
OUT_FP = BASE_DIR / "data_processed/management/monthly_roster_portfolio_readout.csv"


BOUNDARY_NOTE = (
    "Portfolio staffing readout only; external proxies are contextual market-pressure signals, "
    "while internal operating proxies remain the primary decision anchor. "
    "This layer does not represent direct hourly spa demand, observed daypart traffic, or roster-by-hour truth."
)


def build_headline(outlet_key: str, posture: str, urgency: str) -> str:
    outlet = str(outlet_key).strip()
    posture = str(posture).strip()
    urgency = str(urgency).strip().lower()

    if urgency == "high":
        return f"{outlet}: staffing coverage requires immediate management attention."

    if posture == "Controlled deployment lift under supportive context":
        return f"{outlet}: stable staffing base with selective room for controlled deployment lift."

    if posture == "Near-term staffing review and management attention":
        return f"{outlet}: staffing position should be reviewed in the near term."

    if posture == "Selective pressure management and roster fine-tuning":
        return f"{outlet}: localized staffing pressure should be managed through disciplined roster fine-tuning."

    if posture == "Targeted deployment adjustment and monitoring":
        return f"{outlet}: targeted staffing adjustment is appropriate without broad deployment expansion."

    if posture == "Coverage stabilization and deployment rebalancing":
        return f"{outlet}: staffing stability should be reinforced through coverage rebalancing."

    return f"{outlet}: maintain baseline staffing discipline under monthly monitoring."


def build_takeaway(priority_band: str, posture: str, focus: str) -> str:
    pb = str(priority_band).strip().lower()
    posture = str(posture).strip()
    focus = str(focus).strip()

    if pb == "high":
        return (
            f"Priority is high. Management should prioritize coverage stability and deployment control. "
            f"Current staffing stance: {posture}. Recommended focus: {focus}."
        )

    if pb == "medium":
        return (
            f"Priority is medium. Management should use selective staffing adjustment rather than structural overreaction. "
            f"Current staffing stance: {posture}. Recommended focus: {focus}."
        )

    return (
        f"Priority is low. Baseline staffing discipline remains appropriate with continued monitoring. "
        f"Current staffing stance: {posture}. Recommended focus: {focus}."
    )


def build_context_note(regime: str, confidence) -> str:
    regime = str(regime).strip().lower()
    try:
        conf = float(confidence)
        conf_txt = f"{conf:.1f}/100"
    except Exception:
        conf_txt = "n/a"

    if regime == "supportive":
        return (
            f"External context is supportive with confidence {conf_txt}, but it should be used only as contextual framing, "
            f"not as direct demand truth."
        )

    if regime == "soft":
        return (
            f"External context is soft with confidence {conf_txt}, supporting a cautious efficiency-led staffing stance "
            f"without implying direct hourly demand visibility."
        )

    return (
        f"External context is neutral with confidence {conf_txt}; internal operating proxies should remain the main staffing anchor."
    )


def main() -> None:
    if not IN_FP.exists():
        raise FileNotFoundError(f"Missing input file: {IN_FP}")

    df = pd.read_csv(IN_FP)

    required = [
        "outlet_key",
        "month_id",
        "roster_decision_priority_band",
        "management_action_urgency",
        "recommended_management_focus",
        "executive_staffing_posture",
        "external_context_regime",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    df["portfolio_staffing_headline"] = df.apply(
        lambda r: build_headline(
            r["outlet_key"],
            r["executive_staffing_posture"],
            r["management_action_urgency"],
        ),
        axis=1,
    )

    df["portfolio_staffing_takeaway"] = df.apply(
        lambda r: build_takeaway(
            r["roster_decision_priority_band"],
            r["executive_staffing_posture"],
            r["recommended_management_focus"],
        ),
        axis=1,
    )

    df["portfolio_context_note"] = df.apply(
        lambda r: build_context_note(
            r["external_context_regime"],
            r.get("external_signal_confidence_score_0_100"),
        ),
        axis=1,
    )

    df["portfolio_boundary_note"] = BOUNDARY_NOTE

    keep_cols = [
        "outlet_key",
        "month_id",
        "roster_decision_priority_band",
        "management_action_urgency",
        "recommended_management_focus",
        "executive_staffing_posture",
        "external_context_regime",
        "external_signal_confidence_score_0_100",
        "portfolio_staffing_headline",
        "portfolio_staffing_takeaway",
        "portfolio_context_note",
        "portfolio_boundary_note",
    ]
    keep_cols = [c for c in keep_cols if c in df.columns]

    out = df[keep_cols].copy()

    OUT_FP.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_FP, index=False)

    print(f"[OK] saved: {OUT_FP}")
    print(f"[INFO] shape={out.shape}")

    print("\n=== PRIORITY BAND DISTRIBUTION ===")
    print(out["roster_decision_priority_band"].value_counts(dropna=False).to_string())

    print("\n=== MANAGEMENT ACTION URGENCY DISTRIBUTION ===")
    print(out["management_action_urgency"].value_counts(dropna=False).to_string())

    print("\n=== EXECUTIVE STAFFING POSTURE DISTRIBUTION ===")
    print(out["executive_staffing_posture"].value_counts(dropna=False).to_string())

    print("\n=== SAMPLE OUTPUT ===")
    print(out.head(12).to_string(index=False))


if __name__ == "__main__":
    main()
