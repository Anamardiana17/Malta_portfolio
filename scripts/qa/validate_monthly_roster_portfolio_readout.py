from __future__ import annotations

from pathlib import Path
import pandas as pd


BASE_DIR = Path("/Users/ambakinanti/Desktop/Malta_portfolio")
FP = BASE_DIR / "data_processed/management/monthly_roster_portfolio_readout.csv"

REQUIRED_COLUMNS = [
    "outlet_key",
    "month_id",
    "roster_decision_priority_band",
    "management_action_urgency",
    "recommended_management_focus",
    "executive_staffing_posture",
    "external_context_regime",
    "portfolio_staffing_headline",
    "portfolio_staffing_takeaway",
    "portfolio_context_note",
    "portfolio_boundary_note",
]

VALID_PRIORITY = {"low", "medium", "high"}
VALID_URGENCY = {"baseline", "moderate", "high"}
VALID_REGIME = {"neutral", "soft", "supportive"}

REQUIRED_BOUNDARY_SNIPPETS = [
    "external proxies are contextual market-pressure signals",
    "internal operating proxies remain the primary decision anchor",
    "does not represent direct hourly spa demand",
    "observed daypart traffic",
    "roster-by-hour truth",
]

FORBIDDEN_PATTERNS = [
    "direct hourly spa demand truth",
    "observed peak spa hours from external data",
    "daypart truth from passenger counts",
    "roster by hour from external context",
]


def ok(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)
    print(f"[OK] {message}")


def main() -> None:
    ok(FP.exists(), f"Loaded: {FP}")
    df = pd.read_csv(FP)

    print(f"[INFO] shape={df.shape}")

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    ok(len(missing) == 0, "Required columns present")
    ok(not df.empty, "Dataset is non-empty")

    vals = set(df["roster_decision_priority_band"].dropna().astype(str).str.lower().unique())
    ok(vals.issubset(VALID_PRIORITY), "Priority band values valid")

    vals = set(df["management_action_urgency"].dropna().astype(str).str.lower().unique())
    ok(vals.issubset(VALID_URGENCY), "Management action urgency values valid")

    vals = set(df["external_context_regime"].dropna().astype(str).str.lower().unique())
    ok(vals.issubset(VALID_REGIME), "External context regime values valid")

    for col in [
        "portfolio_staffing_headline",
        "portfolio_staffing_takeaway",
        "portfolio_context_note",
        "portfolio_boundary_note",
    ]:
        ok(df[col].fillna("").astype(str).str.strip().ne("").all(), f"{col} is non-empty")

    boundary_text = " ".join(df["portfolio_boundary_note"].fillna("").astype(str).unique()).lower()
    for snippet in REQUIRED_BOUNDARY_SNIPPETS:
        ok(snippet in boundary_text, f"Boundary note preserves snippet: {snippet}")

    combined = " ".join(
        df[
            [
                "portfolio_staffing_headline",
                "portfolio_staffing_takeaway",
                "portfolio_context_note",
                "portfolio_boundary_note",
            ]
        ]
        .fillna("")
        .astype(str)
        .agg(" ".join, axis=1)
        .tolist()
    ).lower()

    for pattern in FORBIDDEN_PATTERNS:
        ok(pattern not in combined, f"No forbidden overclaim phrase: {pattern}")

    print("\n=== PRIORITY BAND DISTRIBUTION ===")
    print(df["roster_decision_priority_band"].value_counts(dropna=False).to_string())

    print("\n=== MANAGEMENT ACTION URGENCY DISTRIBUTION ===")
    print(df["management_action_urgency"].value_counts(dropna=False).to_string())

    print("\n=== EXECUTIVE STAFFING POSTURE DISTRIBUTION ===")
    print(df["executive_staffing_posture"].value_counts(dropna=False).to_string())


if __name__ == "__main__":
    main()
