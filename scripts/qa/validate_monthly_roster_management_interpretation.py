from pathlib import Path
import pandas as pd

BASE = Path(__file__).resolve().parents[2]
FP = BASE / "data_processed" / "management" / "monthly_roster_management_interpretation.csv"

REQUIRED_COLUMNS = [
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

ALLOWED_BANDS = {"low", "medium", "high"}
ALLOWED_REGIMES = {"soft", "neutral", "supportive"}

RISKY_PHRASES = [
    "hourly demand",
    "peak hour",
    "peak demand",
    "daypart demand",
    "roster by hour",
    "true demand",
    "direct demand",
    "observed daypart traffic",
]

BOUNDARY_REQUIRED = [
    "not direct hourly spa demand",
    "not observed daypart traffic",
    "internal operating proxies",
]

TEXT_COLS = [
    "management_priority_read",
    "management_action_posture",
    "external_context_read",
    "staffing_risk_read",
    "recommended_management_focus",
    "evidence_boundary_note",
    "management_interpretation_summary",
]

def fail(msg: str) -> None:
    raise SystemExit(f"[FAIL] {msg}")

def warn(msg: str) -> None:
    print(f"[WARN] {msg}")

def ok(msg: str) -> None:
    print(f"[OK] {msg}")

def main() -> None:
    if not FP.exists():
        fail(f"Missing file: {FP}")

    df = pd.read_csv(FP)
    ok(f"Loaded: {FP}")
    print(f"[INFO] shape={df.shape}")

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        fail(f"Missing required columns: {missing}")
    ok("Required columns present")

    if df.empty:
        fail("Dataset is empty")
    ok("Dataset is non-empty")

    band = df["roster_decision_priority_band"].astype(str).str.strip().str.lower()
    bad_band = sorted(set(band) - ALLOWED_BANDS)
    if bad_band:
        fail(f"Unexpected roster_decision_priority_band values: {bad_band}")
    ok("Priority band values valid")

    regime = df["external_context_regime"].astype(str).str.strip().str.lower()
    bad_regime = sorted(set(regime) - ALLOWED_REGIMES)
    if bad_regime:
        fail(f"Unexpected external_context_regime values: {bad_regime}")
    ok("External context regime values valid")

    for c in TEXT_COLS:
        s = df[c].astype(str).str.strip()
        null_like = s.isna().sum() + (s.eq("")).sum()
        if null_like > 0:
            fail(f"{c} contains empty/null-like values: {int(null_like)}")
    ok("Interpretation text columns are non-empty")

    boundary_text = " ".join(df["evidence_boundary_note"].astype(str).str.lower().unique())
    for phrase in BOUNDARY_REQUIRED:
        if phrase not in boundary_text:
            fail(f'evidence_boundary_note missing required phrase: "{phrase}"')
    ok("Boundary note preserves methodological guardrails")

    found_risk = False
    for c in TEXT_COLS:
        s = df[c].astype(str).str.lower()
        for phrase in RISKY_PHRASES:
            hits = s.str.contains(phrase, na=False)
            if hits.any():
                found_risk = True
                warn(f'Risky phrase "{phrase}" found in {c}: {int(hits.sum())} rows')
    if not found_risk:
        ok("No obvious pseudo-daypart / hourly overclaim wording found")

    print("\n=== PRIORITY BAND DISTRIBUTION ===")
    print(band.value_counts(dropna=False).to_string())

    print("\n=== EXTERNAL CONTEXT REGIME DISTRIBUTION ===")
    print(regime.value_counts(dropna=False).to_string())

    print("\n=== MANAGEMENT FOCUS DISTRIBUTION ===")
    print(df["recommended_management_focus"].value_counts(dropna=False).to_string())

    print("\n[OK] monthly roster management interpretation validation completed")

if __name__ == "__main__":
    main()
