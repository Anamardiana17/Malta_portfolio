from pathlib import Path
import pandas as pd

BASE = Path(__file__).resolve().parents[2]
FP = BASE / "data_processed" / "management" / "monthly_roster_deployment_recommendation.csv"

REQUIRED_COLUMNS = [
    "outlet_key",
    "month_id",
    "external_context_regime",
    "roster_decision_priority_score_0_100",
    "roster_decision_priority_band",
    "managerial_roster_reason",
]

ALLOWED_REGIMES = {"soft", "neutral", "supportive"}
ALLOWED_BANDS = {"low", "medium", "high"}

OPTIONAL_SCORE_COLUMNS = [
    "external_demand_context_score_0_100",
    "external_signal_confidence_score_0_100",
    "external_internal_alignment_score_0_100",
    "coverage_pressure_score_0_100",
    "burnout_exposure_score_0_100",
    "staffing_readiness_score_0_100",
    "capacity_expansion_readiness_score_0_100",
    "roster_decision_priority_score_0_100",
]

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

    for c in OPTIONAL_SCORE_COLUMNS:
        if c in df.columns:
            s = pd.to_numeric(df[c], errors="coerce")
            non_null = int(s.notna().sum())
            null = int(s.isna().sum())
            min_v = round(float(s.min()), 4) if s.notna().any() else None
            mean_v = round(float(s.mean()), 4) if s.notna().any() else None
            max_v = round(float(s.max()), 4) if s.notna().any() else None
            print(f"[INFO] {c}: non_null={non_null}, null={null}, min={min_v}, mean={mean_v}, max={max_v}")
            if ((s.dropna() < 0) | (s.dropna() > 100)).any():
                fail(f"{c} contains values outside 0-100 range")

    score = pd.to_numeric(df["roster_decision_priority_score_0_100"], errors="coerce")
    if score.isna().any():
        fail("roster_decision_priority_score_0_100 contains null/non-numeric values")
    ok("Priority score is numeric and non-null")

    band = df["roster_decision_priority_band"].astype(str).str.strip().str.lower()
    bad_band = sorted(set(band) - ALLOWED_BANDS)
    if bad_band:
        fail(f"Unexpected roster_decision_priority_band values: {bad_band}")
    ok("Priority band values valid")

    expected_band = pd.Series(index=df.index, dtype="object")
    expected_band.loc[score >= 45] = "high"
    expected_band.loc[(score >= 35) & (score < 45)] = "medium"
    expected_band.loc[score < 35] = "low"

    mismatches = df.loc[band != expected_band, [
        "outlet_key", "month_id", "roster_decision_priority_score_0_100", "roster_decision_priority_band"
    ]]
    if not mismatches.empty:
        print("\n=== BAND THRESHOLD MISMATCH SAMPLE ===")
        print(mismatches.head(10).to_string(index=False))
        fail(f"Found {len(mismatches)} priority-band threshold mismatches")
    ok("Priority band thresholds match branch rule")

    regime = df["external_context_regime"].astype(str).str.strip().str.lower()
    bad_regime = sorted(set(regime) - ALLOWED_REGIMES)
    if bad_regime:
        fail(f"Unexpected external_context_regime values: {bad_regime}")
    ok("External context regime values valid")

    if "external_signal_confidence_score_0_100" in df.columns:
        conf = pd.to_numeric(df["external_signal_confidence_score_0_100"], errors="coerce")
        if conf.isna().all():
            warn("external_signal_confidence_score_0_100 is fully null")
        elif conf.isna().any():
            warn(f"external_signal_confidence_score_0_100 has partial nulls: {int(conf.isna().sum())}")

    narrative = df["managerial_roster_reason"].astype(str).str.lower()
    found_risk = False
    for phrase in RISKY_PHRASES:
        hits = narrative.str.contains(phrase, na=False)
        if hits.any():
            found_risk = True
            warn(f'Possible overclaim phrase "{phrase}" found in managerial_roster_reason ({int(hits.sum())} rows)')
    if not found_risk:
        ok("No obvious overclaim wording found in managerial_roster_reason")

    print("\n=== EXTERNAL CONTEXT REGIME DISTRIBUTION ===")
    print(regime.value_counts(dropna=False).to_string())

    print("\n=== PRIORITY BAND DISTRIBUTION ===")
    print(band.value_counts(dropna=False).to_string())

    print("\n[OK] monthly roster deployment recommendation validation completed")

if __name__ == "__main__":
    main()
