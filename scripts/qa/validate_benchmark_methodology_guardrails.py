from __future__ import annotations

from pathlib import Path
import sys
import pandas as pd


BASE_DIR = Path("/Users/ambakinanti/Desktop/Malta_portfolio")
PRICING_DIR = BASE_DIR / "data_processed" / "pricing_research"
BENCHMARK_FP = PRICING_DIR / "treatment_market_validation_sheet.csv"


def load_csv(fp: Path) -> pd.DataFrame:
    if not fp.exists():
        raise FileNotFoundError(f"Missing file: {fp}")
    return pd.read_csv(fp)


def require_cols(df: pd.DataFrame, cols: list[str], file_label: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise AssertionError(f"{file_label}: missing required columns -> {missing}")


def norm(s: pd.Series) -> pd.Series:
    return s.astype(str).fillna("").str.strip().str.lower()


def contains_exact(s: pd.Series) -> pd.Series:
    return norm(s).str.contains("exact", na=False)


def main() -> int:
    df = load_csv(BENCHMARK_FP)

    required_cols = [
        "treatment_category",
        "duration_match_basis",
        "benchmark_layer_used",
        "benchmark_quality_flag",
        "recommended_basis_used",
        "review_status",
    ]
    require_cols(df, required_cols, BENCHMARK_FP.name)

    duration_basis = norm(df["duration_match_basis"])
    layer_used = norm(df["benchmark_layer_used"])
    quality_flag = norm(df["benchmark_quality_flag"])
    recommended_basis = norm(df["recommended_basis_used"])
    review_status = norm(df["review_status"])

    # Rule 1: non-exact duration basis cannot be promoted into exact logic
    bad_duration_promotion = df[
        (~duration_basis.isin(["exact", "exact_match"])) &
        (
            contains_exact(df["benchmark_layer_used"]) |
            contains_exact(df["benchmark_quality_flag"]) |
            contains_exact(df["recommended_basis_used"])
        )
    ].copy()

    if not bad_duration_promotion.empty:
        cols = [c for c in [
            "treatment_category",
            "treatment_variant",
            "session_duration_min",
            "duration_match_basis",
            "benchmark_layer_used",
            "benchmark_quality_flag",
            "recommended_basis_used",
            "benchmark_source_note",
            "linked_market_benchmark",
            "review_status",
            "audit_note",
        ] if c in bad_duration_promotion.columns]
        raise AssertionError(
            "Found rows where non-exact duration basis is promoted into exact logic:\n"
            + bad_duration_promotion[cols].to_string(index=False)
        )

    # Rule 2: commercial/overlay-style logic must not masquerade as pure exact
    commercial_mask = (
        layer_used.str.contains("commercial", na=False) |
        layer_used.str.contains("overlay", na=False) |
        review_status.str.contains("commercial", na=False)
    )

    false_pure_exact = df[
        commercial_mask &
        recommended_basis.eq("exact")
    ].copy()

    if not false_pure_exact.empty:
        cols = [c for c in [
            "treatment_category",
            "treatment_variant",
            "session_duration_min",
            "duration_match_basis",
            "benchmark_layer_used",
            "benchmark_quality_flag",
            "recommended_basis_used",
            "review_status",
            "decision_rule",
            "decision_adjustment_note",
            "audit_note",
        ] if c in false_pure_exact.columns]
        raise AssertionError(
            "Found rows where commercial overlay appears to be labeled as pure exact recommendation:\n"
            + false_pure_exact[cols].to_string(index=False)
        )

    # Rule 3: wrap must remain review-governed on validation sheet
    wrap = df[norm(df["treatment_category"]).eq("wrap")].copy()
    if wrap.empty:
        raise AssertionError("No wrap row found in treatment_market_validation_sheet.csv")

    allowed_wrap_review_status = {
        "needs_commercial_market_validation",
        "review_before_launch",
        "manual_review_before_display",
        "hold_for_review",
        "manual_review",
        "needs_review",
        "review",
    }

    bad_wrap_review = wrap[~norm(wrap["review_status"]).isin({x.lower() for x in allowed_wrap_review_status})].copy()
    if not bad_wrap_review.empty:
        cols = [c for c in [
            "treatment_category",
            "treatment_variant",
            "session_duration_min",
            "duration_match_basis",
            "benchmark_layer_used",
            "recommended_basis_used",
            "review_status",
            "audit_note",
        ] if c in bad_wrap_review.columns]
        raise AssertionError(
            "Wrap row exists but review_status is not aligned with governance expectation:\n"
            + bad_wrap_review[cols].to_string(index=False)
        )

    print("[OK] methodology guardrail passed")
    print("  - exact/non-exact promotion rule passed")
    print("  - commercial-overlay masquerade check passed")
    print("  - wrap review-governed status accepted")

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[FAIL] {exc}", file=sys.stderr)
        raise
