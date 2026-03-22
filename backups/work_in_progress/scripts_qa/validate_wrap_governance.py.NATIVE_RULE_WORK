from __future__ import annotations

from pathlib import Path
import sys
import pandas as pd


BASE_DIR = Path("/Users/ambakinanti/Desktop/Malta_portfolio")
PRICING_DIR = BASE_DIR / "data_processed" / "pricing_research"

MASTER_FP = PRICING_DIR / "final_treatment_pricing_master.csv"
VAT_FP = PRICING_DIR / "final_treatment_pricing_master_with_vat.csv"
POS_FP = PRICING_DIR / "treatment_pos_display_price_sheet.csv"

WRAP_MARKET_MEDIAN = 72.0
WRAP_RECOMMENDED_PRICE = 82.0


def load_csv(fp: Path) -> pd.DataFrame:
    if not fp.exists():
        raise FileNotFoundError(f"Missing file: {fp}")
    return pd.read_csv(fp)


def require_single_row(df: pd.DataFrame, category: str, file_label: str) -> pd.Series:
    subset = df[df["treatment_category"].astype(str).str.strip().str.lower().eq(category)].copy()
    if len(subset) != 1:
        raise AssertionError(
            f"{file_label}: expected exactly 1 row for treatment_category='{category}', got {len(subset)}"
        )
    return subset.iloc[0]


def has_col(df: pd.DataFrame, col: str) -> bool:
    return col in df.columns


def as_float(value, label: str) -> float:
    try:
        return float(value)
    except Exception as exc:
        raise AssertionError(f"Cannot cast {label} to float: {value}") from exc


def assert_equal(actual, expected, label: str) -> None:
    if str(actual).strip() != str(expected).strip():
        raise AssertionError(f"{label}: expected '{expected}', got '{actual}'")


def assert_float(actual, expected, label: str, tol: float = 1e-9) -> None:
    if abs(actual - expected) > tol:
        raise AssertionError(f"{label}: expected {expected}, got {actual}")


def main() -> int:
    master = load_csv(MASTER_FP)
    vat = load_csv(VAT_FP)
    pos = load_csv(POS_FP)

    if "treatment_category" not in master.columns:
        raise AssertionError("final_treatment_pricing_master.csv missing treatment_category")
    if "treatment_category" not in vat.columns:
        raise AssertionError("final_treatment_pricing_master_with_vat.csv missing treatment_category")
    if "treatment_category" not in pos.columns:
        raise AssertionError("treatment_pos_display_price_sheet.csv missing treatment_category")

    wrap_master = require_single_row(master, "wrap", "final_treatment_pricing_master.csv")
    wrap_vat = require_single_row(vat, "wrap", "final_treatment_pricing_master_with_vat.csv")
    wrap_pos = require_single_row(pos, "wrap", "treatment_pos_display_price_sheet.csv")

    # Master checks
    if has_col(master, "market_price_median_eur"):
        assert_float(
            as_float(wrap_master["market_price_median_eur"], "market_price_median_eur"),
            WRAP_MARKET_MEDIAN,
            "wrap market_price_median_eur",
        )

    if has_col(master, "recommended_sell_price_eur"):
        assert_float(
            as_float(wrap_master["recommended_sell_price_eur"], "recommended_sell_price_eur"),
            WRAP_RECOMMENDED_PRICE,
            "wrap recommended_sell_price_eur",
        )

    if has_col(master, "launch_recommendation_flag"):
        assert_equal(
            wrap_master["launch_recommendation_flag"],
            "review_before_launch",
            "wrap launch_recommendation_flag",
        )
    elif has_col(master, "review_status"):
        allowed = {"review_before_launch", "needs_commercial_market_validation", "manual_review", "needs_review", "review"}
        if str(wrap_master["review_status"]).strip().lower() not in {x.lower() for x in allowed}:
            raise AssertionError(
                f"wrap review_status not in allowed set: {wrap_master['review_status']}"
            )

    # VAT checks
    if has_col(vat, "display_status"):
        allowed_vat = {"manual_review_before_display", "hold_for_review", "needs_commercial_market_validation"}
        if str(wrap_vat["display_status"]).strip().lower() not in {x.lower() for x in allowed_vat}:
            raise AssertionError(
                f"wrap VAT display_status not in allowed set: {wrap_vat['display_status']}"
            )

    if has_col(vat, "sell_price_inc_vat_eur"):
        vat_price = as_float(wrap_vat["sell_price_inc_vat_eur"], "sell_price_inc_vat_eur")
        if vat_price <= 0:
            raise AssertionError("wrap VAT sell_price_inc_vat_eur must be > 0")

    # POS checks
    if has_col(pos, "display_status"):
        allowed_pos = {"hold_for_review", "manual_review_before_display", "needs_commercial_market_validation"}
        if str(wrap_pos["display_status"]).strip().lower() not in {x.lower() for x in allowed_pos}:
            raise AssertionError(
                f"wrap POS display_status not in allowed set: {wrap_pos['display_status']}"
            )

    if has_col(pos, "display_price_inc_vat_eur"):
        pos_price = as_float(wrap_pos["display_price_inc_vat_eur"], "display_price_inc_vat_eur")
        if pos_price <= 0:
            raise AssertionError("wrap POS display_price_inc_vat_eur must be > 0")

    print("[OK] wrap governance validation passed")
    print("  - wrap market median locked to 72.0 where field exists")
    print("  - wrap recommended sell price locked to 82.0 where field exists")
    print("  - wrap governance remains review-controlled")

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[FAIL] {exc}", file=sys.stderr)
        raise
