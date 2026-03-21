from __future__ import annotations

import pandas as pd
from pathlib import Path

MASTER_FP = Path("data_processed/pricing_research/final_treatment_pricing_master.csv")
VAT_FP = Path("data_processed/pricing_research/final_treatment_pricing_master_with_vat.csv")
POS_FP = Path("data_processed/pricing_research/treatment_pos_display_price_sheet.csv")

def patch_master() -> None:
    df = pd.read_csv(MASTER_FP, dtype=str).fillna("")

    mask = (
        df["treatment_category"].astype(str).eq("wrap") &
        df["market_check_status"].astype(str).eq("above_market_median")
    )

    if mask.any():
        df.loc[mask, "price_architecture_flag"] = "premium_edge"
        df.loc[mask, "launch_recommendation_flag"] = "review_before_launch"
        df.loc[mask, "execution_priority"] = "high"
        df.loc[mask, "pricing_summary_note"] = (
            "Recommended sell price "
            + df.loc[mask, "recommended_sell_price_eur"].astype(str)
            + " EUR vs floor "
            + df.loc[mask, "pricing_floor_mid_eur"].astype(str)
            + " EUR; exact wrap benchmark is still thin, so commercial review is required before launch."
        )
        df.loc[mask, "decision_adjustment_note"] = (
            "exact sample remains thin and recommended sell price sits above market median; hold for commercial review before launch"
        )

    df.to_csv(MASTER_FP, index=False)
    print(f"[OK] patched: {MASTER_FP}")

def patch_vat() -> None:
    df = pd.read_csv(VAT_FP, dtype=str).fillna("")

    mask = (
        df["treatment_category"].astype(str).eq("wrap") &
        df["market_check_status"].astype(str).eq("above_market_median")
    )

    if mask.any():
        df.loc[mask, "price_architecture_flag"] = "premium_edge"
        df.loc[mask, "launch_recommendation_flag"] = "review_before_launch"
        df.loc[mask, "execution_priority"] = "high"
        df.loc[mask, "display_price_rule"] = "manual_review_before_display"

    df.to_csv(VAT_FP, index=False)
    print(f"[OK] patched: {VAT_FP}")

def patch_pos() -> None:
    df = pd.read_csv(POS_FP, dtype=str).fillna("")

    mask = (
        df["treatment_category"].astype(str).eq("wrap") &
        df["market_check_status"].astype(str).eq("above_market_median")
    )

    if mask.any():
        df.loc[mask, "launch_recommendation_flag"] = "review_before_launch"
        df.loc[mask, "execution_priority"] = "high"
        df.loc[mask, "display_rounding_method"] = "manual_review_hold"
        df.loc[mask, "display_price_rule"] = "manual_review_before_display"
        df.loc[mask, "display_status"] = "hold_for_review"

    df.to_csv(POS_FP, index=False)
    print(f"[OK] patched: {POS_FP}")

if __name__ == "__main__":
    patch_master()
    patch_vat()
    patch_pos()
