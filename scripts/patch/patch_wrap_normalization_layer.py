from __future__ import annotations

import pandas as pd
from pathlib import Path

CLEAN_FP = Path("data_processed/pricing_research/competitor_price_clean.csv")
MV_FP = Path("data_processed/pricing_research/treatment_market_validation_sheet.csv")
MASTER_FP = Path("data_processed/pricing_research/final_treatment_pricing_master.csv")
VAT_FP = Path("data_processed/pricing_research/final_treatment_pricing_master_with_vat.csv")
POS_FP = Path("data_processed/pricing_research/treatment_pos_display_price_sheet.csv")
GEO_FP = Path("data_processed/pricing_research/treatment_geopolitical_stress_test_sheet.csv")

WRAP_NORMALIZABLE_ROWS = {
    "CPRAW_017",  # 1926 Polish & Wrap | 50 min
}

def ensure_col(df: pd.DataFrame, col: str, default: str = "") -> pd.DataFrame:
    if col not in df.columns:
        df[col] = default
    return df

def patch_clean() -> None:
    df = pd.read_csv(CLEAN_FP, dtype=str).fillna("")

    for col in [
        "operational_duration_min",
        "commercial_duration_match_type",
        "duration_normalization_note",
        "commercial_include_flag",
    ]:
        df = ensure_col(df, col, "")

    wrap_mask = df["treatment_category"].astype(str).eq("wrap")

    # default pass-through
    df.loc[wrap_mask, "operational_duration_min"] = df.loc[wrap_mask, "session_duration_min"].astype(str)
    df.loc[wrap_mask, "commercial_duration_match_type"] = df.loc[wrap_mask, "duration_match_type"].astype(str)
    df.loc[wrap_mask, "duration_normalization_note"] = ""
    df.loc[wrap_mask, "commercial_include_flag"] = df.loc[wrap_mask, "benchmark_include_flag_final"].astype(str)

    # normalized subset: 50-min wrap can map to operational 60-min slot
    mask_norm = (
        df["raw_competitor_row_id"].astype(str).isin(WRAP_NORMALIZABLE_ROWS) &
        wrap_mask &
        df["session_duration_min"].astype(str).eq("50") &
        df["target_duration_min"].astype(str).eq("60")
    )

    if mask_norm.any():
        df.loc[mask_norm, "operational_duration_min"] = "60"
        df.loc[mask_norm, "commercial_duration_match_type"] = "normalized_to_60_with_setup_buffer"
        df.loc[mask_norm, "duration_normalization_note"] = (
            "public treatment duration is 50 min; commercially normalized to a 60-min appointment block using 10-min setup/turnover buffer"
        )
        df.loc[mask_norm, "commercial_include_flag"] = "include"

    df.to_csv(CLEAN_FP, index=False)
    print(f"[OK] patched: {CLEAN_FP}")

def compute_wrap_commercial_overlay(clean_df: pd.DataFrame) -> dict[str, str]:
    mask = (
        clean_df["treatment_category"].astype(str).eq("wrap") &
        clean_df["commercial_include_flag"].astype(str).eq("include") &
        clean_df["operational_duration_min"].astype(str).eq("60")
    )

    sub = clean_df.loc[mask].copy()
    if sub.empty:
        return {
            "commercial_market_price_low_eur": "",
            "commercial_market_price_median_eur": "",
            "commercial_market_price_high_eur": "",
            "commercial_sample_size": "",
            "commercial_duration_basis": "",
            "commercial_overlay_note": "",
        }

    prices = pd.to_numeric(sub["listed_price_eur"], errors="coerce").dropna().sort_values()
    if prices.empty:
        return {
            "commercial_market_price_low_eur": "",
            "commercial_market_price_median_eur": "",
            "commercial_market_price_high_eur": "",
            "commercial_sample_size": "",
            "commercial_duration_basis": "",
            "commercial_overlay_note": "",
        }

    return {
        "commercial_market_price_low_eur": f"{prices.min():.1f}",
        "commercial_market_price_median_eur": f"{prices.median():.1f}",
        "commercial_market_price_high_eur": f"{prices.max():.1f}",
        "commercial_sample_size": str(len(prices)),
        "commercial_duration_basis": "exact_60_plus_normalized_50_to_60",
        "commercial_overlay_note": (
            "commercial wrap overlay combines exact 60-min market rows and selected 50-min rows normalized to a 60-min operational slot with setup buffer"
        ),
    }

def patch_market_validation() -> None:
    clean = pd.read_csv(CLEAN_FP, dtype=str).fillna("")
    df = pd.read_csv(MV_FP, dtype=str).fillna("")

    for col in [
        "commercial_market_price_low_eur",
        "commercial_market_price_median_eur",
        "commercial_market_price_high_eur",
        "commercial_sample_size",
        "commercial_duration_basis",
        "commercial_overlay_note",
        "commercial_gap_vs_market_median_eur",
        "commercial_gap_vs_market_median_pct",
        "commercial_market_check_status",
    ]:
        df = ensure_col(df, col, "")

    overlay = compute_wrap_commercial_overlay(clean)

    mask = df["treatment_category"].astype(str).eq("wrap")
    if mask.any():
        for k, v in overlay.items():
            df.loc[mask, k] = v

        rec = pd.to_numeric(df.loc[mask, "recommended_sell_price_eur"], errors="coerce")
        med = pd.to_numeric(df.loc[mask, "commercial_market_price_median_eur"], errors="coerce")

        gap = rec - med
        gap_pct = (gap / med.replace(0, pd.NA)) * 100

        df.loc[mask, "commercial_gap_vs_market_median_eur"] = gap.round(2).astype(str)
        df.loc[mask, "commercial_gap_vs_market_median_pct"] = gap_pct.round(2).astype(str)

        status = []
        for g in gap_pct.fillna(pd.NA):
            if pd.isna(g):
                status.append("")
            elif g > 10:
                status.append("above_commercial_market_median")
            elif g < -10:
                status.append("below_commercial_market_median")
            else:
                status.append("in_line_with_commercial_market_median")
        df.loc[mask, "commercial_market_check_status"] = status

    df.to_csv(MV_FP, index=False)
    print(f"[OK] patched: {MV_FP}")

def patch_master_family(fp: Path, table_name: str) -> None:
    df = pd.read_csv(fp, dtype=str).fillna("")
    mv = pd.read_csv(MV_FP, dtype=str).fillna("")

    join_cols = [
        "treatment_category",
        "commercial_market_price_low_eur",
        "commercial_market_price_median_eur",
        "commercial_market_price_high_eur",
        "commercial_sample_size",
        "commercial_duration_basis",
        "commercial_overlay_note",
        "commercial_gap_vs_market_median_eur",
        "commercial_gap_vs_market_median_pct",
        "commercial_market_check_status",
    ]
    mv_sub = mv[join_cols].drop_duplicates(subset=["treatment_category"])

    for col in join_cols[1:]:
        df = ensure_col(df, col, "")

    df = df.drop(columns=[c for c in join_cols[1:] if c in df.columns], errors="ignore").merge(
        mv_sub, on="treatment_category", how="left"
    ).fillna("")

    wrap_mask = df["treatment_category"].astype(str).eq("wrap")
    if wrap_mask.any():
        if table_name == "master":
            df.loc[wrap_mask, "pricing_summary_note"] = (
                "Recommended sell price "
                + df.loc[wrap_mask, "recommended_sell_price_eur"].astype(str)
                + " EUR vs floor "
                + df.loc[wrap_mask, "pricing_floor_mid_eur"].astype(str)
                + " EUR; exact market median remains thin, while commercial 60-min normalized overlay median is "
                + df.loc[wrap_mask, "commercial_market_price_median_eur"].astype(str)
                + " EUR."
            )
            df.loc[wrap_mask, "decision_adjustment_note"] = (
                "keep exact market benchmark unchanged; use commercial overlay for slot-planning context only; retain review_before_launch until independent exact wrap sample expands"
            )
            df.loc[wrap_mask, "price_architecture_flag"] = "premium_edge"
            df.loc[wrap_mask, "launch_recommendation_flag"] = "review_before_launch"
            df.loc[wrap_mask, "execution_priority"] = "high"

        elif table_name == "vat":
            df.loc[wrap_mask, "display_price_rule"] = "manual_review_before_display"
            df.loc[wrap_mask, "launch_recommendation_flag"] = "review_before_launch"
            df.loc[wrap_mask, "execution_priority"] = "high"

        elif table_name == "pos":
            df.loc[wrap_mask, "display_rounding_method"] = "manual_review_hold"
            df.loc[wrap_mask, "display_price_rule"] = "manual_review_before_display"
            df.loc[wrap_mask, "display_status"] = "hold_for_review"
            df.loc[wrap_mask, "launch_recommendation_flag"] = "review_before_launch"
            df.loc[wrap_mask, "execution_priority"] = "high"

        elif table_name == "geo":
            pass

    df.to_csv(fp, index=False)
    print(f"[OK] patched: {fp}")

if __name__ == "__main__":
    patch_clean()
    patch_market_validation()
    patch_master_family(MASTER_FP, "master")
    patch_master_family(VAT_FP, "vat")
    patch_master_family(POS_FP, "pos")
    patch_master_family(GEO_FP, "geo")
