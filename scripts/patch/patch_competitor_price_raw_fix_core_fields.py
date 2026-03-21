from __future__ import annotations

import pandas as pd
from pathlib import Path

FP = Path("data_processed/pricing_research/competitor_price_raw.csv")

def pick_col(cols: list[str], candidates: list[str], required: bool = True) -> str | None:
    lower_map = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand.lower() in lower_map:
            return lower_map[cand.lower()]
    if required:
        raise KeyError(f"Missing required column. Tried: {candidates}")
    return None

def main() -> None:
    df = pd.read_csv(FP, dtype=str).fillna("")
    cols = list(df.columns)

    col_treatment_name = pick_col(cols, ["treatment_name_raw"])
    col_source_name = pick_col(cols, ["source_name"])

    col_treatment_category = pick_col(cols, ["treatment_category"], required=False)
    col_treatment_variant = pick_col(cols, ["treatment_variant"], required=False)
    col_session_duration = pick_col(cols, ["session_duration_min"], required=False)
    col_target_duration = pick_col(cols, ["target_duration_min"], required=False)
    col_duration_match = pick_col(cols, ["duration_match_type"], required=False)
    col_benchmark_flag = pick_col(cols, ["benchmark_include_flag_raw"], required=False)
    col_price_type = pick_col(cols, ["price_type"], required=False)
    col_capture_method = pick_col(cols, ["capture_method"], required=False)
    col_notes = pick_col(cols, ["notes", "benchmark_note"], required=False)

    updates = [
        {
            "source_name": "nataraya_spa_brochure_2025",
            "treatment_name_raw": "Traditional Swedish Full Body Massage",
            "treatment_category": "swedish",
            "treatment_variant": "standard",
            "session_duration_min": "60",
            "target_duration_min": "60",
            "duration_match_type": "exact_match",
            "benchmark_include_flag_raw": "review_pending",
            "price_type": "listed_public_price",
            "capture_method": "manual_capture",
            "notes": "real public price observed in spa brochure PDF; exact match Swedish competitor",
        },
        {
            "source_name": "nataraya_spa_brochure_2025",
            "treatment_name_raw": "Aromatherapy Full Body Massage",
            "treatment_category": "aromatherapy",
            "treatment_variant": "standard",
            "session_duration_min": "60",
            "target_duration_min": "60",
            "duration_match_type": "exact_match",
            "benchmark_include_flag_raw": "review_pending",
            "price_type": "listed_public_price",
            "capture_method": "manual_capture",
            "notes": "real public price observed in spa brochure PDF; exact match aromatherapy competitor",
        },
        {
            "source_name": "apollo_day_spa_body_wraps_scrubs",
            "treatment_name_raw": "Hydrating Chocolate & Honey Body Mask",
            "treatment_category": "body_treatment",
            "treatment_variant": "standard",
            "session_duration_min": "45",
            "target_duration_min": "60",
            "duration_match_type": "near_match_duration",
            "benchmark_include_flag_raw": "review_pending",
            "price_type": "listed_public_price",
            "capture_method": "manual_capture",
            "notes": "real public price observed on website; mapped to body_treatment with near-match duration vs current 60-min target",
        },
        {
            "source_name": "apollo_day_spa_body_wraps_scrubs",
            "treatment_name_raw": "Detoxifying Eucalyptus & Seaweed Wrap",
            "treatment_category": "wrap",
            "treatment_variant": "standard",
            "session_duration_min": "45",
            "target_duration_min": "60",
            "duration_match_type": "near_match_duration",
            "benchmark_include_flag_raw": "review_pending",
            "price_type": "listed_public_price",
            "capture_method": "manual_capture",
            "notes": "real public price observed on website; mapped to wrap with near-match duration vs current 60-min target",
        },
        {
            "source_name": "phoenicia_malta_spa_treatments",
            "treatment_name_raw": "Detox Body Wrap",
            "treatment_category": "wrap",
            "treatment_variant": "standard",
            "session_duration_min": "45",
            "target_duration_min": "60",
            "duration_match_type": "near_match_duration",
            "benchmark_include_flag_raw": "review_pending",
            "price_type": "listed_public_price",
            "capture_method": "manual_capture",
            "notes": "real public price observed on website; second wrap benchmark with near-match duration vs current 60-min target",
        },
    ]

    touched = 0

    for u in updates:
        mask = (
            df[col_source_name].astype(str).str.strip().eq(u["source_name"]) &
            df[col_treatment_name].astype(str).str.strip().eq(u["treatment_name_raw"])
        )
        idx = df.index[mask]
        if len(idx) == 0:
            print(f"[WARN] row not found: {u['source_name']} | {u['treatment_name_raw']}")
            continue

        for i in idx:
            if col_treatment_category:
                df.at[i, col_treatment_category] = u["treatment_category"]
            if col_treatment_variant:
                df.at[i, col_treatment_variant] = u["treatment_variant"]
            if col_session_duration:
                df.at[i, col_session_duration] = u["session_duration_min"]
            if col_target_duration:
                df.at[i, col_target_duration] = u["target_duration_min"]
            if col_duration_match:
                df.at[i, col_duration_match] = u["duration_match_type"]
            if col_benchmark_flag:
                df.at[i, col_benchmark_flag] = u["benchmark_include_flag_raw"]
            if col_price_type:
                df.at[i, col_price_type] = u["price_type"]
            if col_capture_method:
                df.at[i, col_capture_method] = u["capture_method"]
            if col_notes:
                df.at[i, col_notes] = u["notes"]
            touched += 1

    df.to_csv(FP, index=False)

    print(f"[OK] patched raw file: {FP}")
    print(f"[OK] rows touched: {touched}")

    preview_cols = [c for c in [
        col_source_name,
        col_treatment_name,
        col_treatment_category,
        col_treatment_variant,
        col_session_duration,
        col_target_duration,
        col_duration_match,
        col_benchmark_flag,
        col_price_type,
    ] if c]

    print("\n[PREVIEW]")
    mask_preview = df[col_source_name].isin([
        "nataraya_spa_brochure_2025",
        "apollo_day_spa_body_wraps_scrubs",
        "phoenicia_malta_spa_treatments",
    ])
    print(df.loc[mask_preview, preview_cols].to_string(index=False))
