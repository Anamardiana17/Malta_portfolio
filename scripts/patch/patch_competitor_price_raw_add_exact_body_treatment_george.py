from __future__ import annotations

import pandas as pd
from pathlib import Path

FP = Path("data_processed/pricing_research/competitor_price_raw.csv")

def main() -> None:
    df = pd.read_csv(FP, dtype=str).fillna("")

    required_cols = [
        "raw_competitor_row_id",
        "competitor_name",
        "outlet_name",
        "location_text",
        "treatment_name_raw",
        "treatment_category_raw",
        "treatment_variant_raw",
        "session_duration_min",
        "listed_price_eur",
        "currency",
        "price_type",
        "source_name",
        "source_url",
        "capture_date",
        "capture_method",
        "benchmark_include_flag",
        "notes",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise KeyError(f"Missing required raw columns: {missing}")

    new_row = {
        "raw_competitor_row_id": "CPRAW_019",
        "competitor_name": "The George Malta Spa",
        "outlet_name": "The George Malta",
        "location_text": "St Julian's, Malta",
        "treatment_name_raw": "Wild Argan Oil Body Scrub",
        "treatment_category_raw": "body_treatment",
        "treatment_variant_raw": "standard",
        "session_duration_min": "60",
        "listed_price_eur": "55.0",
        "currency": "EUR",
        "price_type": "listed_public_price",
        "source_name": "the_george_hotel_spa_menu",
        "source_url": "https://www.thegeorgemalta.com/downloads/The%20George%20Hotel%20Spa.pdf",
        "capture_date": "2026-03-18",
        "capture_method": "manual_capture",
        "benchmark_include_flag": "review_pending",
        "notes": "real public price observed in spa menu PDF; exact body_treatment competitor",
    }

    dedupe = (
        (df["source_name"].astype(str).str.strip() == new_row["source_name"]) &
        (df["treatment_name_raw"].astype(str).str.strip() == new_row["treatment_name_raw"])
    )

    before = len(df)
    if not dedupe.any():
        df = pd.concat([df, pd.DataFrame([new_row], columns=df.columns)], ignore_index=True)

    df.to_csv(FP, index=False)

    print(f"[OK] patched raw file: {FP}")
    print(f"[OK] rows before: {before}")
    print(f"[OK] rows after : {len(df)}")
    print(f"[OK] rows added : {len(df) - before}")

    preview_cols = [
        "raw_competitor_row_id",
        "source_name",
        "treatment_name_raw",
        "treatment_category_raw",
        "session_duration_min",
        "listed_price_eur",
    ]
    print("\n[PREVIEW]")
    print(df[df["source_name"].eq("the_george_hotel_spa_menu")][preview_cols].to_string(index=False))

if __name__ == "__main__":
    main()
