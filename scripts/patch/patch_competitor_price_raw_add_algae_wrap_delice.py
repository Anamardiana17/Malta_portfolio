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
        "raw_competitor_row_id": "CPRAW_020",
        "competitor_name": "Nataraya Day Spa & Wellness",
        "outlet_name": "Nataraya Day Spa & Wellness",
        "location_text": "Pergola Hotel & Spa, Mellieha, Malta",
        "treatment_name_raw": "Algae Wrap Delice",
        "treatment_category_raw": "wrap",
        "treatment_variant_raw": "standard",
        "session_duration_min": "60",
        "listed_price_eur": "82.0",
        "currency": "EUR",
        "price_type": "listed_public_price",
        "source_name": "nataraya_spa_brochure_2025",
        "source_url": "https://api.g3.com.mt/media/0xdmtg1c/nataraya-spa-brochure-2025.pdf",
        "capture_date": "2026-03-18",
        "capture_method": "manual_capture",
        "benchmark_include_flag": "review_pending",
        "notes": "real public price observed in spa brochure PDF; second exact wrap competitor row",
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
    print(df[df["raw_competitor_row_id"].eq("CPRAW_020")][preview_cols].to_string(index=False))

if __name__ == "__main__":
    main()
