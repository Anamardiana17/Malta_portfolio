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

    def update_row(source_name: str, treatment_name_raw: str, payload: dict) -> None:
        mask = (
            df["source_name"].astype(str).str.strip().eq(source_name) &
            df["treatment_name_raw"].astype(str).str.strip().eq(treatment_name_raw)
        )
        idx = df.index[mask]
        if len(idx) == 0:
            raise ValueError(f"Row not found for source_name={source_name} | treatment_name_raw={treatment_name_raw}")

        for i in idx:
            for k, v in payload.items():
                if k in df.columns:
                    df.at[i, k] = v

    updates = [
        (
            "nataraya_spa_brochure_2025",
            "Traditional Swedish Full Body Massage",
            {
                "raw_competitor_row_id": "CPRAW_009",
                "treatment_category_raw": "swedish",
                "treatment_variant_raw": "standard",
                "session_duration_min": "60",
                "listed_price_eur": "69.5",
                "currency": "EUR",
                "price_type": "listed_public_price",
                "capture_method": "manual_capture",
                "benchmark_include_flag": "review_pending",
                "notes": "real public price observed in spa brochure PDF; exact Swedish competitor",
            },
        ),
        (
            "nataraya_spa_brochure_2025",
            "Aromatherapy Full Body Massage",
            {
                "raw_competitor_row_id": "CPRAW_010",
                "treatment_category_raw": "aromatherapy",
                "treatment_variant_raw": "standard",
                "session_duration_min": "60",
                "listed_price_eur": "76.0",
                "currency": "EUR",
                "price_type": "listed_public_price",
                "capture_method": "manual_capture",
                "benchmark_include_flag": "review_pending",
                "notes": "real public price observed in spa brochure PDF; exact aromatherapy competitor",
            },
        ),
        (
            "apollo_day_spa_body_wraps_scrubs",
            "Hydrating Chocolate & Honey Body Mask",
            {
                "raw_competitor_row_id": "CPRAW_011",
                "treatment_category_raw": "body_treatment",
                "treatment_variant_raw": "standard",
                "session_duration_min": "45",
                "listed_price_eur": "60.0",
                "currency": "EUR",
                "price_type": "listed_public_price",
                "capture_method": "manual_capture",
                "benchmark_include_flag": "review_pending",
                "notes": "real public price observed on website; mapped to body_treatment near-match candidate vs 60-min target",
            },
        ),
        (
            "apollo_day_spa_body_wraps_scrubs",
            "Detoxifying Eucalyptus & Seaweed Wrap",
            {
                "raw_competitor_row_id": "CPRAW_012",
                "treatment_category_raw": "wrap",
                "treatment_variant_raw": "standard",
                "session_duration_min": "45",
                "listed_price_eur": "60.0",
                "currency": "EUR",
                "price_type": "listed_public_price",
                "capture_method": "manual_capture",
                "benchmark_include_flag": "review_pending",
                "notes": "real public price observed on website; wrap near-match candidate vs 60-min target",
            },
        ),
        (
            "phoenicia_malta_spa_treatments",
            "Detox Body Wrap",
            {
                "raw_competitor_row_id": "CPRAW_013",
                "treatment_category_raw": "wrap",
                "treatment_variant_raw": "standard",
                "session_duration_min": "45",
                "listed_price_eur": "65.0",
                "currency": "EUR",
                "price_type": "listed_public_price",
                "capture_method": "manual_capture",
                "benchmark_include_flag": "review_pending",
                "notes": "real public price observed on website; second wrap near-match candidate vs 60-min target",
            },
        ),
    ]

    for source_name, treatment_name_raw, payload in updates:
        update_row(source_name, treatment_name_raw, payload)

    df.to_csv(FP, index=False)

    print(f"[OK] patched raw file: {FP}")
    print("\n[PREVIEW]")
    mask = df["source_name"].isin([
        "nataraya_spa_brochure_2025",
        "apollo_day_spa_body_wraps_scrubs",
        "phoenicia_malta_spa_treatments",
    ])
    cols = [
        "raw_competitor_row_id",
        "source_name",
        "treatment_name_raw",
        "treatment_category_raw",
        "treatment_variant_raw",
        "session_duration_min",
        "listed_price_eur",
        "price_type",
        "benchmark_include_flag",
    ]
    print(df.loc[mask, cols].to_string(index=False))

if __name__ == "__main__":
    main()
