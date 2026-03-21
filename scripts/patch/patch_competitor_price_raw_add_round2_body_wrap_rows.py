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

    new_rows = [
        {
            "raw_competitor_row_id": "CPRAW_014",
            "competitor_name": "Nataraya Day Spa & Wellness",
            "outlet_name": "Nataraya Day Spa & Wellness",
            "location_text": "Pergola Hotel & Spa, Mellieha, Malta",
            "treatment_name_raw": "Full Body Scrub",
            "treatment_category_raw": "body_treatment",
            "treatment_variant_raw": "standard",
            "session_duration_min": "40",
            "listed_price_eur": "59.0",
            "currency": "EUR",
            "price_type": "listed_public_price",
            "source_name": "nataraya_spa_brochure_mar2024",
            "source_url": "https://api.g3.com.mt/media/sxbc1gdd/nataraya-spa-brochure-mar2024.pdf",
            "capture_date": "2026-03-18",
            "capture_method": "manual_capture",
            "benchmark_include_flag": "review_pending",
            "notes": "real public price observed in spa brochure PDF; body_treatment near-match candidate vs 60-min target",
        },
        {
            "raw_competitor_row_id": "CPRAW_015",
            "competitor_name": "Nataraya Day Spa & Wellness",
            "outlet_name": "Nataraya Day Spa & Wellness",
            "location_text": "Pergola Hotel & Spa, Mellieha, Malta",
            "treatment_name_raw": "Signature Tailor-Made Full Body Wrap",
            "treatment_category_raw": "wrap",
            "treatment_variant_raw": "standard",
            "session_duration_min": "60",
            "listed_price_eur": "72.0",
            "currency": "EUR",
            "price_type": "listed_public_price",
            "source_name": "nataraya_spa_brochure_mar2024",
            "source_url": "https://api.g3.com.mt/media/sxbc1gdd/nataraya-spa-brochure-mar2024.pdf",
            "capture_date": "2026-03-18",
            "capture_method": "manual_capture",
            "benchmark_include_flag": "review_pending",
            "notes": "real public price observed in spa brochure PDF; exact wrap competitor",
        },
        {
            "raw_competitor_row_id": "CPRAW_016",
            "competitor_name": "Excelsior Spa & Fitness Centre",
            "outlet_name": "Excelsior Hotel",
            "location_text": "Floriana, Malta",
            "treatment_name_raw": "Full Body Exfoliation with Back Massage",
            "treatment_category_raw": "body_treatment",
            "treatment_variant_raw": "standard",
            "session_duration_min": "60",
            "listed_price_eur": "100.0",
            "currency": "EUR",
            "price_type": "listed_public_price",
            "source_name": "excelsior_spa_menu",
            "source_url": "https://excelsior.com.mt/wp-content/uploads/2024/03/spa-treatment-menu-digital.pdf",
            "capture_date": "2026-03-18",
            "capture_method": "manual_capture",
            "benchmark_include_flag": "review_pending",
            "notes": "real public price observed in spa menu PDF; exact body_treatment competitor",
        },
        {
            "raw_competitor_row_id": "CPRAW_017",
            "competitor_name": "1926 Le Soleil Spa",
            "outlet_name": "1926 Le Soleil Spa",
            "location_text": "Sliema, Malta",
            "treatment_name_raw": "The 1926 Polish & Wrap",
            "treatment_category_raw": "wrap",
            "treatment_variant_raw": "standard",
            "session_duration_min": "50",
            "listed_price_eur": "100.0",
            "currency": "EUR",
            "price_type": "listed_public_price",
            "source_name": "1926_le_soleil_spa_menu_2024",
            "source_url": "https://1926lesoleil.com/wp-content/uploads/2024/04/Spa-treatment-menu-1926-Collection-compressed-1.pdf",
            "capture_date": "2026-03-18",
            "capture_method": "manual_capture",
            "benchmark_include_flag": "review_pending",
            "notes": "real public price observed in spa menu PDF; wrap near-match candidate vs 60-min target",
        },
        {
            "raw_competitor_row_id": "CPRAW_018",
            "competitor_name": "Pearl Spas",
            "outlet_name": "Pearl Spas",
            "location_text": "Qawra / St Paul's Bay, Malta",
            "treatment_name_raw": "Chocolate Full-Body Wrap",
            "treatment_category_raw": "wrap",
            "treatment_variant_raw": "standard",
            "session_duration_min": "45",
            "listed_price_eur": "60.0",
            "currency": "EUR",
            "price_type": "listed_public_price",
            "source_name": "pearl_spas_treatment_menu_2025",
            "source_url": "https://pearlspas.com/wp-content/uploads/2025/02/0313-EF37-Pearl-Spas-Deliverables-02-Treatment-Menu-square-4-web-6.pdf",
            "capture_date": "2026-03-18",
            "capture_method": "manual_capture",
            "benchmark_include_flag": "review_pending",
            "notes": "real public price observed in spa menu PDF; wrap near-match candidate vs 60-min target",
        },
    ]

    dedupe_cols = ["source_name", "treatment_name_raw"]
    before = len(df)

    add_df = pd.DataFrame(new_rows, columns=df.columns).fillna("")
    existing_keys = set(
        tuple(x) for x in df[dedupe_cols].astype(str).fillna("").to_numpy().tolist()
    )

    rows_to_add = []
    for _, row in add_df.iterrows():
        key = tuple(str(row[c]) for c in dedupe_cols)
        if key not in existing_keys:
            rows_to_add.append(row.to_dict())

    if rows_to_add:
        df = pd.concat([df, pd.DataFrame(rows_to_add, columns=df.columns)], ignore_index=True)

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
    print(df[df["raw_competitor_row_id"].isin(["CPRAW_014","CPRAW_015","CPRAW_016","CPRAW_017","CPRAW_018"])][preview_cols].to_string(index=False))

if __name__ == "__main__":
    main()
