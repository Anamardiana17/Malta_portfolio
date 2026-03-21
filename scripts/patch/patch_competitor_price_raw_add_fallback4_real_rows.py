from __future__ import annotations

import pandas as pd
from pathlib import Path

CANDIDATE_PATHS = [
    Path("competitor_price_raw.csv"),
    Path("data_processed/pricing_research/competitor_price_raw.csv"),
    Path("data_raw/pricing_research/competitor_price_raw.csv"),
]

def find_input_file() -> Path:
    for p in CANDIDATE_PATHS:
        if p.exists():
            return p
    raise FileNotFoundError(
        "competitor_price_raw.csv not found. Checked:\n- "
        + "\n- ".join(str(p) for p in CANDIDATE_PATHS)
    )

def norm_text(x: object) -> str:
    if pd.isna(x):
        return ""
    return str(x).strip()

def pick_existing_cols(row_payload: dict, existing_cols: list[str]) -> dict:
    out = {c: "" for c in existing_cols}
    for k, v in row_payload.items():
        if k in out:
            out[k] = v
    return out

def build_rows() -> list[dict]:
    return [
        {
            "competitor_name": "Nataraya Day Spa & Wellness",
            "outlet_name": "Nataraya Day Spa & Wellness",
            "location_text": "Pergola Hotel & Spa, Mellieha, Malta",
            "treatment_name_raw": "Traditional Swedish Full Body Massage",
            "treatment_category": "swedish",
            "treatment_variant": "standard",
            "session_duration_min": 60,
            "target_duration_min": 60,
            "duration_match_type": "exact",
            "listed_price_eur": 69.50,
            "currency": "EUR",
            "price_type": "listed",
            "source_name": "nataraya_spa_brochure_2025",
            "source_url": "https://api.g3.com.mt/media/0xdmtg1c/nataraya-spa-brochure-2025.pdf",
            "capture_date": "2026-03-18",
            "capture_method": "manual_web_research",
            "benchmark_include_flag_raw": "include",
            "benchmark_note": "exact duration match; Swedish competitor added to replace fallback_clean",
        },
        {
            "competitor_name": "Nataraya Day Spa & Wellness",
            "outlet_name": "Nataraya Day Spa & Wellness",
            "location_text": "Pergola Hotel & Spa, Mellieha, Malta",
            "treatment_name_raw": "Aromatherapy Full Body Massage",
            "treatment_category": "aromatherapy",
            "treatment_variant": "standard",
            "session_duration_min": 60,
            "target_duration_min": 60,
            "duration_match_type": "exact",
            "listed_price_eur": 76.00,
            "currency": "EUR",
            "price_type": "listed",
            "source_name": "nataraya_spa_brochure_2025",
            "source_url": "https://api.g3.com.mt/media/0xdmtg1c/nataraya-spa-brochure-2025.pdf",
            "capture_date": "2026-03-18",
            "capture_method": "manual_web_research",
            "benchmark_include_flag_raw": "include",
            "benchmark_note": "exact duration match; aromatherapy competitor added to replace fallback_clean",
        },
        {
            "competitor_name": "Apollo Day Spa",
            "outlet_name": "Apollo Day Spa",
            "location_text": "Corinthia St George's Bay, St Julian's, Malta",
            "treatment_name_raw": "Hydrating Chocolate & Honey Body Mask",
            "treatment_category": "body_treatment",
            "treatment_variant": "standard",
            "session_duration_min": 45,
            "target_duration_min": 45,
            "duration_match_type": "exact",
            "listed_price_eur": 60.00,
            "currency": "EUR",
            "price_type": "listed",
            "source_name": "apollo_day_spa_body_wraps_scrubs",
            "source_url": "https://www.corinthia.com/en-gb/st-georges-bay/spa-and-leisure/apollo-by-dee-spas/body-wraps-scrubs/",
            "capture_date": "2026-03-18",
            "capture_method": "manual_web_research",
            "benchmark_include_flag_raw": "include",
            "benchmark_note": "exact duration match; mapped as generic body_treatment competitor",
        },
        {
            "competitor_name": "Apollo Day Spa",
            "outlet_name": "Apollo Day Spa",
            "location_text": "Corinthia St George's Bay, St Julian's, Malta",
            "treatment_name_raw": "Detoxifying Eucalyptus & Seaweed Wrap",
            "treatment_category": "wrap",
            "treatment_variant": "standard",
            "session_duration_min": 45,
            "target_duration_min": 45,
            "duration_match_type": "exact",
            "listed_price_eur": 60.00,
            "currency": "EUR",
            "price_type": "listed",
            "source_name": "apollo_day_spa_body_wraps_scrubs",
            "source_url": "https://www.corinthia.com/en-gb/st-georges-bay/spa-and-leisure/apollo-by-dee-spas/body-wraps-scrubs/",
            "capture_date": "2026-03-18",
            "capture_method": "manual_web_research",
            "benchmark_include_flag_raw": "include",
            "benchmark_note": "exact duration match; wrap competitor added to replace fallback_clean",
        },
        {
            "competitor_name": "The Phoenicia Malta Spa & Wellness",
            "outlet_name": "The Phoenicia Malta Spa & Wellness",
            "location_text": "Valletta, Malta",
            "treatment_name_raw": "Detox Body Wrap",
            "treatment_category": "wrap",
            "treatment_variant": "standard",
            "session_duration_min": 45,
            "target_duration_min": 45,
            "duration_match_type": "exact",
            "listed_price_eur": 65.00,
            "currency": "EUR",
            "price_type": "listed",
            "source_name": "phoenicia_malta_spa_treatments",
            "source_url": "https://phoeniciamalta.com/spa-wellness/spa-treatments/",
            "capture_date": "2026-03-18",
            "capture_method": "manual_web_research",
            "benchmark_include_flag_raw": "include",
            "benchmark_note": "exact duration match; second real wrap benchmark for robustness",
        },
    ]

def dedupe_key(df: pd.DataFrame) -> pd.Series:
    def get(col: str) -> pd.Series:
        return df[col].map(norm_text) if col in df.columns else pd.Series([""] * len(df))
    return (
        get("competitor_name").str.lower() + "||" +
        get("treatment_name_raw").str.lower() + "||" +
        get("treatment_category").str.lower() + "||" +
        get("session_duration_min").str.lower() + "||" +
        get("listed_price_eur").str.lower() + "||" +
        get("source_url").str.lower()
    )

def main() -> None:
    fp = find_input_file()
    df = pd.read_csv(fp, dtype=str).fillna("")
    existing_cols = list(df.columns)

    candidate_rows = pd.DataFrame(
        [pick_existing_cols(r, existing_cols) for r in build_rows()],
        columns=existing_cols,
    ).fillna("")

    before = len(df)
    combined = pd.concat([df, candidate_rows], ignore_index=True)

    combined["_dedupe_key"] = dedupe_key(combined)
    combined = combined.drop_duplicates(subset=["_dedupe_key"], keep="first").drop(columns=["_dedupe_key"])

    added = len(combined) - before
    combined.to_csv(fp, index=False)

    print(f"[OK] patched: {fp}")
    print(f"[OK] rows before: {before}")
    print(f"[OK] rows after : {len(combined)}")
    print(f"[OK] rows added : {added}")

    preview_cols = [c for c in [
        "competitor_name",
        "outlet_name",
        "treatment_name_raw",
        "treatment_category",
        "session_duration_min",
        "listed_price_eur",
        "source_name",
    ] if c in combined.columns]
    print("\n[PREVIEW] newly targeted categories:")
    if preview_cols:
        mask = combined["treatment_category"].astype(str).isin(["aromatherapy", "body_treatment", "swedish", "wrap"])
        print(combined.loc[mask, preview_cols].tail(20).to_string(index=False))
    else:
        print("preview columns not found in current schema; patch still saved successfully")

if __name__ == "__main__":
    main()
