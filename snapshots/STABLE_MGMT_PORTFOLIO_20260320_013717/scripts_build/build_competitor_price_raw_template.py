from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path.cwd()
OUT_DIR = PROJECT_ROOT / "data_processed" / "pricing_research"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_FP = OUT_DIR / "competitor_price_raw.csv"

starter_rows = [
    ("CPRAW_001", "aromatherapy", "standard", 60),
    ("CPRAW_002", "body_treatment", "standard", 60),
    ("CPRAW_003", "deep_tissue", "standard", 60),
    ("CPRAW_004", "facial", "basic", 60),
    ("CPRAW_005", "hot_stone", "standard", 75),
    ("CPRAW_006", "massage", "standard", 60),
    ("CPRAW_007", "reflexology", "standard", 45),
    ("CPRAW_008", "scrub", "standard", 45),
    ("CPRAW_009", "swedish", "standard", 60),
    ("CPRAW_010", "wrap", "standard", 60),
]

rows = []
for rid, cat, var, dur in starter_rows:
    rows.append({
        "raw_competitor_row_id": rid,
        "competitor_name": "",
        "outlet_name": "",
        "location_text": "",
        "treatment_name_raw": "",
        "treatment_category_raw": cat,
        "treatment_variant_raw": var,
        "session_duration_min": dur,
        "listed_price_eur": "",
        "currency": "EUR",
        "price_type": "listed_public_price",
        "source_name": "",
        "source_url": "",
        "capture_date": "",
        "capture_method": "manual_or_scrape",
        "benchmark_include_flag": "review_pending",
        "notes": "",
    })

df = pd.DataFrame(rows)
df.to_csv(OUTPUT_FP, index=False)

print(f"[OK] saved: {OUTPUT_FP}")
print(df.to_string(index=False))
