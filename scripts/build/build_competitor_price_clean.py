from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path.cwd()
OUT_DIR = PROJECT_ROOT / "data_processed" / "pricing_research"
OUT_DIR.mkdir(parents=True, exist_ok=True)

INPUT_FP = OUT_DIR / "competitor_price_raw.csv"
OUTPUT_FP = OUT_DIR / "competitor_price_clean.csv"

CATEGORY_MAP = {
    "aromatherapy": "aromatherapy",
    "body treatment": "body_treatment",
    "body_treatment": "body_treatment",
    "deep tissue": "deep_tissue",
    "deep_tissue": "deep_tissue",
    "facial": "facial",
    "hot stone": "hot_stone",
    "hot_stone": "hot_stone",
    "massage": "massage",
    "reflexology": "reflexology",
    "scrub": "scrub",
    "swedish": "swedish",
    "wrap": "wrap",
}

VARIANT_MAP = {
    "": "standard",
    "standard": "standard",
    "basic": "basic",
    "classic": "standard",
    "signature": "signature",
    "premium": "premium",
}

TARGET_DURATION_MAP = {
    ("aromatherapy", "standard"): 60,
    ("body_treatment", "standard"): 60,
    ("deep_tissue", "standard"): 60,
    ("facial", "basic"): 60,
    ("hot_stone", "standard"): 75,
    ("massage", "standard"): 60,
    ("reflexology", "standard"): 45,
    ("scrub", "standard"): 45,
    ("swedish", "standard"): 60,
    ("wrap", "standard"): 60,
}

def normalize_text(x):
    if pd.isna(x):
        return ""
    return str(x).strip()

def normalize_competitor_id(name: str, idx: int) -> str:
    return f"CMP_{idx+1:03d}"

def normalize_category(x):
    val = normalize_text(x).lower().replace("-", " ").replace("_", " ")
    return CATEGORY_MAP.get(val, normalize_text(x).lower().replace(" ", "_"))

def normalize_variant(x):
    val = normalize_text(x).lower()
    return VARIANT_MAP.get(val, val if val else "standard")

def duration_match_type(category, variant, duration):
    target = TARGET_DURATION_MAP.get((category, variant))
    if target is None or pd.isna(duration):
        return "unknown_target"
    diff = abs(int(duration) - int(target))
    if diff == 0:
        return "exact_match"
    if diff <= 15:
        return "near_match_duration"
    return "off_target_duration"

def decide_include_flag(row):
    cat = normalize_text(row.get("treatment_category", ""))
    var = normalize_text(row.get("treatment_variant", ""))
    dur = pd.to_numeric(row.get("session_duration_min"), errors="coerce")
    price = pd.to_numeric(row.get("listed_price_eur"), errors="coerce")

    if not cat or pd.isna(dur) or pd.isna(price):
        return "exclude", "missing_core_fields", "unknown_target"
    if dur <= 0:
        return "exclude", "invalid_duration", "unknown_target"
    if price <= 0:
        return "exclude", "invalid_price", "unknown_target"

    match_type = duration_match_type(cat, var, int(dur))

    if match_type == "off_target_duration":
        return "exclude", "off_target_duration", match_type

    return "include", "", match_type

def load_input():
    if not INPUT_FP.exists():
        raise FileNotFoundError(f"Required file not found: {INPUT_FP}")

    df = pd.read_csv(INPUT_FP)
    print(f"[INFO] competitor raw input found: {INPUT_FP.name} | rows={len(df)}")

    required = {
        "raw_competitor_row_id",
        "competitor_name",
        "treatment_name_raw",
        "treatment_category_raw",
        "treatment_variant_raw",
        "session_duration_min",
        "listed_price_eur",
        "currency",
        "price_type",
        "source_url",
        "capture_date",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in raw input: {sorted(missing)}")

    return df.reset_index(drop=True)

def build_output():
    df = load_input()

    rows = []
    for i, r in df.iterrows():
        competitor_name = normalize_text(r.get("competitor_name", ""))
        outlet_name = normalize_text(r.get("outlet_name", ""))
        location_text = normalize_text(r.get("location_text", ""))
        treatment_name_raw = normalize_text(r.get("treatment_name_raw", ""))
        treatment_category = normalize_category(r.get("treatment_category_raw", ""))
        treatment_variant = normalize_variant(r.get("treatment_variant_raw", ""))
        duration = pd.to_numeric(r.get("session_duration_min"), errors="coerce")
        listed_price = pd.to_numeric(r.get("listed_price_eur"), errors="coerce")

        include_flag, exclusion_reason, match_type = decide_include_flag({
            "treatment_category": treatment_category,
            "treatment_variant": treatment_variant,
            "session_duration_min": duration,
            "listed_price_eur": listed_price,
        })

        target_duration = TARGET_DURATION_MAP.get((treatment_category, treatment_variant))

        rows.append(
            {
                "competitor_clean_id": f"CPCLEAN_{i+1:03d}",
                "variable_block_id": "V2BL_FIX_003",
                "clean_family": "competitor_price_clean",
                "raw_competitor_row_id": normalize_text(r.get("raw_competitor_row_id", "")),
                "competitor_id": normalize_competitor_id(competitor_name, i),
                "competitor_name": competitor_name,
                "outlet_name": outlet_name,
                "location_text": location_text,
                "treatment_name_raw": treatment_name_raw,
                "treatment_category": treatment_category,
                "treatment_variant": treatment_variant,
                "session_duration_min": int(duration) if pd.notna(duration) else None,
                "target_duration_min": int(target_duration) if target_duration is not None else None,
                "duration_match_type": match_type,
                "listed_price_eur": round(float(listed_price), 2) if pd.notna(listed_price) else None,
                "currency": normalize_text(r.get("currency", "")),
                "price_type": normalize_text(r.get("price_type", "")),
                "source_name": normalize_text(r.get("source_name", "")),
                "source_url": normalize_text(r.get("source_url", "")),
                "capture_date": normalize_text(r.get("capture_date", "")),
                "capture_method": normalize_text(r.get("capture_method", "")),
                "benchmark_include_flag_raw": normalize_text(r.get("benchmark_include_flag", "")),
                "benchmark_include_flag_final": include_flag,
                "exclusion_reason": exclusion_reason,
                "notes": normalize_text(r.get("notes", "")),
                "market_context": "Malta",
                "status": "clean_defined",
                "audit_note": "competitor row normalized from raw capture table with duration match classification",
            }
        )

    out = pd.DataFrame(rows).sort_values(
        ["treatment_category", "treatment_variant", "session_duration_min", "competitor_name"],
        na_position="last"
    ).reset_index(drop=True)

    return out

def main():
    out = build_output()
    out.to_csv(OUTPUT_FP, index=False)

    print(f"[OK] saved: {OUTPUT_FP}")
    print(f"[OK] rows: {len(out)}")
    print("\nPreview:")
    print(out.head(20).to_string(index=False))

if __name__ == "__main__":
    main()
