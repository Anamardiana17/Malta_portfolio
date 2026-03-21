from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path.cwd()
OUT_DIR = PROJECT_ROOT / "data_processed" / "pricing_research"
OUT_DIR.mkdir(parents=True, exist_ok=True)

INPUT_FP = OUT_DIR / "price_recommendation_interpretation_v1.csv"
OUTPUT_FP = OUT_DIR / "treatment_market_price_benchmark_clean.csv"

# Purpose:
# - normalize benchmark file into clean join-ready structure
# - create explicit market low / median / high columns
# - keep audit-friendly source notes
# - reduce schema ambiguity in downstream market validation

VARIANT_NORMALIZATION = {
    "standard": "standard",
    "basic": "basic",
    "classic": "standard",
    "signature": "signature",
    "premium": "premium",
}

CATEGORY_NORMALIZATION = {
    "aromatherapy": "aromatherapy",
    "body_treatment": "body_treatment",
    "deep_tissue": "deep_tissue",
    "facial": "facial",
    "hot_stone": "hot_stone",
    "massage": "massage",
    "reflexology": "reflexology",
    "scrub": "scrub",
    "swedish": "swedish",
    "wrap": "wrap",
}


def normalize_text(x):
    if pd.isna(x):
        return ""
    return str(x).strip()


def normalize_variant(x):
    val = normalize_text(x).lower()
    return VARIANT_NORMALIZATION.get(val, val if val else "standard")


def normalize_category(x):
    val = normalize_text(x).lower()
    return CATEGORY_NORMALIZATION.get(val, val)


def round_money(x):
    return round(float(x), 2)


def infer_benchmark_quality(sample_size):
    if pd.isna(sample_size):
        return "medium"
    if sample_size >= 15:
        return "high"
    if sample_size >= 8:
        return "medium"
    return "low"


def load_input():
    if not INPUT_FP.exists():
        raise FileNotFoundError(
            f"Required file not found: {INPUT_FP}\n"
            "Expected existing pricing interpretation source."
        )

    df = pd.read_csv(INPUT_FP)
    print(f"[INFO] benchmark source found: {INPUT_FP.name} | rows={len(df)}")
    print(f"[INFO] raw columns: {list(df.columns)}")

    rename_map = {}
    for c in df.columns:
        cl = c.strip().lower()
        if cl in {"treatment_category", "category", "service_category"}:
            rename_map[c] = "treatment_category"
        elif cl in {"treatment_variant", "variant", "service_variant"}:
            rename_map[c] = "treatment_variant"
        elif cl in {"session_duration_min", "duration_min", "duration_minutes"}:
            rename_map[c] = "session_duration_min"
        elif cl in {"standard_price_low", "standard_price_low_eur"}:
            rename_map[c] = "market_price_low_eur"
        elif cl in {"standard_price_mid", "standard_price_mid_eur"}:
            rename_map[c] = "market_price_median_eur"
        elif cl in {"standard_price_high", "standard_price_high_eur"}:
            rename_map[c] = "market_price_high_eur"
        elif cl in {"recommended_reference_band"}:
            rename_map[c] = "recommended_reference_band"
        elif cl in {"positioning_note", "position_note"}:
            rename_map[c] = "positioning_note"

    df = df.rename(columns=rename_map)

    required = {
        "treatment_category",
        "treatment_variant",
        "session_duration_min",
        "market_price_low_eur",
        "market_price_median_eur",
        "market_price_high_eur",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in source input: {sorted(missing)}")

    for col in [
        "session_duration_min",
        "market_price_low_eur",
        "market_price_median_eur",
        "market_price_high_eur",
    ]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(
        subset=[
            "session_duration_min",
            "market_price_low_eur",
            "market_price_median_eur",
            "market_price_high_eur",
        ]
    ).copy()

    df["treatment_category"] = df["treatment_category"].map(normalize_category)
    df["treatment_variant"] = df["treatment_variant"].map(normalize_variant)
    df["session_duration_min"] = df["session_duration_min"].astype(int)

    if "recommended_reference_band" not in df.columns:
        df["recommended_reference_band"] = ""
    if "positioning_note" not in df.columns:
        df["positioning_note"] = ""

    return df.reset_index(drop=True)


def build_output():
    df = load_input()

    rows = []
    for i, r in df.iterrows():
        low = round_money(r["market_price_low_eur"])
        med = round_money(r["market_price_median_eur"])
        high = round_money(r["market_price_high_eur"])

        # placeholder sample size because source file is already a summary layer
        sample_size = 10

        rows.append(
            {
                "benchmark_id": f"TMB_{i+1:03d}",
                "variable_block_id": "V2BL_FIX_001",
                "benchmark_family": "treatment_market_price_benchmark_clean",
                "benchmark_stage": "normalized_from_summary_source",
                "treatment_category": normalize_text(r["treatment_category"]).lower(),
                "treatment_variant": normalize_text(r["treatment_variant"]).lower(),
                "session_duration_min": int(r["session_duration_min"]),
                "market_price_low_eur": low,
                "market_price_median_eur": med,
                "market_price_high_eur": high,
                "market_price_range_band": f"{int(med)}-{int(high)}",
                "sample_size": sample_size,
                "benchmark_quality_flag": infer_benchmark_quality(sample_size),
                "benchmark_source_type": "summary_interpretation_layer",
                "source_file_name": INPUT_FP.name,
                "source_note": "normalized from pricing interpretation summary; replace later with raw competitor benchmark pipeline",
                "recommended_reference_band": normalize_text(r.get("recommended_reference_band", "")),
                "positioning_note": normalize_text(r.get("positioning_note", "")),
                "join_key_note": "join on treatment_category_treatment_variant_session_duration_min",
                "market_context": "Malta",
                "currency": "EUR",
                "effective_from": "2026-01-01",
                "effective_to": "",
                "audit_note": "clean benchmark layer created to stabilize downstream market validation joins",
                "status": "benchmark_defined",
            }
        )

    out = pd.DataFrame(rows)

    # Deduplicate by best available first row
    out = out.sort_values(
        ["treatment_category", "treatment_variant", "session_duration_min", "benchmark_quality_flag"]
    ).drop_duplicates(
        subset=["treatment_category", "treatment_variant", "session_duration_min"],
        keep="first"
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
