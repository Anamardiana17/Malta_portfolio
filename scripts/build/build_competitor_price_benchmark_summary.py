from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path.cwd()
OUT_DIR = PROJECT_ROOT / "data_processed" / "pricing_research"
OUT_DIR.mkdir(parents=True, exist_ok=True)

INPUT_FP = OUT_DIR / "competitor_price_clean.csv"
OUTPUT_FP = OUT_DIR / "competitor_price_benchmark_summary.csv"
WRAP_EXACT_LOG_FP = OUT_DIR / "wrap_60min_exact_only_research_log.csv"

def normalize_text(x):
    if pd.isna(x):
        return ""
    return str(x).strip()

def infer_quality(sample_size, match_basis):
    if match_basis == "exact_match":
        if sample_size >= 6:
            return "high"
        if sample_size >= 3:
            return "medium"
        return "low"
    if sample_size >= 3:
        return "low"
    return "very_low"

def load_input():
    if not INPUT_FP.exists():
        raise FileNotFoundError(
            f"Required file not found: {INPUT_FP}\n"
            "Run scripts/build/build_competitor_price_clean.py first."
        )

    df = pd.read_csv(INPUT_FP)
    print(f"[INFO] competitor clean input found: {INPUT_FP.name} | rows={len(df)}")

    required = {
        "competitor_clean_id",
        "competitor_name",
        "treatment_category",
        "treatment_variant",
        "target_duration_min",
        "duration_match_type",
        "listed_price_eur",
        "benchmark_include_flag_final",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in clean input: {sorted(missing)}")

    df["target_duration_min"] = pd.to_numeric(df["target_duration_min"], errors="coerce")
    df["listed_price_eur"] = pd.to_numeric(df["listed_price_eur"], errors="coerce")
    df["treatment_category"] = df["treatment_category"].map(normalize_text).str.lower()
    df["treatment_variant"] = df["treatment_variant"].map(normalize_text).str.lower()
    df["benchmark_include_flag_final"] = df["benchmark_include_flag_final"].map(normalize_text).str.lower()
    df["duration_match_type"] = df["duration_match_type"].map(normalize_text).str.lower()

    df = df[
        (df["benchmark_include_flag_final"] == "include") &
        (df["target_duration_min"].notna()) &
        (df["listed_price_eur"].notna())
    ].copy()

    if not df.empty:
        df["target_duration_min"] = df["target_duration_min"].astype(int)

    return df.reset_index(drop=True)

def summarize(df, match_basis):
    grouped = (
        df.groupby(["treatment_category", "treatment_variant", "target_duration_min"], dropna=False)
        .agg(
            market_price_low_eur=("listed_price_eur", "min"),
            market_price_median_eur=("listed_price_eur", "median"),
            market_price_high_eur=("listed_price_eur", "max"),
            sample_size=("competitor_clean_id", "count"),
            competitor_count=("competitor_name", lambda s: s.astype(str).str.strip().replace("", pd.NA).dropna().nunique()),
        )
        .reset_index()
    )

    rows = []
    for i, r in grouped.iterrows():
        sample_size = int(r["sample_size"])
        rows.append({
            "benchmark_summary_id": f"CPSUM_{match_basis[:2].upper()}_{i+1:03d}",
            "variable_block_id": "V2BL_FIX_004",
            "benchmark_family": "competitor_price_benchmark_summary",
            "treatment_category": normalize_text(r["treatment_category"]).lower(),
            "treatment_variant": normalize_text(r["treatment_variant"]).lower(),
            "session_duration_min": int(r["target_duration_min"]),
            "market_price_low_eur": round(float(r["market_price_low_eur"]), 2),
            "market_price_median_eur": round(float(r["market_price_median_eur"]), 2),
            "market_price_high_eur": round(float(r["market_price_high_eur"]), 2),
            "sample_size": sample_size,
            "competitor_count": int(r["competitor_count"]) if pd.notna(r["competitor_count"]) else 0,
            "benchmark_quality_flag": infer_quality(sample_size, match_basis),
            "duration_match_basis": match_basis,
            "source_note": f"built from competitor_price_clean rows with {match_basis}",
            "market_context": "Malta",
            "currency": "EUR",
            "status": "benchmark_defined",
            "audit_note": "summary benchmark generated from included competitor rows only",
            "benchmark_method_note": "",
            "benchmark_governance_note": "",
            "native_rule_flag": "",
        })
    return pd.DataFrame(rows)

def build_output():
    df = load_input()

    if df.empty:
        return pd.DataFrame(columns=[
            "benchmark_summary_id","variable_block_id","benchmark_family","treatment_category",
            "treatment_variant","session_duration_min","market_price_low_eur","market_price_median_eur",
            "market_price_high_eur","sample_size","competitor_count","benchmark_quality_flag",
            "duration_match_basis","source_note","market_context","currency","status","audit_note",
            "benchmark_method_note","benchmark_governance_note","native_rule_flag"
        ])

    exact_df = df[df["duration_match_type"] == "exact_match"].copy()
    near_df = df[df["duration_match_type"] == "near_match_duration"].copy()

    exact_summary = summarize(exact_df, "exact_match") if not exact_df.empty else pd.DataFrame()
    near_summary = summarize(near_df, "near_match_duration") if not near_df.empty else pd.DataFrame()

    if exact_summary.empty:
        out = near_summary.copy()
    else:
        out = exact_summary.copy()
        if not near_summary.empty:
            existing_keys = set(
                zip(out["treatment_category"], out["treatment_variant"], out["session_duration_min"])
            )
            near_summary = near_summary[
                ~near_summary.apply(
                    lambda r: (r["treatment_category"], r["treatment_variant"], r["session_duration_min"]) in existing_keys,
                    axis=1
                )
            ].copy()
            out = pd.concat([out, near_summary], ignore_index=True)

    out = out.sort_values(
        ["treatment_category", "treatment_variant", "session_duration_min"]
    ).reset_index(drop=True)

    return out

def main():
    out = build_output()

    # --- governance override: wrap exact market fact locked to approved baseline ---
    mask_wrap_exact = (
        out["treatment_category"].astype(str).str.strip().str.lower().eq("wrap")
        & out["treatment_variant"].astype(str).str.strip().str.lower().eq("standard")
        & out["session_duration_min"].astype(float).eq(60)
        & out["duration_match_basis"].astype(str).str.strip().str.lower().eq("exact_match")
    )

    out.loc[mask_wrap_exact, "market_price_low_eur"] = 72.0
    out.loc[mask_wrap_exact, "market_price_median_eur"] = 72.0
    out.loc[mask_wrap_exact, "market_price_high_eur"] = 72.0
    out.loc[mask_wrap_exact, "sample_size"] = 1
    out.loc[mask_wrap_exact, "benchmark_quality_flag"] = "low"
    out.loc[mask_wrap_exact, "source_note"] = (
        "wrap exact market fact locked to governance-approved baseline 72.0 EUR; "
        "additional exact observations retained outside fact baseline for decision-support only"
    )
    out.loc[mask_wrap_exact, "benchmark_method_note"] = (
        "native_locked_exact_only_wrap_60min_market_fact"
    )
    out.loc[mask_wrap_exact, "benchmark_governance_note"] = (
        "exact_only_research_locked_manual_native_rule"
    )
    out.loc[mask_wrap_exact, "native_rule_flag"] = "yes"

    wrap_log = out.loc[mask_wrap_exact, [
        "benchmark_summary_id",
        "treatment_category",
        "treatment_variant",
        "session_duration_min",
        "duration_match_basis",
        "market_price_low_eur",
        "market_price_median_eur",
        "market_price_high_eur",
        "sample_size",
        "competitor_count",
        "benchmark_quality_flag",
        "benchmark_method_note",
        "benchmark_governance_note",
        "native_rule_flag",
        "source_note",
        "audit_note",
    ]].copy()

    wrap_log.to_csv(WRAP_EXACT_LOG_FP, index=False)
    out.to_csv(OUTPUT_FP, index=False)

    print(f"[OK] saved: {OUTPUT_FP}")
    print(f"[OK] saved: {WRAP_EXACT_LOG_FP}")
    print(f"[OK] rows: {len(out)}")
    print("\nPreview:")
    if len(out) > 0:
        print(out.to_string(index=False))
    else:
        print("[INFO] no included competitor rows yet; summary file created empty")

if __name__ == "__main__":
    main()

    