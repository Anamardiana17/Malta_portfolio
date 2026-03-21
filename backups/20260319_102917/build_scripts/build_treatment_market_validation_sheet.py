from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path.cwd()
OUT_DIR = PROJECT_ROOT / "data_processed" / "pricing_research"
OUT_DIR.mkdir(parents=True, exist_ok=True)

DECISION_FP = OUT_DIR / "treatment_commercial_decision_sheet.csv"
PRIMARY_FP = OUT_DIR / "competitor_price_benchmark_summary.csv"
FALLBACK_FP = OUT_DIR / "treatment_market_price_benchmark_clean.csv"
OUTPUT_FP = OUT_DIR / "treatment_market_validation_sheet.csv"


def normalize_text(x):
    if isinstance(x, pd.Series):
        x = x.iloc[0] if len(x) else ""
    if pd.isna(x):
        return ""
    return str(x).strip()


def round_money(x):
    return round(float(x), 2)


def make_unique_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = []
    seen = {}
    for c in df.columns:
        if c not in seen:
            seen[c] = 0
            cols.append(c)
        else:
            seen[c] += 1
            cols.append(f"{c}__dup{seen[c]}")
    df = df.copy()
    df.columns = cols
    return df


def load_decision_input():
    df = pd.read_csv(DECISION_FP)

    for col in [
        "session_duration_min",
        "pricing_floor_mid_eur",
        "recommended_sell_price_eur",
        "gap_vs_floor_eur",
        "gap_vs_floor_pct",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["treatment_category"] = df["treatment_category"].map(normalize_text).str.lower()
    df["treatment_variant"] = df["treatment_variant"].map(normalize_text).str.lower()
    df["session_duration_min"] = df["session_duration_min"].astype(int)

    if "benchmark_source_note" in df.columns:
        df = df.rename(columns={"benchmark_source_note": "decision_benchmark_source_note"})

    return df.reset_index(drop=True)


def standardize_benchmark(df, source_name):
    df = make_unique_columns(df)

    rename_map = {}
    for c in df.columns:
        cl = c.strip().lower()
        if cl in {"benchmark_id", "benchmark_summary_id"}:
            rename_map[c] = "benchmark_id"
        elif cl in {"treatment_category"}:
            rename_map[c] = "treatment_category"
        elif cl in {"treatment_variant"}:
            rename_map[c] = "treatment_variant"
        elif cl in {"session_duration_min"}:
            rename_map[c] = "session_duration_min"
        elif cl in {"market_price_low_eur"}:
            rename_map[c] = "market_price_low_eur"
        elif cl in {"market_price_median_eur"}:
            rename_map[c] = "market_price_median_eur"
        elif cl in {"market_price_high_eur"}:
            rename_map[c] = "market_price_high_eur"
        elif cl in {"benchmark_source_note", "source_note", "source_file_name"}:
            rename_map[c] = "benchmark_source_note"
        elif cl in {"benchmark_quality_flag"}:
            rename_map[c] = "benchmark_quality_flag"
        elif cl in {"sample_size"}:
            rename_map[c] = "sample_size"
        elif cl in {"duration_match_basis"}:
            rename_map[c] = "duration_match_basis"

    df = df.rename(columns=rename_map)
    df = make_unique_columns(df)

    # keep first occurrence only for duplicated canonical names
    df = df.loc[:, ~df.columns.duplicated()].copy()

    if "benchmark_id" not in df.columns:
        df["benchmark_id"] = ""
    if "benchmark_source_note" not in df.columns:
        df["benchmark_source_note"] = source_name
    if "benchmark_quality_flag" not in df.columns:
        df["benchmark_quality_flag"] = ""
    if "sample_size" not in df.columns:
        df["sample_size"] = None
    if "duration_match_basis" not in df.columns:
        df["duration_match_basis"] = ""

    keep = [
        "benchmark_id",
        "treatment_category",
        "treatment_variant",
        "session_duration_min",
        "market_price_low_eur",
        "market_price_median_eur",
        "market_price_high_eur",
        "benchmark_source_note",
        "benchmark_quality_flag",
        "sample_size",
        "duration_match_basis",
    ]

    missing = [c for c in keep if c not in df.columns]
    if missing:
        raise ValueError(f"Benchmark file missing canonical columns {missing} | source={source_name}")

    df = df[keep].copy()

    for col in [
        "session_duration_min",
        "market_price_low_eur",
        "market_price_median_eur",
        "market_price_high_eur",
        "sample_size",
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

    df["treatment_category"] = df["treatment_category"].map(normalize_text).str.lower()
    df["treatment_variant"] = df["treatment_variant"].map(normalize_text).str.lower()
    df["session_duration_min"] = df["session_duration_min"].astype(int)

    return df.drop_duplicates(
        subset=["treatment_category", "treatment_variant", "session_duration_min"],
        keep="first"
    ).reset_index(drop=True)


def load_blended_benchmark():
    primary = pd.DataFrame()
    fallback = pd.DataFrame()

    if PRIMARY_FP.exists():
        primary = standardize_benchmark(pd.read_csv(PRIMARY_FP), PRIMARY_FP.name)
        print(f"[INFO] primary benchmark found: {PRIMARY_FP.name} | rows={len(primary)}")

    if FALLBACK_FP.exists():
        fallback = standardize_benchmark(pd.read_csv(FALLBACK_FP), FALLBACK_FP.name)
        print(f"[INFO] fallback benchmark found: {FALLBACK_FP.name} | rows={len(fallback)}")

    if primary.empty and fallback.empty:
        raise FileNotFoundError("No benchmark source available")

    if primary.empty:
        fallback = fallback.copy()
        fallback["benchmark_layer_used"] = "fallback_only"
        return fallback

    if fallback.empty:
        primary = primary.copy()
        primary["benchmark_layer_used"] = "primary_only"
        return primary

    primary = primary.copy()
    fallback = fallback.copy()
    primary["benchmark_layer_used"] = "primary_competitor"
    fallback["benchmark_layer_used"] = "fallback_clean"

    keys = ["treatment_category", "treatment_variant", "session_duration_min"]
    primary_keys = set(zip(primary[keys[0]], primary[keys[1]], primary[keys[2]]))
    fallback = fallback[
        ~fallback.apply(lambda r: (r[keys[0]], r[keys[1]], r[keys[2]]) in primary_keys, axis=1)
    ].copy()

    # align columns safely
    all_cols = list(dict.fromkeys(list(primary.columns) + list(fallback.columns)))
    primary = primary.reindex(columns=all_cols)
    fallback = fallback.reindex(columns=all_cols)

    out = pd.concat([primary, fallback], ignore_index=True)
    out = out.loc[:, ~out.columns.duplicated()].copy()
    return out.sort_values(keys).reset_index(drop=True)


def classify_market_status(gap_pct):
    if pd.isna(gap_pct):
        return "market_benchmark_missing"
    if gap_pct <= -10:
        return "below_market_median"
    if gap_pct >= 10:
        return "above_market_median"
    return "in_line_with_market_median"


def build_output():
    decision_df = load_decision_input()
    market_df = load_blended_benchmark()

    merged = decision_df.merge(
        market_df,
        how="left",
        on=["treatment_category", "treatment_variant", "session_duration_min"],
        suffixes=("", "_mkt"),
    )

    rows = []
    for i, r in merged.iterrows():
        rec_price = pd.to_numeric(r.get("recommended_sell_price_eur"), errors="coerce")
        market_median = pd.to_numeric(r.get("market_price_median_eur"), errors="coerce")
        market_low = pd.to_numeric(r.get("market_price_low_eur"), errors="coerce")
        market_high = pd.to_numeric(r.get("market_price_high_eur"), errors="coerce")

        gap_eur = None
        gap_pct = None
        if pd.notna(rec_price) and pd.notna(market_median) and market_median > 0:
            gap_eur = round_money(rec_price - market_median)
            gap_pct = round(((rec_price / market_median) - 1) * 100, 2)

        if pd.isna(market_median):
            note = "market benchmark not yet available; keep internal recommendation as placeholder"
        elif gap_pct <= -10:
            note = "recommended sell price sits below market median; may support penetration or accessibility strategy"
        elif gap_pct >= 10:
            note = "recommended sell price sits above market median; validate brand strength and service differentiation before launch"
        else:
            note = "recommended sell price is broadly aligned with market median benchmark"

        rows.append({
            "market_validation_id": f"TMV_{i+1:03d}",
            "variable_block_id": "V2BL_010",
            "sheet_family": "treatment_market_validation",
            "sheet_stage": "policy_first_placeholder",
            "is_final_sell_price": "no",
            "commercial_decision_id": normalize_text(r["commercial_decision_id"]),
            "benchmark_id": normalize_text(r.get("benchmark_id", "")),
            "treatment_category": normalize_text(r["treatment_category"]).lower(),
            "treatment_variant": normalize_text(r["treatment_variant"]).lower(),
            "session_duration_min": int(r["session_duration_min"]),
            "pricing_position": normalize_text(r.get("pricing_position", "")),
            "recommended_basis_used": normalize_text(r.get("recommended_basis_used", "")),
            "recommended_sell_price_eur": round_money(rec_price),
            "pricing_floor_mid_eur": round_money(r["pricing_floor_mid_eur"]) if pd.notna(r.get("pricing_floor_mid_eur")) else None,
            "gap_vs_floor_eur": round_money(r["gap_vs_floor_eur"]) if pd.notna(r.get("gap_vs_floor_eur")) else None,
            "gap_vs_floor_pct": round(float(r["gap_vs_floor_pct"]), 2) if pd.notna(r.get("gap_vs_floor_pct")) else None,
            "market_price_low_eur": round_money(market_low) if pd.notna(market_low) else None,
            "market_price_median_eur": round_money(market_median) if pd.notna(market_median) else None,
            "market_price_high_eur": round_money(market_high) if pd.notna(market_high) else None,
            "gap_vs_market_median_eur": gap_eur,
            "gap_vs_market_median_pct": gap_pct,
            "market_check_status": classify_market_status(gap_pct),
            "benchmark_source_note": normalize_text(r.get("benchmark_source_note", "")),
            "benchmark_quality_flag": normalize_text(r.get("benchmark_quality_flag", "")),
            "sample_size": int(r["sample_size"]) if pd.notna(r.get("sample_size")) else None,
            "duration_match_basis": normalize_text(r.get("duration_match_basis", "")),
            "benchmark_layer_used": normalize_text(r.get("benchmark_layer_used", "")),
            "decision_rule": "internal_recommendation_vs_market_median_validation",
            "decision_adjustment_note": note,
            "formula_role": "market_validation_comparison",
            "formula_placeholder": "recommended_sell_price - market_price_median",
            "linked_upstream_dependency": "treatment_commercial_decision_sheet.csv",
            "linked_market_benchmark": normalize_text(r.get("benchmark_source_note", "")),
            "cost_scope_included": "internal_recommendation_and_market_median_comparison",
            "cost_scope_excluded": "channel_discount_tax_display_rule_final_psychological_rounding_live_promo_effect",
            "confidence_level": "medium_low",
            "review_status": "needs_commercial_market_validation",
            "owner_function": "pricing_research",
            "market_context": "Malta",
            "currency": "EUR",
            "effective_from": "2026-01-01",
            "effective_to": "",
            "audit_note": "market validation prioritizes competitor summary and backfills from clean benchmark when competitor rows are missing",
            "status": "assumption_defined",
        })

    return pd.DataFrame(rows).sort_values(
        ["treatment_category", "treatment_variant", "session_duration_min"]
    ).reset_index(drop=True)


def main():
    out = build_output()
    out.to_csv(OUTPUT_FP, index=False)
    print(f"[OK] saved: {OUTPUT_FP}")
    print(f"[OK] rows: {len(out)}")
    print("\nPreview:")
    print(out.head(20).to_string(index=False))


if __name__ == "__main__":
    main()
