from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path.cwd()
OUT_DIR = PROJECT_ROOT / "data_processed" / "pricing_research"
OUT_DIR.mkdir(parents=True, exist_ok=True)

FLOOR_FP = OUT_DIR / "treatment_pricing_floor_engine.csv"
OUTPUT_FP = OUT_DIR / "treatment_price_recommendation_band_engine.csv"

POSITIONING_POLICY = {
    "facial":        {"standard_uplift": 1.00, "premium_uplift": 1.12, "luxury_uplift": 1.25},
    "body_treatment":{"standard_uplift": 1.00, "premium_uplift": 1.10, "luxury_uplift": 1.20},
    "aromatherapy":  {"standard_uplift": 1.00, "premium_uplift": 1.10, "luxury_uplift": 1.20},
    "deep_tissue":   {"standard_uplift": 1.00, "premium_uplift": 1.10, "luxury_uplift": 1.20},
    "massage":       {"standard_uplift": 1.00, "premium_uplift": 1.08, "luxury_uplift": 1.18},
    "reflexology":   {"standard_uplift": 1.00, "premium_uplift": 1.08, "luxury_uplift": 1.15},
    "scrub":         {"standard_uplift": 1.00, "premium_uplift": 1.10, "luxury_uplift": 1.20},
    "wrap":          {"standard_uplift": 1.00, "premium_uplift": 1.12, "luxury_uplift": 1.22},
    "default":       {"standard_uplift": 1.00, "premium_uplift": 1.10, "luxury_uplift": 1.20},
}

ROUND_TO = 1.0


def normalize_text(x):
    if pd.isna(x):
        return ""
    return str(x).strip()


def round_price(x, step=1.0):
    return round(float(x) / step) * step


def load_floor_input():
    if not FLOOR_FP.exists():
        raise FileNotFoundError(
            f"Required file not found: {FLOOR_FP}\n"
            "Run scripts/build/build_treatment_pricing_floor_engine.py first."
        )

    df = pd.read_csv(FLOOR_FP)
    print(f"[INFO] pricing floor input found: {FLOOR_FP.name} | rows={len(df)}")

    required = {
        "pricing_floor_id",
        "treatment_category",
        "treatment_variant",
        "session_duration_min",
        "pricing_floor_low_eur",
        "pricing_floor_mid_eur",
        "pricing_floor_high_eur",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in floor input: {sorted(missing)}")

    for col in [
        "session_duration_min",
        "pricing_floor_low_eur",
        "pricing_floor_mid_eur",
        "pricing_floor_high_eur",
    ]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(
        subset=[
            "session_duration_min",
            "pricing_floor_low_eur",
            "pricing_floor_mid_eur",
            "pricing_floor_high_eur",
        ]
    ).copy()

    df["treatment_category"] = df["treatment_category"].map(normalize_text).str.lower()
    df["treatment_variant"] = df["treatment_variant"].map(normalize_text).str.lower()
    df["session_duration_min"] = df["session_duration_min"].astype(int)

    return df.reset_index(drop=True)


def get_policy(category: str) -> dict:
    key = normalize_text(category).lower()
    return POSITIONING_POLICY.get(key, POSITIONING_POLICY["default"])


def build_output():
    df = load_floor_input()
    rows = []

    for i, r in df.iterrows():
        category = r["treatment_category"]
        variant = r["treatment_variant"]
        duration = int(r["session_duration_min"])
        policy = get_policy(category)

        floor_low = float(r["pricing_floor_low_eur"])
        floor_mid = float(r["pricing_floor_mid_eur"])
        floor_high = float(r["pricing_floor_high_eur"])

        standard_low = round_price(floor_low * policy["standard_uplift"], ROUND_TO)
        standard_mid = round_price(floor_mid * policy["standard_uplift"], ROUND_TO)
        standard_high = round_price(floor_high * policy["standard_uplift"], ROUND_TO)

        premium_low = round_price(floor_low * policy["premium_uplift"], ROUND_TO)
        premium_mid = round_price(floor_mid * policy["premium_uplift"], ROUND_TO)
        premium_high = round_price(floor_high * policy["premium_uplift"], ROUND_TO)

        luxury_low = round_price(floor_low * policy["luxury_uplift"], ROUND_TO)
        luxury_mid = round_price(floor_mid * policy["luxury_uplift"], ROUND_TO)
        luxury_high = round_price(floor_high * policy["luxury_uplift"], ROUND_TO)

        recommended_reference_band = f"{int(premium_mid)}-{int(luxury_mid)}"

        rows.append(
            {
                "price_band_id": f"TPRB_{i+1:03d}",
                "variable_block_id": "V2BL_008",
                "engine_family": "treatment_price_recommendation_band",
                "engine_stage": "policy_first_placeholder",
                "is_final_pricing_decision": "no",
                "pricing_floor_id": normalize_text(r["pricing_floor_id"]),
                "treatment_category": category,
                "treatment_variant": variant,
                "session_duration_min": duration,
                "pricing_floor_low_eur": floor_low,
                "pricing_floor_mid_eur": floor_mid,
                "pricing_floor_high_eur": floor_high,
                "standard_price_low_eur": standard_low,
                "standard_price_mid_eur": standard_mid,
                "standard_price_high_eur": standard_high,
                "premium_price_low_eur": premium_low,
                "premium_price_mid_eur": premium_mid,
                "premium_price_high_eur": premium_high,
                "luxury_price_low_eur": luxury_low,
                "luxury_price_mid_eur": luxury_mid,
                "luxury_price_high_eur": luxury_high,
                "recommended_reference_band": recommended_reference_band,
                "recommended_price_basis": "premium_mid_to_luxury_mid",
                "formula_role": "price_band_positioning",
                "formula_placeholder": "pricing_floor x positioning_uplift",
                "positioning_policy_method": "treatment_category_default_positioning_policy",
                "linked_upstream_dependency": "treatment_pricing_floor_engine.csv",
                "cost_scope_included": "pricing_floor_plus_positioning_uplift_only",
                "cost_scope_excluded": "competitor_reality_check_channel_discount_tax_display_rule_final_psychological_rounding",
                "confidence_level": "medium_low",
                "review_status": "needs_commercial_validation",
                "owner_function": "pricing_research",
                "market_context": "Malta",
                "currency": "EUR",
                "effective_from": "2026-01-01",
                "effective_to": "",
                "audit_note": "recommendation band is policy-first placeholder derived from pricing floor and positioning uplift",
                "status": "assumption_defined",
            }
        )

    out = pd.DataFrame(rows).sort_values(
        ["treatment_category", "treatment_variant", "session_duration_min"]
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
