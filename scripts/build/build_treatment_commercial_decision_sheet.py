from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path.cwd()
OUT_DIR = PROJECT_ROOT / "data_processed" / "pricing_research"
OUT_DIR.mkdir(parents=True, exist_ok=True)

INPUT_FP = OUT_DIR / "treatment_price_recommendation_band_engine.csv"
OUTPUT_FP = OUT_DIR / "treatment_commercial_decision_sheet.csv"

# Purpose:
# - transform pricing band into commercial recommendation sheet
# - provide one recommended sell price per treatment
# - keep assumption-policy-first / audit-friendly
# - no competitor layer yet; market gap stays blank

POSITIONING_DECISION_POLICY = {
    "facial": {
        "recommended_basis": "premium_mid",
        "pricing_position": "premium_core",
        "decision_note": "facial can usually sustain stronger premium capture through product ritual and perceived expertise",
    },
    "body_treatment": {
        "recommended_basis": "premium_mid",
        "pricing_position": "premium_core",
        "decision_note": "body ritual can support premium positioning if experience design and product texture are visible to guest",
    },
    "aromatherapy": {
        "recommended_basis": "premium_mid",
        "pricing_position": "premium_core",
        "decision_note": "aromatherapy can justify premium through sensory ritual and oil positioning",
    },
    "deep_tissue": {
        "recommended_basis": "premium_mid",
        "pricing_position": "premium_core",
        "decision_note": "deep tissue can hold premium when therapist skill and pain-relief positioning are credible",
    },
    "massage": {
        "recommended_basis": "standard_high",
        "pricing_position": "accessible_premium",
        "decision_note": "general massage is often more price-visible; keep recommended sell price commercially reachable",
    },
    "reflexology": {
        "recommended_basis": "standard_high",
        "pricing_position": "traffic_builder",
        "decision_note": "reflexology often works well as an entry-price service and volume builder",
    },
    "scrub": {
        "recommended_basis": "premium_mid",
        "pricing_position": "premium_core",
        "decision_note": "scrub can support premium through material visibility and add-on potential",
    },
    "wrap": {
        "recommended_basis": "premium_mid",
        "pricing_position": "premium_core",
        "decision_note": "wrap tends to justify stronger premium through ritual complexity and material load",
    },
    "default": {
        "recommended_basis": "premium_mid",
        "pricing_position": "premium_core",
        "decision_note": "default commercial stance uses premium-mid until competitor reality check is added",
    },
}


def normalize_text(x):
    if pd.isna(x):
        return ""
    return str(x).strip()


def round_price(x):
    return round(float(x), 2)


def load_input():
    if not INPUT_FP.exists():
        raise FileNotFoundError(
            f"Required file not found: {INPUT_FP}\n"
            "Run scripts/build/build_treatment_price_recommendation_band_engine.py first."
        )

    df = pd.read_csv(INPUT_FP)
    print(f"[INFO] price band input found: {INPUT_FP.name} | rows={len(df)}")

    required = {
        "price_band_id",
        "treatment_category",
        "treatment_variant",
        "session_duration_min",
        "pricing_floor_mid_eur",
        "standard_price_mid_eur",
        "standard_price_high_eur",
        "premium_price_mid_eur",
        "luxury_price_mid_eur",
        "recommended_reference_band",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in price band input: {sorted(missing)}")

    numeric_cols = [
        "session_duration_min",
        "pricing_floor_low_eur",
        "pricing_floor_mid_eur",
        "pricing_floor_high_eur",
        "standard_price_low_eur",
        "standard_price_mid_eur",
        "standard_price_high_eur",
        "premium_price_low_eur",
        "premium_price_mid_eur",
        "premium_price_high_eur",
        "luxury_price_low_eur",
        "luxury_price_mid_eur",
        "luxury_price_high_eur",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["treatment_category"] = df["treatment_category"].map(normalize_text).str.lower()
    df["treatment_variant"] = df["treatment_variant"].map(normalize_text).str.lower()
    df["session_duration_min"] = df["session_duration_min"].astype(int)

    return df.reset_index(drop=True)


def get_policy(category: str) -> dict:
    key = normalize_text(category).lower()
    return POSITIONING_DECISION_POLICY.get(key, POSITIONING_DECISION_POLICY["default"])


def choose_recommended_price(row: pd.Series, basis: str) -> float:
    basis_map = {
        "standard_mid": row.get("standard_price_mid_eur"),
        "standard_high": row.get("standard_price_high_eur"),
        "premium_mid": row.get("premium_price_mid_eur"),
        "premium_high": row.get("premium_price_high_eur"),
        "luxury_mid": row.get("luxury_price_mid_eur"),
    }
    value = basis_map.get(basis)
    if pd.isna(value):
        raise ValueError(f"Recommended basis '{basis}' not available for row {row.get('price_band_id')}")
    return round_price(value)


def build_output():
    df = load_input()

    rows = []
    for i, r in df.iterrows():
        category = r["treatment_category"]
        variant = r["treatment_variant"]
        duration = int(r["session_duration_min"])
        policy = get_policy(category)

        recommended_basis = policy["recommended_basis"]
        recommended_sell_price = choose_recommended_price(r, recommended_basis)

        pricing_floor_mid = round_price(r["pricing_floor_mid_eur"])
        standard_price_mid = round_price(r["standard_price_mid_eur"])
        premium_price_mid = round_price(r["premium_price_mid_eur"])
        luxury_price_mid = round_price(r["luxury_price_mid_eur"])

        gap_vs_floor_eur = round_price(recommended_sell_price - pricing_floor_mid)
        gap_vs_floor_pct = round((recommended_sell_price / pricing_floor_mid - 1) * 100, 2) if pricing_floor_mid > 0 else None

        rows.append(
            {
                "commercial_decision_id": f"TCD_{i+1:03d}",
                "variable_block_id": "V2BL_009",
                "sheet_family": "treatment_commercial_decision",
                "sheet_stage": "policy_first_placeholder",
                "is_final_sell_price": "no",
                "price_band_id": normalize_text(r["price_band_id"]),
                "treatment_category": category,
                "treatment_variant": variant,
                "session_duration_min": duration,
                "pricing_floor_mid_eur": pricing_floor_mid,
                "standard_price_mid_eur": standard_price_mid,
                "premium_price_mid_eur": premium_price_mid,
                "luxury_price_mid_eur": luxury_price_mid,
                "recommended_sell_price_eur": recommended_sell_price,
                "recommended_basis_used": recommended_basis,
                "pricing_position": policy["pricing_position"],
                "recommended_reference_band": normalize_text(r["recommended_reference_band"]),
                "gap_vs_floor_eur": gap_vs_floor_eur,
                "gap_vs_floor_pct": gap_vs_floor_pct,
                "gap_vs_market_median_eur": "",
                "gap_vs_market_median_pct": "",
                "market_check_status": "not_yet_integrated",
                "decision_rule": "category_positioning_policy_based_selection",
                "decision_note": policy["decision_note"],
                "formula_role": "commercial_decision_selection",
                "formula_placeholder": "select recommended sell price from standard/premium/luxury band",
                "linked_upstream_dependency": "treatment_price_recommendation_band_engine.csv",
                "cost_scope_included": "pricing_floor_and_positioning_band_logic",
                "cost_scope_excluded": "competitor_reality_check_channel_discount_tax_display_rule_final_psychological_rounding",
                "confidence_level": "medium_low",
                "review_status": "needs_commercial_approval",
                "owner_function": "pricing_research",
                "market_context": "Malta",
                "currency": "EUR",
                "effective_from": "2026-01-01",
                "effective_to": "",
                "audit_note": "commercial decision sheet is policy-first placeholder until competitor layer and final rounding rules are integrated",
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
