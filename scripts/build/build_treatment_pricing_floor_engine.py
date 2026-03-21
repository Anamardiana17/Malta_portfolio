from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path.cwd()
OUT_DIR = PROJECT_ROOT / "data_processed" / "pricing_research"
OUT_DIR.mkdir(parents=True, exist_ok=True)

DIRECT_COST_FP = OUT_DIR / "treatment_direct_cost_engine.csv"
OUTPUT_FP = OUT_DIR / "treatment_pricing_floor_engine.csv"

# Purpose:
# - convert direct cost into price floor using target gross margin assumptions
# - still policy-first / scenario-first
# - not final commercial pricing decision

DEFAULT_MARGIN_POLICY = {
    "facial":       {"low": 0.60, "mid": 0.68, "high": 0.72},
    "body_treatment":{"low": 0.58, "mid": 0.65, "high": 0.70},
    "aromatherapy": {"low": 0.58, "mid": 0.65, "high": 0.70},
    "deep_tissue":  {"low": 0.60, "mid": 0.67, "high": 0.72},
    "massage":      {"low": 0.58, "mid": 0.65, "high": 0.70},
    "reflexology":  {"low": 0.55, "mid": 0.62, "high": 0.68},
    "scrub":        {"low": 0.58, "mid": 0.65, "high": 0.70},
    "wrap":         {"low": 0.60, "mid": 0.68, "high": 0.72},
    "default":      {"low": 0.58, "mid": 0.65, "high": 0.70},
}

ROUND_PRICE_TO = 1.0


def normalize_text(x):
    if pd.isna(x):
        return ""
    return str(x).strip()


def round_price(x, step=1.0):
    return round(float(x) / step) * step


def load_direct_cost():
    if not DIRECT_COST_FP.exists():
        raise FileNotFoundError(
            f"Required file not found: {DIRECT_COST_FP}\n"
            "Run scripts/build/build_treatment_direct_cost_engine.py first."
        )

    df = pd.read_csv(DIRECT_COST_FP)
    print(f"[INFO] direct cost input found: {DIRECT_COST_FP.name} | rows={len(df)}")

    required = {
        "direct_cost_id",
        "treatment_category",
        "treatment_variant",
        "session_duration_min",
        "direct_cost_low_eur",
        "direct_cost_mid_eur",
        "direct_cost_high_eur",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in direct cost input: {sorted(missing)}")

    for col in [
        "session_duration_min",
        "direct_cost_low_eur",
        "direct_cost_mid_eur",
        "direct_cost_high_eur",
    ]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(
        subset=[
            "session_duration_min",
            "direct_cost_low_eur",
            "direct_cost_mid_eur",
            "direct_cost_high_eur",
        ]
    ).copy()

    df["treatment_category"] = df["treatment_category"].map(normalize_text).str.lower()
    df["treatment_variant"] = df["treatment_variant"].map(normalize_text).str.lower()
    df["session_duration_min"] = df["session_duration_min"].astype(int)

    return df.reset_index(drop=True)


def get_margin_policy(category: str) -> dict:
    key = normalize_text(category).lower()
    return DEFAULT_MARGIN_POLICY.get(key, DEFAULT_MARGIN_POLICY["default"])


def safe_floor_price(cost, margin):
    # price = cost / (1 - margin)
    if margin >= 1:
        raise ValueError("Margin must be < 1")
    return cost / (1 - margin)


def build_output():
    df = load_direct_cost()

    rows = []
    for i, r in df.iterrows():
        category = r["treatment_category"]
        variant = r["treatment_variant"]
        duration = int(r["session_duration_min"])

        policy = get_margin_policy(category)

        direct_low = float(r["direct_cost_low_eur"])
        direct_mid = float(r["direct_cost_mid_eur"])
        direct_high = float(r["direct_cost_high_eur"])

        # Scenario logic:
        # low floor -> high assumed margin
        # mid floor -> mid assumed margin
        # high floor -> low assumed margin
        floor_low = round_price(safe_floor_price(direct_low, policy["high"]), ROUND_PRICE_TO)
        floor_mid = round_price(safe_floor_price(direct_mid, policy["mid"]), ROUND_PRICE_TO)
        floor_high = round_price(safe_floor_price(direct_high, policy["low"]), ROUND_PRICE_TO)

        rows.append(
            {
                "pricing_floor_id": f"TPF_{i+1:03d}",
                "variable_block_id": "V2BL_007",
                "engine_family": "treatment_pricing_floor",
                "engine_stage": "policy_first_placeholder",
                "is_final_pricing_decision": "no",
                "direct_cost_id": normalize_text(r["direct_cost_id"]),
                "treatment_category": category,
                "treatment_variant": variant,
                "session_duration_min": duration,
                "direct_cost_low_eur": round(direct_low, 2),
                "direct_cost_mid_eur": round(direct_mid, 2),
                "direct_cost_high_eur": round(direct_high, 2),
                "target_margin_low_case": policy["low"],
                "target_margin_mid_case": policy["mid"],
                "target_margin_high_case": policy["high"],
                "pricing_floor_low_eur": floor_low,
                "pricing_floor_mid_eur": floor_mid,
                "pricing_floor_high_eur": floor_high,
                "recommended_pricing_floor_basis": "mid",
                "formula_role": "pricing_floor_conversion",
                "formula_placeholder": "direct_cost / (1 - target_margin)",
                "margin_policy_method": "treatment_category_default_margin_policy",
                "linked_upstream_dependency": "treatment_direct_cost_engine.csv",
                "cost_scope_included": "direct_cost_based_floor_only",
                "cost_scope_excluded": "brand_premium_market_positioning_competitor_alignment_final_rounding_vat_policy",
                "confidence_level": "medium_low",
                "review_status": "needs_finance_commercial_validation",
                "owner_function": "pricing_research",
                "market_context": "Malta",
                "currency": "EUR",
                "effective_from": "2026-01-01",
                "effective_to": "",
                "audit_note": "pricing floor is scenario-based placeholder derived from direct cost and target margin assumptions",
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
