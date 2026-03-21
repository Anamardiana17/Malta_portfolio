from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path.cwd()
OUT_DIR = PROJECT_ROOT / "data_processed" / "pricing_research"
OUT_DIR.mkdir(parents=True, exist_ok=True)

INPUT_FP = OUT_DIR / "final_treatment_pricing_master_with_vat.csv"
OUTPUT_FP = OUT_DIR / "treatment_geopolitical_stress_test_sheet.csv"

# Scenario intent:
# Worst-case geopolitical shock proxy for a severe US-Iran conflict escalation
# affecting oil, shipping, inflation, tourism sentiment, and discretionary spend.
#
# This is a scenario sheet, not a forecast and not a final commercial decision.

SCENARIO_ID = "GEO_001"
SCENARIO_NAME = "worst_case_us_iran_conflict_shock"
SCENARIO_LEVEL = "severe"
SCENARIO_VERSION = "v1"

# Stress assumptions
# Keep them transparent and editable.
DEFAULT_STRESS_POLICY = {
    "traffic_builder": {
        "material_cost_uplift_pct": 8.0,
        "labor_utility_uplift_pct": 3.0,
        "demand_softening_pct": -18.0,
        "recommended_price_action": "hold_or_minimal_increase",
        "recommended_promo_action": "protect_traffic_with_entry_offer",
    },
    "accessible_premium": {
        "material_cost_uplift_pct": 10.0,
        "labor_utility_uplift_pct": 4.0,
        "demand_softening_pct": -15.0,
        "recommended_price_action": "selective_increase",
        "recommended_promo_action": "bundle_value_message",
    },
    "premium_core": {
        "material_cost_uplift_pct": 12.0,
        "labor_utility_uplift_pct": 5.0,
        "demand_softening_pct": -12.0,
        "recommended_price_action": "controlled_increase",
        "recommended_promo_action": "protect_margin_not_discount_first",
    },
    "premium_edge": {
        "material_cost_uplift_pct": 14.0,
        "labor_utility_uplift_pct": 6.0,
        "demand_softening_pct": -14.0,
        "recommended_price_action": "review_before_increase",
        "recommended_promo_action": "differentiate_before_price_push",
    },
    "default": {
        "material_cost_uplift_pct": 12.0,
        "labor_utility_uplift_pct": 5.0,
        "demand_softening_pct": -15.0,
        "recommended_price_action": "controlled_increase",
        "recommended_promo_action": "protect_margin_not_discount_first",
    },
}


def normalize_text(x):
    if pd.isna(x):
        return ""
    return str(x).strip()


def round_money(x):
    return round(float(x), 2)


def load_input():
    if not INPUT_FP.exists():
        raise FileNotFoundError(
            f"Required file not found: {INPUT_FP}\n"
            "Run scripts/build/build_final_treatment_pricing_master_with_vat.py first."
        )

    df = pd.read_csv(INPUT_FP)
    print(f"[INFO] pricing master with VAT input found: {INPUT_FP.name} | rows={len(df)}")

    required = {
        "pricing_master_vat_id",
        "pricing_master_id",
        "treatment_category",
        "treatment_variant",
        "session_duration_min",
        "pricing_position",
        "price_architecture_flag",
        "launch_recommendation_flag",
        "sell_price_ex_vat_eur",
        "sell_price_inc_vat_eur",
        "pricing_floor_ex_vat_eur",
        "vat_rate",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in input: {sorted(missing)}")

    numeric_cols = [
        "session_duration_min",
        "sell_price_ex_vat_eur",
        "sell_price_inc_vat_eur",
        "pricing_floor_ex_vat_eur",
        "pricing_floor_inc_vat_eur",
        "vat_rate",
        "gap_vs_market_median_pct",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["treatment_category"] = df["treatment_category"].map(normalize_text).str.lower()
    df["treatment_variant"] = df["treatment_variant"].map(normalize_text).str.lower()
    df["pricing_position"] = df["pricing_position"].map(normalize_text)
    df["price_architecture_flag"] = df["price_architecture_flag"].map(normalize_text)
    df["session_duration_min"] = df["session_duration_min"].astype(int)

    return df.reset_index(drop=True)


def get_stress_policy(row):
    key = normalize_text(row.get("price_architecture_flag", ""))
    if key in DEFAULT_STRESS_POLICY:
        return DEFAULT_STRESS_POLICY[key]

    key2 = normalize_text(row.get("pricing_position", ""))
    if key2 in DEFAULT_STRESS_POLICY:
        return DEFAULT_STRESS_POLICY[key2]

    return DEFAULT_STRESS_POLICY["default"]


def choose_resilience_flag(price_architecture_flag, launch_recommendation_flag):
    paf = normalize_text(price_architecture_flag)
    lrf = normalize_text(launch_recommendation_flag)

    if paf == "traffic_builder":
        return "demand_sensitive"
    if paf == "accessible_premium":
        return "balanced_resilience"
    if lrf == "review_before_launch":
        return "fragile_under_shock"
    return "margin_resilient_if_differentiated"


def choose_action_priority(demand_softening_pct, price_action):
    if demand_softening_pct <= -18:
        return "urgent"
    if price_action in {"review_before_increase", "hold_or_minimal_increase"}:
        return "high"
    return "medium"


def build_stress_note(row, stressed_price_inc_vat, demand_softening_pct, price_action):
    category = normalize_text(row["treatment_category"])
    return (
        f"Severe geopolitical shock scenario for {category}: demand proxy {demand_softening_pct:.2f}% "
        f"and stressed display price {stressed_price_inc_vat:.2f} EUR inc-VAT; action = {price_action}."
    )


def build_output():
    df = load_input()

    rows = []
    for i, r in df.iterrows():
        policy = get_stress_policy(r)

        base_ex_vat = float(r["sell_price_ex_vat_eur"])
        base_inc_vat = float(r["sell_price_inc_vat_eur"])
        floor_ex_vat = float(r["pricing_floor_ex_vat_eur"])
        vat_rate = float(r["vat_rate"])

        material_uplift_pct = float(policy["material_cost_uplift_pct"])
        labor_utility_uplift_pct = float(policy["labor_utility_uplift_pct"])
        demand_softening_pct = float(policy["demand_softening_pct"])

        # Simplified internal stress factor:
        # 60% weight to material/import-related pressure
        # 40% weight to labor/utility inflation pressure
        weighted_cost_pressure_pct = round((material_uplift_pct * 0.60) + (labor_utility_uplift_pct * 0.40), 2)

        stressed_floor_ex_vat = round_money(floor_ex_vat * (1 + weighted_cost_pressure_pct / 100))
        stressed_sell_price_ex_vat = round_money(base_ex_vat * (1 + weighted_cost_pressure_pct / 100))
        stressed_vat_amount = round_money(stressed_sell_price_ex_vat * vat_rate)
        stressed_sell_price_inc_vat = round_money(stressed_sell_price_ex_vat + stressed_vat_amount)

        margin_buffer_vs_stressed_floor_eur = round_money(stressed_sell_price_ex_vat - stressed_floor_ex_vat)
        margin_buffer_vs_stressed_floor_pct = (
            round(((stressed_sell_price_ex_vat / stressed_floor_ex_vat) - 1) * 100, 2)
            if stressed_floor_ex_vat > 0 else None
        )

        resilience_flag = choose_resilience_flag(
            r.get("price_architecture_flag", ""),
            r.get("launch_recommendation_flag", ""),
        )
        action_priority = choose_action_priority(
            demand_softening_pct,
            policy["recommended_price_action"],
        )

        rows.append(
            {
                "geo_stress_id": f"TGST_{i+1:03d}",
                "variable_block_id": "V2BL_014",
                "sheet_family": "treatment_geopolitical_stress_test",
                "sheet_stage": "scenario_placeholder",
                "is_live_shock_pricing": "no",
                "scenario_id": SCENARIO_ID,
                "scenario_name": SCENARIO_NAME,
                "scenario_level": SCENARIO_LEVEL,
                "scenario_version": SCENARIO_VERSION,
                "pricing_master_vat_id": normalize_text(r["pricing_master_vat_id"]),
                "pricing_master_id": normalize_text(r["pricing_master_id"]),
                "treatment_category": normalize_text(r["treatment_category"]).lower(),
                "treatment_variant": normalize_text(r["treatment_variant"]).lower(),
                "session_duration_min": int(r["session_duration_min"]),
                "pricing_position": normalize_text(r.get("pricing_position", "")),
                "price_architecture_flag": normalize_text(r.get("price_architecture_flag", "")),
                "launch_recommendation_flag": normalize_text(r.get("launch_recommendation_flag", "")),
                "base_sell_price_ex_vat_eur": round_money(base_ex_vat),
                "base_sell_price_inc_vat_eur": round_money(base_inc_vat),
                "base_pricing_floor_ex_vat_eur": round_money(floor_ex_vat),
                "material_cost_uplift_pct": material_uplift_pct,
                "labor_utility_uplift_pct": labor_utility_uplift_pct,
                "weighted_cost_pressure_pct": weighted_cost_pressure_pct,
                "demand_softening_pct": demand_softening_pct,
                "stressed_pricing_floor_ex_vat_eur": stressed_floor_ex_vat,
                "stressed_sell_price_ex_vat_eur": stressed_sell_price_ex_vat,
                "stressed_vat_rate": round(vat_rate, 4),
                "stressed_vat_amount_eur": stressed_vat_amount,
                "stressed_sell_price_inc_vat_eur": stressed_sell_price_inc_vat,
                "margin_buffer_vs_stressed_floor_eur": margin_buffer_vs_stressed_floor_eur,
                "margin_buffer_vs_stressed_floor_pct": margin_buffer_vs_stressed_floor_pct,
                "resilience_flag": resilience_flag,
                "recommended_price_action": policy["recommended_price_action"],
                "recommended_promo_action": policy["recommended_promo_action"],
                "action_priority": action_priority,
                "market_check_status": normalize_text(r.get("market_check_status", "")),
                "shock_response_note": build_stress_note(
                    r,
                    stressed_sell_price_inc_vat,
                    demand_softening_pct,
                    policy["recommended_price_action"],
                ),
                "formula_role": "geopolitical_stress_test",
                "formula_placeholder": "base price and floor stressed by weighted external shock proxy",
                "linked_upstream_dependency": "final_treatment_pricing_master_with_vat.csv",
                "scenario_reference_note": "Designed for severe oil-shipping-inflation-demand shock scenario",
                "cost_scope_included": "pricing_floor_price_and_demand_stress_proxy",
                "cost_scope_excluded": "live_forex_hedging_supplier_contract_detail_actual_occupancy_simulation",
                "confidence_level": "low_to_medium",
                "review_status": "needs_risk_management_review",
                "owner_function": "pricing_research",
                "market_context": "Malta",
                "currency": "EUR",
                "effective_from": "2026-01-01",
                "effective_to": "",
                "audit_note": "Scenario sheet only; use for downside planning, not as direct launch price file",
                "status": "scenario_defined",
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
