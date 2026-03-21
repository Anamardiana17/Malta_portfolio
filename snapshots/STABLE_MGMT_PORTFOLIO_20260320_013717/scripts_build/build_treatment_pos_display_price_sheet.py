from pathlib import Path
import pandas as pd
import math

PROJECT_ROOT = Path.cwd()
OUT_DIR = PROJECT_ROOT / "data_processed" / "pricing_research"
OUT_DIR.mkdir(parents=True, exist_ok=True)

INPUT_FP = OUT_DIR / "final_treatment_pricing_master_with_vat.csv"
OUTPUT_FP = OUT_DIR / "treatment_pos_display_price_sheet.csv"


def normalize_text(x):
    if pd.isna(x):
        return ""
    return str(x).strip()


def round_money(x):
    return round(float(x), 2)


def ceil_to_int(x):
    return int(math.ceil(float(x)))


def psychological_price_up(value_inc_vat: float) -> float:
    base = ceil_to_int(value_inc_vat)
    return round(base - 0.01, 2)


def standard_display_price(value_inc_vat: float) -> float:
    return round_money(value_inc_vat)


def choose_display_price(row: pd.Series):
    inc_vat = float(row["sell_price_inc_vat_eur"])
    rule = normalize_text(row.get("display_price_rule", ""))

    if rule == "psychological_price_preferred":
        return psychological_price_up(inc_vat), "ceil_to_xx_99"
    if rule == "manual_review_before_display":
        return standard_display_price(inc_vat), "manual_review_hold"
    return standard_display_price(inc_vat), "standard_exact_display"


def choose_display_status(row: pd.Series):
    rule = normalize_text(row.get("display_price_rule", ""))
    launch_flag = normalize_text(row.get("launch_recommendation_flag", ""))

    if rule == "manual_review_before_display" or launch_flag == "review_before_launch":
        return "hold_for_review"
    if launch_flag in {"launch", "launch_as_entry_price", "launch_with_upside_room", "launch_with_value_message"}:
        return "ready_for_display"
    return "conditional_display"


def choose_pos_label(row: pd.Series):
    pricing_position = normalize_text(row.get("pricing_position", ""))

    if pricing_position == "traffic_builder":
        return "entry_offer"
    if pricing_position == "accessible_premium":
        return "core_value"
    return "premium_signature"


def build_display_note(row: pd.Series, display_price_inc_vat: float):
    pricing_position = normalize_text(row.get("pricing_position", ""))
    launch_flag = normalize_text(row.get("launch_recommendation_flag", ""))

    if pricing_position == "traffic_builder":
        role_note = "entry traffic role"
    elif pricing_position == "accessible_premium":
        role_note = "accessible premium role"
    else:
        role_note = "premium signature role"

    return f"Display price {display_price_inc_vat:.2f} EUR inc-VAT; {role_note}; launch flag = {launch_flag}."


def load_input():
    if not INPUT_FP.exists():
        raise FileNotFoundError(
            f"Required file not found: {INPUT_FP}\n"
            "Run scripts/build/build_final_treatment_pricing_master_with_vat.py first."
        )

    df = pd.read_csv(INPUT_FP)
    print(f"[INFO] VAT pricing master input found: {INPUT_FP.name} | rows={len(df)}")

    required = {
        "pricing_master_vat_id",
        "pricing_master_id",
        "treatment_category",
        "treatment_variant",
        "session_duration_min",
        "pricing_position",
        "price_architecture_flag",
        "launch_recommendation_flag",
        "execution_priority",
        "sell_price_ex_vat_eur",
        "vat_rate",
        "vat_amount_eur",
        "sell_price_inc_vat_eur",
        "display_price_rule",
        "market_check_status",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in VAT pricing master input: {sorted(missing)}")

    numeric_cols = [
        "session_duration_min",
        "sell_price_ex_vat_eur",
        "vat_rate",
        "vat_amount_eur",
        "sell_price_inc_vat_eur",
        "pricing_floor_ex_vat_eur",
        "pricing_floor_inc_vat_eur",
        "gap_vs_floor_ex_vat_eur",
        "gap_vs_floor_pct",
        "market_price_low_eur",
        "market_price_median_eur",
          "commercial_market_price_median_eur",
        "market_price_high_eur",
        "gap_vs_market_median_eur",
        "gap_vs_market_median_pct",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["treatment_category"] = df["treatment_category"].map(normalize_text).str.lower()
    df["treatment_variant"] = df["treatment_variant"].map(normalize_text).str.lower()
    df["session_duration_min"] = df["session_duration_min"].astype(int)

    return df.reset_index(drop=True)


def build_output():
    df = load_input()

    rows = []
    for i, r in df.iterrows():
        display_price_inc_vat, display_rounding_method = choose_display_price(r)
        display_status = choose_display_status(r)
        pos_label = choose_pos_label(r)

        rows.append(
            {
                "pos_display_id": f"TPOS_{i+1:03d}",
                "variable_block_id": "V2BL_013",
                "sheet_family": "treatment_pos_display_price",
                "sheet_stage": "policy_first_placeholder",
                "is_live_pos_price": "no",
                "pricing_master_vat_id": normalize_text(r["pricing_master_vat_id"]),
                "pricing_master_id": normalize_text(r["pricing_master_id"]),
                "treatment_category": normalize_text(r["treatment_category"]).lower(),
                "treatment_variant": normalize_text(r["treatment_variant"]).lower(),
                "session_duration_min": int(r["session_duration_min"]),
                "pricing_position": normalize_text(r["pricing_position"]),
                "price_architecture_flag": normalize_text(r["price_architecture_flag"]),
                "launch_recommendation_flag": normalize_text(r["launch_recommendation_flag"]),
                "execution_priority": normalize_text(r["execution_priority"]),
                "pos_label": pos_label,
                "sell_price_ex_vat_eur": round_money(r["sell_price_ex_vat_eur"]),
                "vat_rate": round(float(r["vat_rate"]), 4),
                "vat_amount_eur": round_money(r["vat_amount_eur"]),
                "sell_price_inc_vat_eur": round_money(r["sell_price_inc_vat_eur"]),
                "display_price_inc_vat_eur": round_money(display_price_inc_vat),
                "display_price_ex_vat_equivalent_eur": round_money(display_price_inc_vat / (1 + float(r["vat_rate"]))),
                "display_rounding_method": display_rounding_method,
                "display_price_rule": normalize_text(r["display_price_rule"]),
                "display_status": display_status,
                "market_check_status": normalize_text(r["market_check_status"]),
                  "commercial_decision_basis": normalize_text(r.get("commercial_decision_basis", "")),
                  "governance_status": normalize_text(r.get("governance_status", "")),
                  "governance_note": normalize_text(r.get("governance_note", "")),
                "market_price_median_eur": round_money(r["market_price_median_eur"]) if pd.notna(r.get("market_price_median_eur")) else None,
                  "commercial_market_price_median_eur": round_money(r["commercial_market_price_median_eur"]) if pd.notna(r.get("commercial_market_price_median_eur")) else None,
                "gap_vs_market_median_eur": round_money(r["gap_vs_market_median_eur"]) if pd.notna(r.get("gap_vs_market_median_eur")) else None,
                "gap_vs_market_median_pct": round(float(r["gap_vs_market_median_pct"]), 2) if pd.notna(r.get("gap_vs_market_median_pct")) else None,
                "display_note": build_display_note(r, display_price_inc_vat),
                "pricing_summary_note": normalize_text(r.get("pricing_summary_note", "")),
                "decision_adjustment_note": normalize_text(r.get("decision_adjustment_note", "")),
                "formula_role": "pos_display_conversion",
                "formula_placeholder": "display_price derived from sell_price_inc_vat subject to display_rule",
                "linked_upstream_dependency": "final_treatment_pricing_master_with_vat.csv",
                "cost_scope_included": "retail_display_price_after_output_vat_layer",
                "cost_scope_excluded": "live_pos_sync_channel_discount_coupon_bundle_member_price_rule",
                "confidence_level": "medium_low",
                "review_status": "needs_retail_ops_signoff",
                "owner_function": "pricing_research",
                "market_context": "Malta",
                "currency": "EUR",
                "effective_from": "2026-01-01",
                "effective_to": "",
                "audit_note": "POS display sheet separates analytical price from customer-facing display price and rounding logic",
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
