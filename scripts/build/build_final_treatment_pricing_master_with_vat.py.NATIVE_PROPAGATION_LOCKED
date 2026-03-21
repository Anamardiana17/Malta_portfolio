from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path.cwd()
OUT_DIR = PROJECT_ROOT / "data_processed" / "pricing_research"
OUT_DIR.mkdir(parents=True, exist_ok=True)

INPUT_FP = OUT_DIR / "final_treatment_pricing_master.csv"
OUTPUT_FP = OUT_DIR / "final_treatment_pricing_master_with_vat.csv"

VAT_RATE = 0.18


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
            "Run scripts/build/build_final_treatment_pricing_master.py first."
        )

    df = pd.read_csv(INPUT_FP)
    print(f"[INFO] pricing master input found: {INPUT_FP.name} | rows={len(df)}")

    required = {
        "pricing_master_id",
        "treatment_category",
        "treatment_variant",
        "session_duration_min",
        "recommended_sell_price_eur",
        "pricing_floor_mid_eur",
        "market_check_status",
        "price_architecture_flag",
        "launch_recommendation_flag",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in pricing master input: {sorted(missing)}")

    numeric_cols = [
        "session_duration_min",
        "recommended_sell_price_eur",
        "pricing_floor_mid_eur",
        "gap_vs_floor_eur",
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


def decide_display_price_rule(pricing_position, launch_flag):
    pricing_position = normalize_text(pricing_position)
    launch_flag = normalize_text(launch_flag)

    if pricing_position == "traffic_builder":
        return "psychological_price_preferred"
    if launch_flag in {"review_before_launch", "launch_with_market_check"}:
        return "manual_review_before_display"
    return "standard_round_display_allowed"


def build_output():
    df = load_input()

    rows = []
    for _, r in df.iterrows():
        ex_vat = round_money(r["recommended_sell_price_eur"])
        vat_amount = round_money(ex_vat * VAT_RATE)
        inc_vat = round_money(ex_vat + vat_amount)

        floor_ex_vat = round_money(r["pricing_floor_mid_eur"])
        floor_vat_amount = round_money(floor_ex_vat * VAT_RATE)
        floor_inc_vat = round_money(floor_ex_vat + floor_vat_amount)

        rows.append(
            {
                "pricing_master_vat_id": normalize_text(r["pricing_master_id"]).replace("FTPM", "FTPMVAT"),
                "variable_block_id": "V2BL_012",
                "master_family": "final_treatment_pricing_master_with_vat",
                "master_stage": "policy_first_placeholder",
                "is_final_approved_price": "no",
                "pricing_master_id": normalize_text(r["pricing_master_id"]),
                "treatment_category": normalize_text(r["treatment_category"]).lower(),
                "treatment_variant": normalize_text(r["treatment_variant"]).lower(),
                "session_duration_min": int(r["session_duration_min"]),
                "pricing_position": normalize_text(r.get("pricing_position", "")),
                "price_architecture_flag": normalize_text(r.get("price_architecture_flag", "")),
                "launch_recommendation_flag": normalize_text(r.get("launch_recommendation_flag", "")),
                "execution_priority": normalize_text(r.get("execution_priority", "")),
                "recommended_basis_used": normalize_text(r.get("recommended_basis_used", "")),
                "sell_price_ex_vat_eur": ex_vat,
                "vat_rate": VAT_RATE,
                "vat_amount_eur": vat_amount,
                "sell_price_inc_vat_eur": inc_vat,
                "pricing_floor_ex_vat_eur": floor_ex_vat,
                "pricing_floor_vat_amount_eur": floor_vat_amount,
                "pricing_floor_inc_vat_eur": floor_inc_vat,
                "gap_vs_floor_ex_vat_eur": round_money(r["gap_vs_floor_eur"]) if pd.notna(r.get("gap_vs_floor_eur")) else None,
                "gap_vs_floor_pct": round(float(r["gap_vs_floor_pct"]), 2) if pd.notna(r.get("gap_vs_floor_pct")) else None,
                "market_price_low_eur": round_money(r["market_price_low_eur"]) if pd.notna(r.get("market_price_low_eur")) else None,
                "market_price_median_eur": round_money(r["market_price_median_eur"]) if pd.notna(r.get("market_price_median_eur")) else None,
                  "commercial_market_price_median_eur": round_money(r["commercial_market_price_median_eur"]) if pd.notna(r.get("commercial_market_price_median_eur")) else None,
                "market_price_high_eur": round_money(r["market_price_high_eur"]) if pd.notna(r.get("market_price_high_eur")) else None,
                "gap_vs_market_median_eur": round_money(r["gap_vs_market_median_eur"]) if pd.notna(r.get("gap_vs_market_median_eur")) else None,
                "gap_vs_market_median_pct": round(float(r["gap_vs_market_median_pct"]), 2) if pd.notna(r.get("gap_vs_market_median_pct")) else None,
                "market_check_status": normalize_text(r.get("market_check_status", "")),
                "display_price_rule": decide_display_price_rule(
                    normalize_text(r.get("pricing_position", "")),
                    normalize_text(r.get("launch_recommendation_flag", "")),
                ),
                "pricing_summary_note": normalize_text(r.get("pricing_summary_note", "")),
                "decision_adjustment_note": normalize_text(r.get("decision_adjustment_note", "")),
                "formula_role": "vat_layer_application",
                "formula_placeholder": "sell_price_inc_vat = sell_price_ex_vat * (1 + vat_rate)",
                "linked_upstream_dependency": "final_treatment_pricing_master.csv",
                "cost_scope_included": "recommended_ex_vat_price_plus_output_vat_layer",
                "cost_scope_excluded": "input_vat_reclaim_logic_channel_discount_tax_invoice_rule_final_pos_config",
                "confidence_level": "medium_low",
                "review_status": "needs_finance_tax_signoff",
                "owner_function": "pricing_research",
                "market_context": "Malta",
                "currency": "EUR",
                "effective_from": "2026-01-01",
                "effective_to": "",
                "audit_note": "VAT layer added after internal ex-VAT pricing build; suitable for retail display preparation, still pending final signoff",
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
