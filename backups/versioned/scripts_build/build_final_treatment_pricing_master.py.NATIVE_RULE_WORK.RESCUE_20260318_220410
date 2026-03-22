from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path.cwd()
OUT_DIR = PROJECT_ROOT / "data_processed" / "pricing_research"
OUT_DIR.mkdir(parents=True, exist_ok=True)

INPUT_FP = OUT_DIR / "treatment_market_validation_sheet.csv"
OUTPUT_FP = OUT_DIR / "final_treatment_pricing_master.csv"


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
            "Run scripts/build/build_treatment_market_validation_sheet.py first."
        )

    df = pd.read_csv(INPUT_FP)
    print(f"[INFO] market validation input found: {INPUT_FP.name} | rows={len(df)}")

    required = {
        "market_validation_id",
        "commercial_decision_id",
        "treatment_category",
        "treatment_variant",
        "session_duration_min",
        "pricing_position",
        "recommended_basis_used",
        "recommended_sell_price_eur",
        "pricing_floor_mid_eur",
        "gap_vs_floor_eur",
        "gap_vs_floor_pct",
        "market_price_median_eur",
        "gap_vs_market_median_eur",
        "gap_vs_market_median_pct",
        "market_check_status",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in market validation input: {sorted(missing)}")

    numeric_cols = [
        "session_duration_min",
        "recommended_sell_price_eur",
        "pricing_floor_mid_eur",
        "gap_vs_floor_eur",
        "gap_vs_floor_pct",
        "market_price_low_eur",
        "market_price_median_eur",
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


def decide_launch_flag(pricing_position, gap_vs_market_median_pct, market_check_status):
    if pd.isna(gap_vs_market_median_pct):
        if pricing_position == "traffic_builder":
            return "launch_as_entry_price"
        if pricing_position == "accessible_premium":
            return "launch_with_soft_validation"
        return "launch_with_market_check"

    if pricing_position == "traffic_builder":
        if gap_vs_market_median_pct <= -10:
            return "launch_as_entry_price"
        return "launch_with_value_message"

    if pricing_position == "accessible_premium":
        if gap_vs_market_median_pct > 10:
            return "review_before_launch"
        if gap_vs_market_median_pct < -15:
            return "launch_with_upside_room"
        return "launch"

    if pricing_position == "premium_core":
        if gap_vs_market_median_pct > 15:
            return "review_before_launch"
        if gap_vs_market_median_pct < -15:
            return "launch_with_upside_room"
        return "launch"

    if market_check_status == "in_line_with_market_median":
        return "launch"
    return "launch_with_review"


def decide_price_architecture_flag(pricing_position, gap_vs_market_median_pct):
    if pricing_position == "traffic_builder":
        return "traffic_builder"
    if pricing_position == "accessible_premium":
        return "accessible_premium"
    if pd.notna(gap_vs_market_median_pct) and gap_vs_market_median_pct >= 10:
        return "premium_edge"
    return "premium_core"


def decide_execution_priority(pricing_position, gap_vs_market_median_pct):
    if pricing_position == "traffic_builder":
        return "high"
    if pd.isna(gap_vs_market_median_pct):
        return "medium"
    if abs(gap_vs_market_median_pct) >= 15:
        return "high"
    return "medium"


def build_summary_note(row):
    pos = normalize_text(row["pricing_position"])
    market_status = normalize_text(row["market_check_status"])
    price = round_money(row["recommended_sell_price_eur"])
    floor = round_money(row["pricing_floor_mid_eur"])

    if market_status == "above_market_median":
        market_note = "priced above market median"
    elif market_status == "below_market_median":
        market_note = "priced below market median"
    elif market_status == "in_line_with_market_median":
        market_note = "priced broadly in line with market median"
    else:
        market_note = "market median not yet validated"

    if pos == "traffic_builder":
        pos_note = "traffic-oriented entry role"
    elif pos == "accessible_premium":
        pos_note = "accessible premium role"
    else:
        pos_note = "premium core role"

    return f"Recommended sell price {price:.2f} EUR vs floor {floor:.2f} EUR; {market_note}; {pos_note}."


def is_wrap_60_standard(row):
    return (
        normalize_text(row.get("treatment_category", "")).lower() == "wrap"
        and normalize_text(row.get("treatment_variant", "")).lower() == "standard"
        and pd.to_numeric(row.get("session_duration_min"), errors="coerce") == 60
    )


def build_output():
    df = load_input()

    rows = []
    for i, r in df.iterrows():
        pricing_position = normalize_text(r["pricing_position"])
        gap_vs_market_pct = pd.to_numeric(r["gap_vs_market_median_pct"], errors="coerce")

        launch_flag = decide_launch_flag(
            pricing_position=pricing_position,
            gap_vs_market_median_pct=gap_vs_market_pct,
            market_check_status=normalize_text(r["market_check_status"]),
        )
        architecture_flag = decide_price_architecture_flag(
            pricing_position=pricing_position,
            gap_vs_market_median_pct=gap_vs_market_pct,
        )
        execution_priority = decide_execution_priority(
            pricing_position=pricing_position,
            gap_vs_market_median_pct=gap_vs_market_pct,
        )

        is_wrap_rule = is_wrap_60_standard(r)

        commercial_market_price_median_eur = (
            72.0 if is_wrap_rule else (
                round_money(r["market_price_median_eur"])
                if pd.notna(r["market_price_median_eur"]) else None
            )
        )

        recommended_sell_price_eur = (
            82.0 if is_wrap_rule else round_money(r["recommended_sell_price_eur"])
        )

        commercial_decision_basis = (
            "review_controlled_wrap_rule" if is_wrap_rule else "standard_market_validation_flow"
        )

        governance_status = (
            "review_required" if is_wrap_rule else "standard_review_flow"
        )

        governance_note = (
            "wrap exact market fact locked at 72.00 EUR; commercial recommended sell price set to 82.00 EUR under review-controlled governance"
            if is_wrap_rule else ""
        )

        benchmark_methodology_status = (
            "exact_only_locked" if is_wrap_rule else normalize_text(r.get("market_check_status", ""))
        )

        rows.append(
            {
                "pricing_master_id": f"FTPM_{i+1:03d}",
                "variable_block_id": "V2BL_011",
                "master_family": "final_treatment_pricing_master",
                "master_stage": "policy_first_placeholder",
                "is_final_approved_price": "no",
                "market_validation_id": normalize_text(r["market_validation_id"]),
                "commercial_decision_id": normalize_text(r["commercial_decision_id"]),
                "treatment_category": normalize_text(r["treatment_category"]).lower(),
                "treatment_variant": normalize_text(r["treatment_variant"]).lower(),
                "session_duration_min": int(r["session_duration_min"]),
                "pricing_position": pricing_position,
                "recommended_basis_used": normalize_text(r["recommended_basis_used"]),
                "recommended_sell_price_eur": recommended_sell_price_eur,
                "pricing_floor_mid_eur": round_money(r["pricing_floor_mid_eur"]),
                "gap_vs_floor_eur": round_money(r["gap_vs_floor_eur"]),
                "gap_vs_floor_pct": round(float(r["gap_vs_floor_pct"]), 2) if pd.notna(r["gap_vs_floor_pct"]) else None,
                "market_price_low_eur": round_money(r["market_price_low_eur"]) if pd.notna(r.get("market_price_low_eur")) else None,
                "market_price_median_eur": round_money(r["market_price_median_eur"]) if pd.notna(r["market_price_median_eur"]) else None,
                "commercial_market_price_median_eur": commercial_market_price_median_eur,
                "market_price_high_eur": round_money(r["market_price_high_eur"]) if pd.notna(r.get("market_price_high_eur")) else None,
                "gap_vs_market_median_eur": round_money(r["gap_vs_market_median_eur"]) if pd.notna(r["gap_vs_market_median_eur"]) else None,
                "gap_vs_market_median_pct": round(float(r["gap_vs_market_median_pct"]), 2) if pd.notna(r["gap_vs_market_median_pct"]) else None,
                "market_check_status": normalize_text(r["market_check_status"]),
                "benchmark_methodology_status": benchmark_methodology_status,
                "commercial_decision_basis": commercial_decision_basis,
                "governance_status": governance_status,
                "governance_note": governance_note,
                "price_architecture_flag": architecture_flag,
                "launch_recommendation_flag": launch_flag,
                "execution_priority": execution_priority,
                "benchmark_source_note": normalize_text(r.get("benchmark_source_note", "")),
                "pricing_summary_note": build_summary_note(r),
                "decision_adjustment_note": normalize_text(r.get("decision_adjustment_note", "")),
                "formula_role": "final_pricing_master_rollup",
                "formula_placeholder": "roll up internal recommendation, floor view, and market validation into one master sheet",
                "linked_upstream_dependency": "treatment_market_validation_sheet.csv",
                "cost_scope_included": "recommended_price_floor_and_market_validation_rollup",
                "cost_scope_excluded": "channel_discount_tax_display_rule_final_psychological_rounding_live_launch_experiment",
                "confidence_level": "medium_low",
                "review_status": "needs_management_signoff",
                "owner_function": "pricing_research",
                "market_context": "Malta",
                "currency": "EUR",
                "effective_from": "2026-01-01",
                "effective_to": "",
                "audit_note": "final pricing master is still pre-approval and depends on final competitor refinement and commercial signoff",
                "status": "assumption_defined",
            }
        )

    out = pd.DataFrame(rows).sort_values(
        ["treatment_category", "treatment_variant", "session_duration_min"]
    ).reset_index(drop=True)

    return out


def main():
    out = build_output()

    # --- wrap native governance propagation ---
    mask_wrap = (
        out["treatment_category"].astype(str).str.strip().str.lower().eq("wrap")
        & out["treatment_variant"].astype(str).str.strip().str.lower().eq("standard")
        & out["session_duration_min"].astype(float).eq(60)
    )

    out.loc[mask_wrap, "launch_recommendation_flag"] = "review_before_launch"

    if "review_status" in out.columns:
        out.loc[mask_wrap, "review_status"] = "needs_management_signoff"

    if "governance_status" in out.columns:
        out.loc[mask_wrap, "governance_status"] = "review_required"

    if "commercial_decision_basis" in out.columns:
        out.loc[mask_wrap, "commercial_decision_basis"] = "review_controlled_wrap_rule"

    if "governance_note" in out.columns:
        out.loc[mask_wrap, "governance_note"] = (
            "wrap exact market fact locked at 72.00 EUR; commercial recommended sell price set to 82.00 EUR under review-controlled governance"
        )

    if "audit_note" in out.columns:
        out.loc[mask_wrap, "audit_note"] = (
            out.loc[mask_wrap, "audit_note"].astype(str)
            + " | wrap native governance rule applied: market fact locked at 72.00 EUR; sell price held at 82.00 EUR; launch remains review-controlled"
        )
    out.to_csv(OUTPUT_FP, index=False)

    print(f"[OK] saved: {OUTPUT_FP}")
    print(f"[OK] rows: {len(out)}")
    print("\nPreview:")
    print(out.head(20).to_string(index=False))


if __name__ == "__main__":
    main()
