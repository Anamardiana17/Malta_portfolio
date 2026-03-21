from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path.cwd()
OUT_DIR = PROJECT_ROOT / "data_processed" / "pricing_research"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_FP = OUT_DIR / "loaded_therapist_hourly_cost_engine.csv"
UTIL_POLICY_FP = OUT_DIR / "utilization_assumption_policy.csv"
LABOR_INPUT_FP = OUT_DIR / "therapist_paid_hourly_cost_assumptions.csv"

DEFAULT_PRODUCTIVE_UTILIZATION_RATIO = 0.70

DEFAULT_LABOR_INPUT = [
    {
        "labor_input_id": "LHCIN_001",
        "therapist_role": "junior_therapist",
        "contract_type": "employee",
        "paid_hourly_cost_low_eur": 8.0,
        "paid_hourly_cost_mid_eur": 9.5,
        "paid_hourly_cost_high_eur": 11.0,
        "currency": "EUR",
        "market_context": "Malta",
        "source_note": "fallback_placeholder_no_input_file",
    },
    {
        "labor_input_id": "LHCIN_002",
        "therapist_role": "therapist",
        "contract_type": "employee",
        "paid_hourly_cost_low_eur": 9.0,
        "paid_hourly_cost_mid_eur": 11.0,
        "paid_hourly_cost_high_eur": 13.0,
        "currency": "EUR",
        "market_context": "Malta",
        "source_note": "fallback_placeholder_no_input_file",
    },
    {
        "labor_input_id": "LHCIN_003",
        "therapist_role": "senior_therapist",
        "contract_type": "employee",
        "paid_hourly_cost_low_eur": 11.0,
        "paid_hourly_cost_mid_eur": 13.5,
        "paid_hourly_cost_high_eur": 16.0,
        "currency": "EUR",
        "market_context": "Malta",
        "source_note": "fallback_placeholder_no_input_file",
    },
]

def normalize_text(x):
    if pd.isna(x):
        return ""
    return str(x).strip()

def round_money(x):
    return round(float(x), 2)

def extract_productive_utilization_ratio():
    if not UTIL_POLICY_FP.exists():
        print(f"[WARN] policy file not found: {UTIL_POLICY_FP}")
        print(f"[INFO] fallback productive utilization ratio used: {DEFAULT_PRODUCTIVE_UTILIZATION_RATIO}")
        return DEFAULT_PRODUCTIVE_UTILIZATION_RATIO

    try:
        df = pd.read_csv(UTIL_POLICY_FP, dtype=str).fillna("")
        print(f"[INFO] policy reference found: {UTIL_POLICY_FP.name} | rows={len(df)}")
    except Exception as e:
        print(f"[WARN] could not read policy reference: {e}")
        print(f"[INFO] fallback productive utilization ratio used: {DEFAULT_PRODUCTIVE_UTILIZATION_RATIO}")
        return DEFAULT_PRODUCTIVE_UTILIZATION_RATIO

    candidate_value_cols = ["base_case_value", "value", "default_value", "policy_value"]
    candidate_key_cols = ["util_policy_id", "policy_title", "formula_placeholder", "policy_group"]

    for _, row in df.iterrows():
        joined = " | ".join(normalize_text(row.get(c, "")) for c in candidate_key_cols).lower()
        if (
            "productive utilization definition" in joined
            or "productive_utilization" in joined
            or "paid-to-productive hour conversion" in joined
            or "paid_to_productive_hour_formula" in joined
        ):
            for val_col in candidate_value_cols:
                raw = normalize_text(row.get(val_col, ""))
                try:
                    ratio = float(raw)
                    if 0 < ratio < 1:
                        print(f"[INFO] productive utilization ratio extracted from {val_col}: {ratio}")
                        return ratio
                except Exception:
                    pass

    for val_col in candidate_value_cols:
        if val_col not in df.columns:
            continue
        vals = pd.to_numeric(df[val_col], errors="coerce").dropna()
        vals = vals[(vals > 0) & (vals < 1)]
        if not vals.empty:
            ratio = float(vals.iloc[0])
            print(f"[INFO] productive utilization ratio fallback-extracted from {val_col}: {ratio}")
            return ratio

    print(f"[INFO] no valid policy ratio found; fallback used: {DEFAULT_PRODUCTIVE_UTILIZATION_RATIO}")
    return DEFAULT_PRODUCTIVE_UTILIZATION_RATIO

def load_labor_input():
    if LABOR_INPUT_FP.exists():
        df = pd.read_csv(LABOR_INPUT_FP)
        print(f"[INFO] labor input found: {LABOR_INPUT_FP.name} | rows={len(df)}")

        rename_map = {}
        for c in df.columns:
            cl = c.strip().lower()
            if cl in {"labor_input_id", "input_id"}:
                rename_map[c] = "labor_input_id"
            elif cl in {"therapist_role", "role"}:
                rename_map[c] = "therapist_role"
            elif cl in {"contract_type"}:
                rename_map[c] = "contract_type"
            elif cl in {"paid_hourly_cost_low_eur", "hourly_cost_low_eur"}:
                rename_map[c] = "paid_hourly_cost_low_eur"
            elif cl in {"paid_hourly_cost_mid_eur", "hourly_cost_mid_eur"}:
                rename_map[c] = "paid_hourly_cost_mid_eur"
            elif cl in {"paid_hourly_cost_high_eur", "hourly_cost_high_eur"}:
                rename_map[c] = "paid_hourly_cost_high_eur"
            elif cl in {"currency"}:
                rename_map[c] = "currency"
            elif cl in {"market_context", "market"}:
                rename_map[c] = "market_context"
            elif cl in {"source_note", "audit_note"}:
                rename_map[c] = "source_note"

        df = df.rename(columns=rename_map)

        required = {
            "labor_input_id",
            "therapist_role",
            "contract_type",
            "paid_hourly_cost_low_eur",
            "paid_hourly_cost_mid_eur",
            "paid_hourly_cost_high_eur",
        }
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"Required columns missing in labor input: {sorted(missing)} | file={LABOR_INPUT_FP}")

        keep_cols = [
            "labor_input_id",
            "therapist_role",
            "contract_type",
            "paid_hourly_cost_low_eur",
            "paid_hourly_cost_mid_eur",
            "paid_hourly_cost_high_eur",
        ]
        optional_cols = [c for c in ["currency", "market_context", "source_note"] if c in df.columns]

        out = df[keep_cols + optional_cols].copy()
        for col in ["paid_hourly_cost_low_eur", "paid_hourly_cost_mid_eur", "paid_hourly_cost_high_eur"]:
            out[col] = pd.to_numeric(out[col], errors="coerce")
        out = out.dropna(subset=["paid_hourly_cost_low_eur", "paid_hourly_cost_mid_eur", "paid_hourly_cost_high_eur"]).copy()

        if "currency" not in out.columns:
            out["currency"] = "EUR"
        if "market_context" not in out.columns:
            out["market_context"] = "Malta"
        if "source_note" not in out.columns:
            out["source_note"] = LABOR_INPUT_FP.name

        return out.reset_index(drop=True)

    print(f"[WARN] labor input file not found: {LABOR_INPUT_FP}")
    print("[INFO] fallback labor input table will be used")
    return pd.DataFrame(DEFAULT_LABOR_INPUT)

def build_output():
    productive_utilization_ratio = extract_productive_utilization_ratio()
    labor_df = load_labor_input()

    rows = []
    for i, r in labor_df.iterrows():
        low_paid = float(r["paid_hourly_cost_low_eur"])
        mid_paid = float(r["paid_hourly_cost_mid_eur"])
        high_paid = float(r["paid_hourly_cost_high_eur"])

        low_productive = round_money(low_paid / productive_utilization_ratio)
        mid_productive = round_money(mid_paid / productive_utilization_ratio)
        high_productive = round_money(high_paid / productive_utilization_ratio)

        rows.append(
            {
                "engine_id": f"LHC_{i+1:03d}",
                "variable_block_id": "V2BL_006",
                "engine_family": "loaded_therapist_hourly_cost",
                "engine_stage": "policy_first_placeholder",
                "is_payroll_final": "no",
                "labor_input_id": normalize_text(r["labor_input_id"]),
                "therapist_role": normalize_text(r["therapist_role"]).lower(),
                "contract_type": normalize_text(r["contract_type"]).lower(),
                "paid_hourly_cost_low_eur": round_money(low_paid),
                "paid_hourly_cost_mid_eur": round_money(mid_paid),
                "paid_hourly_cost_high_eur": round_money(high_paid),
                "productive_utilization_ratio": productive_utilization_ratio,
                "loaded_productive_hour_cost_low_eur": low_productive,
                "loaded_productive_hour_cost_mid_eur": mid_productive,
                "loaded_productive_hour_cost_high_eur": high_productive,
                "recommended_loaded_productive_hour_cost_basis": "mid",
                "formula_role": "labor_engine_integration",
                "formula_placeholder": "paid_hourly_cost / productive_utilization_ratio",
                "linked_policy_reference": "utilization_assumption_policy.csv:UTIL_006",
                "linked_upstream_dependency": "therapist_paid_hourly_cost_assumptions.csv",
                "cost_scope_included": "paid_labor_cost_per_paid_hour_converted_to_productive_hour_basis",
                "cost_scope_excluded": "materials_room_cost_equipment_depreciation_admin_overhead_vat",
                "assumption_method": "paid_hour_cost_divided_by_productive_utilization_ratio",
                "confidence_level": "medium_low",
                "review_status": "needs_finance_ops_validation",
                "owner_function": "pricing_research",
                "market_context": normalize_text(r.get("market_context", "Malta")) or "Malta",
                "currency": normalize_text(r.get("currency", "EUR")) or "EUR",
                "effective_from": "2026-01-01",
                "effective_to": "",
                "source_note": normalize_text(r.get("source_note", "")),
                "audit_note": "engine output is policy-first placeholder; use for pricing scenario modeling, not payroll close",
                "status": "assumption_defined",
            }
        )

    out = pd.DataFrame(rows).sort_values(
        ["therapist_role", "contract_type", "labor_input_id"]
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
