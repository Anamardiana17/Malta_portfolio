from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path.cwd()
OUT_DIR = PROJECT_ROOT / "data_processed" / "pricing_research"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_FP = OUT_DIR / "treatment_material_cost_assumptions.csv"
UTIL_POLICY_FP = OUT_DIR / "utilization_assumption_policy.csv"

CANDIDATE_SOURCE_FILES = [
    OUT_DIR / "price_recommendation_interpretation_v1.csv",
    OUT_DIR / "treatment_pricing_research_master.csv",
    OUT_DIR / "treatment_pricing_master.csv",
    OUT_DIR / "spa_service_menu_clean.csv",
    OUT_DIR / "service_menu_clean.csv",
]

CATEGORY_RULES = {
    "facial": {
        "material_cost_low_eur": 4.50,
        "material_cost_mid_eur": 7.50,
        "material_cost_high_eur": 11.50,
        "main_cost_driver": "skincare_product_intensity",
        "consumable_profile": "mask_serum_cleanser_toner_spatula_linen_laundry_share",
        "assumption_basis": "facials usually consume more SKU variety and product volume per session",
    },
    "body_treatment": {
        "material_cost_low_eur": 4.00,
        "material_cost_mid_eur": 6.50,
        "material_cost_high_eur": 10.00,
        "main_cost_driver": "wrap_scrub_oil_product_load",
        "consumable_profile": "scrub_wrap_oil_disposables_linen_laundry_share",
        "assumption_basis": "body rituals often require moderate-to-high product quantity and linen use",
    },
    "aromatherapy": {
        "material_cost_low_eur": 2.50,
        "material_cost_mid_eur": 4.50,
        "material_cost_high_eur": 7.00,
        "main_cost_driver": "oil_blend_and_linen_use",
        "consumable_profile": "massage_oil_essential_oil_disposables_linen_laundry_share",
        "assumption_basis": "oil-led treatments typically have lower SKU count but recurring oil and linen consumption",
    },
    "deep_tissue": {
        "material_cost_low_eur": 2.50,
        "material_cost_mid_eur": 4.00,
        "material_cost_high_eur": 6.00,
        "main_cost_driver": "oil_or_balm_and_linen_use",
        "consumable_profile": "massage_oil_or_balm_disposables_linen_laundry_share",
        "assumption_basis": "deep tissue generally uses simpler materials than facial or wrap categories",
    },
    "massage": {
        "material_cost_low_eur": 2.50,
        "material_cost_mid_eur": 4.00,
        "material_cost_high_eur": 6.00,
        "main_cost_driver": "oil_or_balm_and_linen_use",
        "consumable_profile": "massage_oil_or_balm_disposables_linen_laundry_share",
        "assumption_basis": "general massage assumed similar to other oil/balm treatments",
    },
    "swedish": {
        "material_cost_low_eur": 2.50,
        "material_cost_mid_eur": 4.50,
        "material_cost_high_eur": 7.00,
        "main_cost_driver": "oil_blend_and_linen_use",
        "consumable_profile": "massage_oil_disposables_linen_laundry_share",
        "assumption_basis": "swedish massage typically carries light-to-moderate oil usage with standard linen load",
    },
    "hot_stone": {
        "material_cost_low_eur": 3.00,
        "material_cost_mid_eur": 5.00,
        "material_cost_high_eur": 8.00,
        "main_cost_driver": "oil_plus_stone_preparation_and_linen_use",
        "consumable_profile": "massage_oil_stone_setup_hygiene_linen_laundry_share",
        "assumption_basis": "hot stone uses oil plus additional setup, sanitation, and ritual prep burden",
    },
    "reflexology": {
        "material_cost_low_eur": 1.50,
        "material_cost_mid_eur": 2.50,
        "material_cost_high_eur": 4.00,
        "main_cost_driver": "minimal_product_plus_hygiene_consumables",
        "consumable_profile": "cream_or_oil_disposables_hygiene_share",
        "assumption_basis": "reflexology usually has lighter product consumption than full-body treatments",
    },
    "scrub": {
        "material_cost_low_eur": 3.50,
        "material_cost_mid_eur": 5.50,
        "material_cost_high_eur": 8.50,
        "main_cost_driver": "product_mass_per_session",
        "consumable_profile": "scrub_base_oil_disposables_linen_laundry_share",
        "assumption_basis": "scrubs consume visible product mass and cleanup materials",
    },
    "wrap": {
        "material_cost_low_eur": 4.50,
        "material_cost_mid_eur": 7.50,
        "material_cost_high_eur": 11.00,
        "main_cost_driver": "product_mass_and_wrap_materials",
        "consumable_profile": "mask_wrap_materials_disposables_linen_laundry_share",
        "assumption_basis": "wraps combine product load with extra application/covering materials",
    },
    "default": {
        "material_cost_low_eur": 2.50,
        "material_cost_mid_eur": 4.50,
        "material_cost_high_eur": 7.00,
        "main_cost_driver": "generic_consumables_and_product_load",
        "consumable_profile": "generic_product_disposables_linen_laundry_share",
        "assumption_basis": "fallback placeholder until treatment-specific BOM is available",
    },
}

DURATION_MULTIPLIERS = [
    (45, 0.85),
    (60, 1.00),
    (75, 1.15),
    (90, 1.30),
    (120, 1.55),
]

ROUND_TO = 0.5

def round_to_step(x: float, step: float = 0.5) -> float:
    return round(x / step) * step

def normalize_text(x):
    if pd.isna(x):
        return ""
    return str(x).strip()

def find_source_file():
    for fp in CANDIDATE_SOURCE_FILES:
        if fp.exists():
            return fp
    return None

def infer_treatment_master():
    src = find_source_file()

    if src is None:
        starter = pd.DataFrame(
            [
                ["aromatherapy", "standard", 60],
                ["body_treatment", "standard", 60],
                ["deep_tissue", "standard", 60],
                ["facial", "basic", 60],
                ["hot_stone", "standard", 75],
                ["massage", "standard", 60],
                ["reflexology", "standard", 45],
                ["scrub", "standard", 45],
                ["swedish", "standard", 60],
                ["wrap", "standard", 60],
            ],
            columns=["treatment_category", "treatment_variant", "session_duration_min"],
        )
        starter["source_file_used"] = "fallback_starter_set"
        return starter

    df = pd.read_csv(src)

    rename_map = {}
    for c in df.columns:
        cl = c.strip().lower()
        if cl in {"treatment_category", "category", "service_category"}:
            rename_map[c] = "treatment_category"
        elif cl in {"treatment_variant", "variant", "service_variant"}:
            rename_map[c] = "treatment_variant"
        elif cl in {"session_duration_min", "duration_min", "duration_minutes"}:
            rename_map[c] = "session_duration_min"

    df = df.rename(columns=rename_map)

    required = {"treatment_category", "treatment_variant", "session_duration_min"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Required columns missing: {sorted(missing)} | file={src}")

    out = (
        df.loc[:, ["treatment_category", "treatment_variant", "session_duration_min"]]
        .copy()
        .drop_duplicates()
    )
    out["treatment_category"] = out["treatment_category"].map(normalize_text).str.lower()
    out["treatment_variant"] = out["treatment_variant"].map(normalize_text).str.lower()
    out["session_duration_min"] = pd.to_numeric(out["session_duration_min"], errors="coerce")
    out = out.dropna(subset=["session_duration_min"]).copy()
    out["session_duration_min"] = out["session_duration_min"].astype(int)
    out["source_file_used"] = src.name
    out = out.sort_values(["treatment_category", "treatment_variant", "session_duration_min"]).reset_index(drop=True)
    return out

def get_duration_multiplier(minutes: int) -> float:
    exact = dict(DURATION_MULTIPLIERS)
    if minutes in exact:
        return exact[minutes]

    points = sorted(DURATION_MULTIPLIERS, key=lambda x: x[0])

    if minutes <= points[0][0]:
        return points[0][1]
    if minutes >= points[-1][0]:
        extra = minutes - points[-1][0]
        return round(points[-1][1] + (extra / 60.0) * 0.25, 4)

    for i in range(len(points) - 1):
        x1, y1 = points[i]
        x2, y2 = points[i + 1]
        if x1 <= minutes <= x2:
            ratio = (minutes - x1) / (x2 - x1)
            return round(y1 + ratio * (y2 - y1), 4)

    return 1.0

def rule_for_category(cat: str) -> dict:
    cat = normalize_text(cat).lower()
    return CATEGORY_RULES.get(cat, CATEGORY_RULES["default"])

def validate_policy_file():
    if not UTIL_POLICY_FP.exists():
        print(f"[WARN] policy reference not found: {UTIL_POLICY_FP}")
        return

    try:
        policy_df = pd.read_csv(UTIL_POLICY_FP, dtype=str).fillna("")
        print(f"[INFO] policy reference found: {UTIL_POLICY_FP.name} | rows={len(policy_df)}")
    except Exception as e:
        print(f"[WARN] could not read policy reference: {e}")

def build_output():
    base = infer_treatment_master()

    rows = []
    for _, r in base.iterrows():
        cat = r["treatment_category"]
        variant = r["treatment_variant"]
        minutes = int(r["session_duration_min"])
        src_name = r["source_file_used"]

        rule = rule_for_category(cat)
        dur_mult = get_duration_multiplier(minutes)

        low = round_to_step(rule["material_cost_low_eur"] * dur_mult, ROUND_TO)
        mid = round_to_step(rule["material_cost_mid_eur"] * dur_mult, ROUND_TO)
        high = round_to_step(rule["material_cost_high_eur"] * dur_mult, ROUND_TO)

        rows.append(
            {
                "assumption_id": f"TMAT_{len(rows)+1:03d}",
                "variable_block_id": "V2BL_005",
                "assumption_family": "treatment_material_cost",
                "assumption_stage": "policy_first_placeholder",
                "is_true_bom_final": "no",
                "treatment_category": cat,
                "treatment_variant": variant,
                "session_duration_min": minutes,
                "material_cost_low_eur": low,
                "material_cost_mid_eur": mid,
                "material_cost_high_eur": high,
                "recommended_material_cost_basis": "mid",
                "duration_multiplier_applied": dur_mult,
                "main_cost_driver": rule["main_cost_driver"],
                "consumable_profile": rule["consumable_profile"],
                "cost_scope_included": "direct_treatment_consumables_only",
                "cost_scope_excluded": "labor_room_occupancy_equipment_depreciation_overhead_vat",
                "assumption_basis": rule["assumption_basis"],
                "assumption_method": "category_policy_x_duration_multiplier",
                "linked_policy_reference": "utilization_assumption_policy.csv",
                "linked_upstream_dependency": "treatment_master_placeholder",
                "confidence_level": "medium_low",
                "review_status": "needs_finance_ops_validation",
                "owner_function": "pricing_research",
                "market_context": "Malta",
                "currency": "EUR",
                "effective_from": "2026-01-01",
                "effective_to": "",
                "source_file_used_for_treatment_list": src_name,
                "audit_note": "placeholder assumption only; created before final BOM; use for scenario modeling, not purchase control",
                "status": "assumption_defined",
            }
        )

    out = pd.DataFrame(rows).sort_values(
        ["treatment_category", "treatment_variant", "session_duration_min"]
    ).reset_index(drop=True)

    return out

def main():
    validate_policy_file()
    out = build_output()
    out.to_csv(OUTPUT_FP, index=False)

    print(f"[OK] saved: {OUTPUT_FP}")
    print(f"[OK] rows: {len(out)}")
    print("\nPreview:")
    print(out.head(20).to_string(index=False))

if __name__ == "__main__":
    main()
