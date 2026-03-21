from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path.cwd()
OUT_DIR = PROJECT_ROOT / "data_processed" / "pricing_research"
OUT_DIR.mkdir(parents=True, exist_ok=True)

MATERIAL_FP = OUT_DIR / "treatment_material_cost_assumptions.csv"
LABOR_FP = OUT_DIR / "loaded_therapist_hourly_cost_engine.csv"
TAXONOMY_FP = OUT_DIR / "treatment_taxonomy_master.csv"
OUTPUT_FP = OUT_DIR / "treatment_direct_cost_engine.csv"

DEFAULT_ROLE_MAPPING = {
    "facial": "senior_therapist",
    "body_treatment": "therapist",
    "aromatherapy": "therapist",
    "deep_tissue": "senior_therapist",
    "massage": "therapist",
    "swedish": "therapist",
    "hot_stone": "senior_therapist",
    "reflexology": "junior_therapist",
    "scrub": "therapist",
    "wrap": "therapist",
    "default": "therapist",
}

TAXONOMY_SKILL_TO_ROLE = {
    "advanced": "senior_therapist",
    "standard": "therapist",
    "junior": "junior_therapist",
}

def normalize_text(x):
    if pd.isna(x):
        return ""
    return str(x).strip()

def round_money(x):
    return round(float(x), 2)

def load_material_input():
    if not MATERIAL_FP.exists():
        raise FileNotFoundError(
            f"Required file not found: {MATERIAL_FP}\n"
            "Run scripts/build/build_treatment_material_cost_assumptions.py first."
        )

    df = pd.read_csv(MATERIAL_FP)
    print(f"[INFO] material input found: {MATERIAL_FP.name} | rows={len(df)}")

    required = {
        "assumption_id",
        "treatment_category",
        "treatment_variant",
        "session_duration_min",
        "material_cost_low_eur",
        "material_cost_mid_eur",
        "material_cost_high_eur",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in material input: {sorted(missing)}")

    for col in [
        "session_duration_min",
        "material_cost_low_eur",
        "material_cost_mid_eur",
        "material_cost_high_eur",
    ]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(
        subset=[
            "session_duration_min",
            "material_cost_low_eur",
            "material_cost_mid_eur",
            "material_cost_high_eur",
        ]
    ).copy()

    df["treatment_category"] = df["treatment_category"].map(normalize_text).str.lower()
    df["treatment_variant"] = df["treatment_variant"].map(normalize_text).str.lower()
    df["session_duration_min"] = df["session_duration_min"].astype(int)

    return df.reset_index(drop=True)

def canonicalize_labor_columns(df):
    rename_map = {}

    for c in df.columns:
        cl = c.strip().lower()

        if cl in {"engine_id", "loaded_cost_id", "labor_engine_id"}:
            rename_map[c] = "engine_id"
        elif cl in {"therapist_role", "role", "staff_role"}:
            rename_map[c] = "therapist_role"
        elif cl in {
            "loaded_productive_hour_cost_low_eur",
            "productive_hour_cost_low_eur",
            "loaded_hour_cost_low_eur",
            "hour_cost_low_eur",
            "productive_cost_low_eur",
        }:
            rename_map[c] = "loaded_productive_hour_cost_low_eur"
        elif cl in {
            "loaded_productive_hour_cost_mid_eur",
            "productive_hour_cost_mid_eur",
            "loaded_hour_cost_mid_eur",
            "hour_cost_mid_eur",
            "productive_cost_mid_eur",
        }:
            rename_map[c] = "loaded_productive_hour_cost_mid_eur"
        elif cl in {
            "loaded_productive_hour_cost_high_eur",
            "productive_hour_cost_high_eur",
            "loaded_hour_cost_high_eur",
            "hour_cost_high_eur",
            "productive_cost_high_eur",
        }:
            rename_map[c] = "loaded_productive_hour_cost_high_eur"
        elif cl in {"contract_type"}:
            rename_map[c] = "contract_type"
        elif cl in {"labor_input_id", "input_id"}:
            rename_map[c] = "labor_input_id"

    df = df.rename(columns=rename_map)

    if "engine_id" not in df.columns:
        df["engine_id"] = [f"LHC_{i+1:03d}" for i in range(len(df))]

    if "therapist_role" not in df.columns:
        possible_role_cols = [c for c in df.columns if "role" in c.lower()]
        if possible_role_cols:
            df["therapist_role"] = df[possible_role_cols[0]]
        else:
            df["therapist_role"] = "therapist"

    return df

def load_labor_input():
    if not LABOR_FP.exists():
        raise FileNotFoundError(
            f"Required file not found: {LABOR_FP}\n"
            "Run scripts/build/build_loaded_therapist_hourly_cost_engine.py first."
        )

    df = pd.read_csv(LABOR_FP)
    print(f"[INFO] labor engine found: {LABOR_FP.name} | rows={len(df)}")
    print(f"[INFO] raw labor columns: {list(df.columns)}")

    df = canonicalize_labor_columns(df)
    print(f"[INFO] canonical labor columns: {list(df.columns)}")

    required = {
        "engine_id",
        "therapist_role",
        "loaded_productive_hour_cost_low_eur",
        "loaded_productive_hour_cost_mid_eur",
        "loaded_productive_hour_cost_high_eur",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"Missing required columns in labor engine after canonicalization: {sorted(missing)}"
        )

    for col in [
        "loaded_productive_hour_cost_low_eur",
        "loaded_productive_hour_cost_mid_eur",
        "loaded_productive_hour_cost_high_eur",
    ]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["therapist_role"] = df["therapist_role"].map(normalize_text).str.lower()

    df = df.dropna(
        subset=[
            "loaded_productive_hour_cost_low_eur",
            "loaded_productive_hour_cost_mid_eur",
            "loaded_productive_hour_cost_high_eur",
        ]
    ).copy()

    if "contract_type" not in df.columns:
        df["contract_type"] = ""

    if "labor_input_id" not in df.columns:
        df["labor_input_id"] = ""

    sort_cols = [c for c in ["therapist_role", "contract_type", "labor_input_id"] if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols).copy()

    df = df.drop_duplicates(subset=["therapist_role"], keep="first").reset_index(drop=True)
    return df

def load_taxonomy_lookup():
    if not TAXONOMY_FP.exists():
        print(f"[WARN] taxonomy file not found: {TAXONOMY_FP}")
        return {}

    df = pd.read_csv(TAXONOMY_FP)
    print(f"[INFO] taxonomy input found: {TAXONOMY_FP.name} | rows={len(df)}")

    required = {
        "canonical_category",
        "canonical_variant",
        "session_duration_min",
        "labor_skill_level",
    }
    missing = required - set(df.columns)
    if missing:
        print(f"[WARN] taxonomy file missing columns: {sorted(missing)}")
        return {}

    df["canonical_category"] = df["canonical_category"].map(normalize_text).str.lower()
    df["canonical_variant"] = df["canonical_variant"].map(normalize_text).str.lower()
    df["session_duration_min"] = pd.to_numeric(df["session_duration_min"], errors="coerce")
    df = df.dropna(subset=["session_duration_min"]).copy()
    df["session_duration_min"] = df["session_duration_min"].astype(int)
    df["labor_skill_level"] = df["labor_skill_level"].map(normalize_text).str.lower()

    lookup = {}
    for _, r in df.iterrows():
        key = (r["canonical_category"], r["canonical_variant"], int(r["session_duration_min"]))
        lookup[key] = r["labor_skill_level"]
    return lookup

def map_role(treatment_category: str, treatment_variant: str, session_duration_min: int, taxonomy_lookup: dict):
    key = (
        normalize_text(treatment_category).lower(),
        normalize_text(treatment_variant).lower(),
        int(session_duration_min),
    )

    labor_skill = taxonomy_lookup.get(key, "")
    if labor_skill in TAXONOMY_SKILL_TO_ROLE:
        return TAXONOMY_SKILL_TO_ROLE[labor_skill], "taxonomy_labor_skill_mapping"

    cat_key = normalize_text(treatment_category).lower()
    return DEFAULT_ROLE_MAPPING.get(cat_key, DEFAULT_ROLE_MAPPING["default"]), "default_category_role_mapping"

def build_output():
    material_df = load_material_input()
    labor_df = load_labor_input()
    taxonomy_lookup = load_taxonomy_lookup()

    labor_lookup = labor_df.set_index("therapist_role").to_dict(orient="index")

    rows = []
    for i, r in material_df.iterrows():
        treatment_category = r["treatment_category"]
        treatment_variant = r["treatment_variant"]
        session_duration_min = int(r["session_duration_min"])

        mapped_role, role_mapping_method = map_role(
            treatment_category=treatment_category,
            treatment_variant=treatment_variant,
            session_duration_min=session_duration_min,
            taxonomy_lookup=taxonomy_lookup,
        )

        labor_row = labor_lookup.get(mapped_role)

        if labor_row is None:
            if "therapist" in labor_lookup:
                labor_row = labor_lookup["therapist"]
                mapped_role = "therapist"
                role_mapping_method = "fallback_generic_therapist"
            else:
                raise ValueError(
                    f"No labor row found for mapped therapist role '{mapped_role}'. "
                    f"Available roles: {sorted(labor_lookup.keys())}"
                )

        duration_hours = session_duration_min / 60.0

        labor_low = round_money(labor_row["loaded_productive_hour_cost_low_eur"] * duration_hours)
        labor_mid = round_money(labor_row["loaded_productive_hour_cost_mid_eur"] * duration_hours)
        labor_high = round_money(labor_row["loaded_productive_hour_cost_high_eur"] * duration_hours)

        material_low = round_money(r["material_cost_low_eur"])
        material_mid = round_money(r["material_cost_mid_eur"])
        material_high = round_money(r["material_cost_high_eur"])

        direct_low = round_money(material_low + labor_low)
        direct_mid = round_money(material_mid + labor_mid)
        direct_high = round_money(material_high + labor_high)

        rows.append(
            {
                "direct_cost_id": f"TDC_{i+1:03d}",
                "engine_family": "treatment_direct_cost",
                "engine_stage": "policy_first_placeholder",
                "is_final_costing_model": "no",
                "treatment_material_assumption_id": normalize_text(r["assumption_id"]),
                "labor_engine_id": normalize_text(labor_row["engine_id"]),
                "treatment_category": treatment_category,
                "treatment_variant": treatment_variant,
                "session_duration_min": session_duration_min,
                "assigned_therapist_role": mapped_role,
                "role_mapping_method": role_mapping_method,
                "duration_hours": round(duration_hours, 4),
                "material_cost_low_eur": material_low,
                "material_cost_mid_eur": material_mid,
                "material_cost_high_eur": material_high,
                "productive_labor_cost_low_eur": labor_low,
                "productive_labor_cost_mid_eur": labor_mid,
                "productive_labor_cost_high_eur": labor_high,
                "direct_cost_low_eur": direct_low,
                "direct_cost_mid_eur": direct_mid,
                "direct_cost_high_eur": direct_high,
                "recommended_direct_cost_basis": "mid",
                "labor_assignment_method": role_mapping_method,
                "formula_role": "direct_cost_integration",
                "formula_placeholder": "material_cost + (loaded_productive_hour_cost x duration_hours)",
                "linked_material_reference": "treatment_material_cost_assumptions.csv",
                "linked_labor_reference": "loaded_therapist_hourly_cost_engine.csv",
                "linked_taxonomy_reference": "treatment_taxonomy_master.csv",
                "cost_scope_included": "direct_treatment_materials_plus_productive_labor_only",
                "cost_scope_excluded": "room_occupancy_equipment_depreciation_admin_overhead_sales_marketing_vat_profit_markup",
                "confidence_level": "medium_low",
                "review_status": "needs_finance_ops_validation",
                "owner_function": "pricing_research",
                "market_context": "Malta",
                "currency": "EUR",
                "effective_from": "2026-01-01",
                "effective_to": "",
                "audit_note": "policy-first integrated direct cost placeholder; labor role now supports taxonomy-based mapping",
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
