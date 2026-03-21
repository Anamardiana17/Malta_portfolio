from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path.cwd()
OUT_DIR = PROJECT_ROOT / "data_processed" / "pricing_research"
OUT_DIR.mkdir(parents=True, exist_ok=True)

CANDIDATE_INPUTS = [
    OUT_DIR / "treatment_material_cost_assumptions.csv",
    OUT_DIR / "treatment_direct_cost_engine.csv",
    OUT_DIR / "treatment_price_recommendation_band_engine.csv",
    OUT_DIR / "treatment_commercial_decision_sheet.csv",
]

OUTPUT_FP = OUT_DIR / "treatment_taxonomy_master.csv"

# Canonical taxonomy policy
CATEGORY_RULES = {
    "aromatherapy": {
        "treatment_family": "massage",
        "canonical_category": "aromatherapy",
        "canonical_variant": "standard",
        "service_role": "premium_core",
        "material_intensity": "medium",
        "labor_skill_level": "standard",
        "ritual_complexity": "medium",
    },
    "body_treatment": {
        "treatment_family": "body_ritual",
        "canonical_category": "body_treatment",
        "canonical_variant": "standard",
        "service_role": "premium_core",
        "material_intensity": "medium_high",
        "labor_skill_level": "standard",
        "ritual_complexity": "medium_high",
    },
    "deep_tissue": {
        "treatment_family": "massage",
        "canonical_category": "deep_tissue",
        "canonical_variant": "standard",
        "service_role": "premium_core",
        "material_intensity": "low_medium",
        "labor_skill_level": "advanced",
        "ritual_complexity": "medium",
    },
    "facial": {
        "treatment_family": "facial",
        "canonical_category": "facial",
        "canonical_variant": "basic",
        "service_role": "premium_core",
        "material_intensity": "high",
        "labor_skill_level": "advanced",
        "ritual_complexity": "high",
    },
    "hot_stone": {
        "treatment_family": "massage",
        "canonical_category": "hot_stone",
        "canonical_variant": "standard",
        "service_role": "premium_core",
        "material_intensity": "medium",
        "labor_skill_level": "advanced",
        "ritual_complexity": "high",
    },
    "massage": {
        "treatment_family": "massage",
        "canonical_category": "massage",
        "canonical_variant": "standard",
        "service_role": "accessible_premium",
        "material_intensity": "low_medium",
        "labor_skill_level": "standard",
        "ritual_complexity": "low_medium",
    },
    "reflexology": {
        "treatment_family": "foot_therapy",
        "canonical_category": "reflexology",
        "canonical_variant": "standard",
        "service_role": "traffic_builder",
        "material_intensity": "low",
        "labor_skill_level": "standard",
        "ritual_complexity": "low",
    },
    "scrub": {
        "treatment_family": "body_ritual",
        "canonical_category": "scrub",
        "canonical_variant": "standard",
        "service_role": "premium_core",
        "material_intensity": "medium_high",
        "labor_skill_level": "standard",
        "ritual_complexity": "medium",
    },
    "swedish": {
        "treatment_family": "massage",
        "canonical_category": "swedish",
        "canonical_variant": "standard",
        "service_role": "premium_core",
        "material_intensity": "low_medium",
        "labor_skill_level": "standard",
        "ritual_complexity": "low_medium",
    },
    "wrap": {
        "treatment_family": "body_ritual",
        "canonical_category": "wrap",
        "canonical_variant": "standard",
        "service_role": "premium_core",
        "material_intensity": "high",
        "labor_skill_level": "standard",
        "ritual_complexity": "high",
    },
    "default": {
        "treatment_family": "other",
        "canonical_category": "other",
        "canonical_variant": "standard",
        "service_role": "premium_core",
        "material_intensity": "medium",
        "labor_skill_level": "standard",
        "ritual_complexity": "medium",
    },
}

VARIANT_NORMALIZATION = {
    "": "standard",
    "basic": "basic",
    "standard": "standard",
    "classic": "standard",
    "signature": "signature",
    "premium": "premium",
}

def normalize_text(x):
    if pd.isna(x):
        return ""
    return str(x).strip()

def normalize_variant(x):
    val = normalize_text(x).lower()
    return VARIANT_NORMALIZATION.get(val, val if val else "standard")

def pick_input():
    for fp in CANDIDATE_INPUTS:
        if fp.exists():
            return fp
    raise FileNotFoundError(f"No candidate input found in: {[str(x) for x in CANDIDATE_INPUTS]}")

def load_input():
    fp = pick_input()
    df = pd.read_csv(fp)
    print(f"[INFO] taxonomy input found: {fp.name} | rows={len(df)}")

    required = {"treatment_category", "treatment_variant", "session_duration_min"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in taxonomy input: {sorted(missing)}")

    df["treatment_category"] = df["treatment_category"].map(normalize_text).str.lower()
    df["treatment_variant"] = df["treatment_variant"].map(normalize_variant)
    df["session_duration_min"] = pd.to_numeric(df["session_duration_min"], errors="coerce")
    df = df.dropna(subset=["session_duration_min"]).copy()
    df["session_duration_min"] = df["session_duration_min"].astype(int)

    df = df[["treatment_category", "treatment_variant", "session_duration_min"]].drop_duplicates().reset_index(drop=True)
    return df, fp.name

def get_rule(category):
    return CATEGORY_RULES.get(category, CATEGORY_RULES["default"])

def build_output():
    df, source_name = load_input()

    rows = []
    for i, r in df.iterrows():
        raw_category = r["treatment_category"]
        raw_variant = r["treatment_variant"]
        duration = int(r["session_duration_min"])

        rule = get_rule(raw_category)

        rows.append(
            {
                "taxonomy_id": f"TTX_{i+1:03d}",
                "variable_block_id": "V2BL_FIX_002",
                "taxonomy_family": "treatment_taxonomy_master",
                "taxonomy_stage": "normalized_master",
                "raw_treatment_category": raw_category,
                "raw_treatment_variant": raw_variant,
                "session_duration_min": duration,
                "treatment_family": rule["treatment_family"],
                "canonical_category": rule["canonical_category"],
                "canonical_variant": raw_variant if raw_variant else rule["canonical_variant"],
                "service_role": rule["service_role"],
                "material_intensity": rule["material_intensity"],
                "labor_skill_level": rule["labor_skill_level"],
                "ritual_complexity": rule["ritual_complexity"],
                "pricing_policy_group": rule["canonical_category"],
                "market_benchmark_group": rule["canonical_category"],
                "cost_policy_group": rule["canonical_category"],
                "taxonomy_join_key": f'{rule["canonical_category"]}__{raw_variant if raw_variant else rule["canonical_variant"]}__{duration}',
                "source_file_name": source_name,
                "market_context": "Malta",
                "status": "taxonomy_defined",
                "audit_note": "taxonomy layer created to stabilize mapping across cost, market, and pricing engines",
            }
        )

    out = pd.DataFrame(rows).sort_values(
        ["canonical_category", "canonical_variant", "session_duration_min"]
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
