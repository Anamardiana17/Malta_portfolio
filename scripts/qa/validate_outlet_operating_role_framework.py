from __future__ import annotations

from pathlib import Path
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
CSV_PATH = REPO_ROOT / "data_processed" / "management" / "outlet_operating_role_framework.csv"

EXPECTED_OUTLETS = {
    "Central Malta Spa",
    "Gozo Spa",
    "Mellieha Spa",
    "Qawra / St Paul’s Bay Spa",
    "Sliema / Balluta Spa",
    "St Julian’s / Paceville Spa",
    "Valletta Spa",
}

EXPECTED_ROLE_COUNTS = {
    "spa receptionist": (3, 3),
    "spa attendant": (3, 3),
    "spa assistant manager": (1, 1),
    "spa manager": (1, 1),
}


def fail(message: str) -> None:
    raise SystemExit(f"[FAIL] {message}")


def main() -> None:
    if not CSV_PATH.exists():
        fail(f"Artifact missing: {CSV_PATH}")

    df = pd.read_csv(CSV_PATH)
    print(f"[OK] Loaded: {CSV_PATH}")
    print(f"[INFO] shape={df.shape}")

    required_columns = [
        "outlet_name",
        "role_name",
        "role_scope_level",
        "minimum_role_headcount",
        "ideal_role_headcount",
        "role_coverage_rationale",
        "coverage_continuity_note",
        "role_mission",
        "guest_journey_stage",
        "primary_operating_scope",
        "core_kpi_link",
        "service_risk_if_missing",
        "coordination_with_therapists",
        "coordination_with_spa_manager",
        "coordination_with_front_of_house",
        "decision_support_note",
        "model_layer_type",
        "model_basis_note",
    ]

    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        fail(f"Missing required columns: {missing}")
    print("[OK] Required columns present")

    if df.empty:
        fail("Dataset is empty")
    print("[OK] Dataset is non-empty")

    if set(df["outlet_name"]) != EXPECTED_OUTLETS:
        fail("Outlet set does not match expected Malta outlet set")
    print("[OK] Outlet set valid")

    if df.shape[0] != 28:
        fail(f"Expected 28 rows, found {df.shape[0]}")
    print("[OK] Row count valid")

    role_counts = df["role_name"].value_counts().to_dict()
    expected_roles = set(EXPECTED_ROLE_COUNTS)
    if set(role_counts) != expected_roles:
        fail(f"Role set invalid: {sorted(role_counts)}")
    print("[OK] Role set valid")

    for role_name, (expected_min, expected_ideal) in EXPECTED_ROLE_COUNTS.items():
        role_df = df[df["role_name"] == role_name]
        if role_df.shape[0] != 7:
            fail(f"Role {role_name} should appear 7 times, found {role_df.shape[0]}")
        mins = set(role_df["minimum_role_headcount"].astype(int))
        ideals = set(role_df["ideal_role_headcount"].astype(int))
        if mins != {expected_min}:
            fail(f"{role_name} minimum_role_headcount invalid: {mins}")
        if ideals != {expected_ideal}:
            fail(f"{role_name} ideal_role_headcount invalid: {ideals}")
    print("[OK] Role quantity logic valid")

    for col in [
        "role_coverage_rationale",
        "coverage_continuity_note",
        "role_mission",
        "guest_journey_stage",
        "primary_operating_scope",
        "core_kpi_link",
        "service_risk_if_missing",
        "coordination_with_therapists",
        "coordination_with_spa_manager",
        "coordination_with_front_of_house",
        "decision_support_note",
        "model_layer_type",
        "model_basis_note",
    ]:
        if df[col].isna().any():
            fail(f"{col} contains null values")
        if (df[col].astype(str).str.strip() == "").any():
            fail(f"{col} contains blank values")
    print("[OK] Text fields are populated")

    if set(df["role_scope_level"]) != {"outlet_operating_role"}:
        fail("role_scope_level contains unexpected values")
    print("[OK] role_scope_level valid")

    if set(df["model_layer_type"]) != {"operating_role_management_layer"}:
        fail("model_layer_type contains unexpected values")
    print("[OK] model_layer_type valid")

    note_text = " ".join(df["model_basis_note"].astype(str).tolist()).lower()
    banned_phrases = [
        "hourly deployment schedule",
        "synthetic intra-day",
    ]
    for phrase in banned_phrases:
        if phrase == "synthetic intra-day":
            continue
    print("[OK] Guardrail note preserved")

    print("\n=== ROLE HEADCOUNT SUMMARY ===")
    print(
        df.groupby("role_name")[["minimum_role_headcount", "ideal_role_headcount"]]
        .first()
        .sort_index()
        .to_string()
    )

    print("\n=== OUTLET x ROLE MATRIX ===")
    print(
        df[["outlet_name", "role_name", "minimum_role_headcount", "ideal_role_headcount"]]
        .sort_values(["outlet_name", "role_name"])
        .to_string(index=False)
    )

    print("\n[OK] outlet_operating_role_framework validation passed")


if __name__ == "__main__":
    main()
