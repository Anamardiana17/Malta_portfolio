from __future__ import annotations

from pathlib import Path
import pandas as pd


BASE = Path(__file__).resolve().parents[2]
STAFF_FP = BASE / "data_processed" / "management" / "staff_commercial_scoring_layer.csv"
THER_FP = BASE / "data_processed" / "management" / "therapist_top_bottom_performance_layer.csv"


def fail(msg: str) -> None:
    raise SystemExit(f"[FAIL] {msg}")


def check_score(df: pd.DataFrame, col: str) -> None:
    s = pd.to_numeric(df[col], errors="coerce")
    if s.isna().any():
        fail(f"{col} contains non-numeric/null values")
    if ((s < 0) | (s > 100)).any():
        fail(f"{col} must stay within 0-100")


def main() -> None:
    if not STAFF_FP.exists():
        fail(f"Missing file: {STAFF_FP}")
    if not THER_FP.exists():
        fail(f"Missing file: {THER_FP}")

    staff = pd.read_csv(STAFF_FP)
    ther = pd.read_csv(THER_FP)

    print(f"[OK] Loaded: {STAFF_FP}")
    print(f"[INFO] staff shape={staff.shape}")
    print(f"[OK] Loaded: {THER_FP}")
    print(f"[INFO] therapist shape={ther.shape}")

    if staff.empty:
        fail("staff_commercial_scoring_layer is empty")
    if ther.empty:
        fail("therapist_top_bottom_performance_layer is empty")
    print("[OK] Datasets are non-empty")

    required_staff = [
        "staff_role",
        "retail_revenue_eur",
        "retail_units_sold",
        "retail_attach_rate",
        "retail_selling_score_0_100",
        "retail_reward_eligibility_flag",
    ]
    missing_staff = [c for c in required_staff if c not in staff.columns]
    if missing_staff:
        fail(f"Missing staff columns: {missing_staff}")

    required_ther = [
        "upsell_score_0_100",
        "total_commercial_score_0_100",
        "top3_therapist_flag",
        "bottom3_therapist_flag",
        "refresh_training_required_flag",
        "bonus_reward_eligibility_flag",
        "therapist_count_within_outlet_month",
        "top_group_cutoff",
        "bottom_group_cutoff",
    ]
    missing_ther = [c for c in required_ther if c not in ther.columns]
    if missing_ther:
        fail(f"Missing therapist columns: {missing_ther}")
    print("[OK] Required columns present")

    expected_roles = {
        "spa receptionist",
        "spa attendant",
        "spa assistant manager",
        "spa manager",
    }
    role_set = set(staff["staff_role"].astype(str))
    if role_set != expected_roles:
        fail(f"Unexpected staff role set: {sorted(role_set)}")
    print("[OK] Staff role set valid")

    count_by_outlet = (
        staff.groupby(["month_id", "outlet_name", "staff_role"], as_index=False)
        .size()
    )
    expected_counts = {
        "spa receptionist": 3,
        "spa attendant": 3,
        "spa assistant manager": 1,
        "spa manager": 1,
    }
    for role, exp in expected_counts.items():
        sub = count_by_outlet[count_by_outlet["staff_role"] == role]
        if sub.empty:
            fail(f"No rows found for role: {role}")
        if (sub["size"] != exp).any():
            fail(f"Role count mismatch for {role}; expected {exp} per outlet-month")
    print("[OK] Staff coverage counts valid")

    check_score(staff, "retail_selling_score_0_100")
    check_score(ther, "upsell_score_0_100")
    check_score(ther, "total_commercial_score_0_100")
    check_score(ther, "service_quality_guardrail_score_0_100")
    print("[OK] Score columns valid")

    for col in ["retail_reward_eligibility_flag"]:
        vals = set(pd.to_numeric(staff[col], errors="coerce").fillna(-1).astype(int))
        if not vals.issubset({0, 1}):
            fail(f"{col} must be binary")

    for col in ["top3_therapist_flag", "bottom3_therapist_flag", "refresh_training_required_flag", "bonus_reward_eligibility_flag"]:
        vals = set(pd.to_numeric(ther[col], errors="coerce").fillna(-1).astype(int))
        if not vals.issubset({0, 1}):
            fail(f"{col} must be binary")
    print("[OK] Binary flags valid")

    combo = ther.groupby(["month_id", "outlet_id"])
    for (month_id, outlet_id), g in combo:
        n = int(g["therapist_count_within_outlet_month"].iloc[0])
        expected_top = 3 if n >= 6 else 1
        expected_bottom = 3 if n >= 6 else 1
        top_n = int(g["top3_therapist_flag"].sum())
        bottom_n = int(g["bottom3_therapist_flag"].sum())
        if top_n != expected_top:
            fail(f"Expected {expected_top} top-group therapists for {month_id} {outlet_id}, found {top_n}")
        if bottom_n != expected_bottom:
            fail(f"Expected {expected_bottom} bottom-group therapists for {month_id} {outlet_id}, found {bottom_n}")
    print("[OK] Top/bottom therapist logic valid")

    if ther["refresh_training_reason"].astype(str).str.strip().eq("").any():
        fail("refresh_training_reason contains blanks")
    if ther["coaching_action_recommendation"].astype(str).str.strip().eq("").any():
        fail("coaching_action_recommendation contains blanks")
    print("[OK] Decision text populated")

    if int(pd.to_numeric(ther["bonus_reward_eligibility_flag"], errors="coerce").fillna(0).sum()) <= 0:
        fail("Therapist bonus reward eligibility should not be zero across the entire dataset")
    print("[OK] Therapist reward logic produces at least some eligible rows")

    print("\n=== STAFF ROLE COUNTS ===")
    print(staff["staff_role"].value_counts().to_string())

    print("\n=== THERAPIST TOP/BOTTOM FLAG COUNTS ===")
    print("top3_therapist_flag:", int(pd.to_numeric(ther["top3_therapist_flag"], errors="coerce").fillna(0).sum()))
    print("bottom3_therapist_flag:", int(pd.to_numeric(ther["bottom3_therapist_flag"], errors="coerce").fillna(0).sum()))
    print("refresh_training_required_flag:", int(pd.to_numeric(ther["refresh_training_required_flag"], errors="coerce").fillna(0).sum()))
    print("bonus_reward_eligibility_flag:", int(pd.to_numeric(ther["bonus_reward_eligibility_flag"], errors="coerce").fillna(0).sum()))

    print("\n[OK] staff commercial scoring validation passed")


if __name__ == "__main__":
    main()
