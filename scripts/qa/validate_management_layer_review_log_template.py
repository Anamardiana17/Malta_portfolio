from __future__ import annotations

from pathlib import Path
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
TARGET_PATH = REPO_ROOT / "data_processed" / "management" / "management_layer_review_log_template.csv"

REQUIRED_COLUMNS = [
    "review_cycle_id",
    "artifact_name",
    "artifact_type",
    "review_scope",
    "reviewer_name",
    "review_date",
    "review_status",
    "evidence_reference",
    "follow_up_action",
    "handoff_ready_flag",
    "notes",
]

ALLOWED_REVIEW_STATUS = {"pending", "pass", "pass_with_notes", "blocked"}
ALLOWED_HANDOFF_READY = {"yes", "no"}


def main() -> None:
    if not TARGET_PATH.exists():
        raise FileNotFoundError(f"Missing file: {TARGET_PATH}")

    df = pd.read_csv(TARGET_PATH)
    print(f"[OK] Loaded: {TARGET_PATH}")
    print(f"[INFO] shape={df.shape}")

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise AssertionError(f"Missing required columns: {missing}")
    print("[OK] Required columns present")

    if df.empty:
        raise AssertionError("Review log template must not be empty")
    print("[OK] Template is non-empty")

    status_values = set(df["review_status"].dropna().astype(str).str.strip())
    if not status_values.issubset(ALLOWED_REVIEW_STATUS):
        raise AssertionError(
            f"Invalid review_status values: {sorted(status_values - ALLOWED_REVIEW_STATUS)}"
        )
    print("[OK] review_status values valid")

    handoff_values = set(df["handoff_ready_flag"].dropna().astype(str).str.strip())
    if not handoff_values.issubset(ALLOWED_HANDOFF_READY):
        raise AssertionError(
            f"Invalid handoff_ready_flag values: {sorted(handoff_values - ALLOWED_HANDOFF_READY)}"
        )
    print("[OK] handoff_ready_flag values valid")

    template_rows = df["review_cycle_id"].astype(str).str.strip().eq("TEMPLATE").sum()
    if template_rows < 1:
        raise AssertionError("Expected at least one TEMPLATE row for seeded governance usage")
    print("[OK] TEMPLATE seed row present")

    evidence_non_null = (
        df["evidence_reference"].fillna("").astype(str).str.strip().ne("").sum()
    )
    if evidence_non_null < 1:
        raise AssertionError("At least one evidence_reference value must be populated")
    print("[OK] Evidence reference guidance present")

    print("\n=== REVIEW STATUS DISTRIBUTION ===")
    print(df["review_status"].value_counts(dropna=False).to_string())

    print("\n=== HANDOFF READY FLAG DISTRIBUTION ===")
    print(df["handoff_ready_flag"].value_counts(dropna=False).to_string())


if __name__ == "__main__":
    main()
