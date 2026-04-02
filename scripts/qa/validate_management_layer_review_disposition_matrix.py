from __future__ import annotations

from pathlib import Path
import sys
import pandas as pd


EXPECTED_CODES = {
    "accept",
    "accept_with_followup",
    "revise_before_release",
    "block_release",
}

EXPECTED_RELEASE_ELIGIBILITY = {
    "eligible",
    "eligible_with_followup",
    "not_eligible_until_revised",
    "blocked",
}

EXPECTED_YES_NO_OPTIONAL = {"yes", "no", "optional"}
EXPECTED_SEVERITY = {"none", "low", "medium", "high"}


def fail(msg: str) -> None:
    print(f"[FAIL] {msg}")
    sys.exit(1)


def main() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    fp = repo_root / "data_processed" / "management" / "management_layer_review_disposition_matrix.csv"

    if not fp.exists():
        fail(f"Missing file: {fp}")

    df = pd.read_csv(fp)
    print(f"[OK] Loaded: {fp}")
    print(f"[INFO] shape={df.shape}")

    required_cols = [
        "review_disposition_code",
        "review_disposition_label",
        "finding_severity",
        "release_eligibility",
        "requires_artifact_change",
        "requires_build_rerun",
        "requires_qa_rerun",
        "reviewer_evidence_expectation",
        "handoff_implication",
        "audit_note_guidance",
    ]

    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        fail(f"Missing required columns: {missing}")
    print("[OK] Required columns present")

    if df.empty:
        fail("Dataset is empty")
    print("[OK] Dataset is non-empty")

    codes = set(df["review_disposition_code"].dropna().astype(str))
    if codes != EXPECTED_CODES:
        fail(f"Unexpected disposition codes: {sorted(codes)}")
    print("[OK] Disposition codes valid")

    if df["review_disposition_code"].duplicated().any():
        fail("Duplicate review_disposition_code values found")
    print("[OK] Disposition codes unique")

    severity = set(df["finding_severity"].dropna().astype(str))
    if severity != EXPECTED_SEVERITY:
        fail(f"Unexpected finding_severity values: {sorted(severity)}")
    print("[OK] Finding severity values valid")

    release_vals = set(df["release_eligibility"].dropna().astype(str))
    if release_vals != EXPECTED_RELEASE_ELIGIBILITY:
        fail(f"Unexpected release_eligibility values: {sorted(release_vals)}")
    print("[OK] Release eligibility values valid")

    for col in ["requires_artifact_change", "requires_build_rerun", "requires_qa_rerun"]:
        vals = set(df[col].dropna().astype(str))
        if not vals.issubset(EXPECTED_YES_NO_OPTIONAL):
            fail(f"Unexpected values in {col}: {sorted(vals)}")
    print("[OK] Boolean/optional governance flags valid")

    text_cols = [
        "review_disposition_label",
        "reviewer_evidence_expectation",
        "handoff_implication",
        "audit_note_guidance",
    ]
    for col in text_cols:
        series = df[col].fillna("").astype(str).str.strip()
        if (series == "").any():
            fail(f"Blank text found in column: {col}")
    print("[OK] Required narrative fields non-empty")

    ordered = df["review_disposition_code"].tolist()
    expected_order = [
        "accept",
        "accept_with_followup",
        "revise_before_release",
        "block_release",
    ]
    if ordered != expected_order:
        fail(f"Unexpected row order: {ordered}")
    print("[OK] Disposition ordering stable")

    print("\n=== REVIEW DISPOSITION COUNTS ===")
    print(df["review_disposition_code"].value_counts(dropna=False).to_string())

    print("\n[PASS] management_layer_review_disposition_matrix validation passed")


if __name__ == "__main__":
    main()
