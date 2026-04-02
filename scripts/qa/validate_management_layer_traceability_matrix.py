from __future__ import annotations

from pathlib import Path
import pandas as pd
import sys


REQUIRED_COLUMNS = [
    "artifact_path",
    "artifact_type",
    "primary_build_script",
    "primary_qa_script",
    "review_sequence_order",
    "reviewer_usage",
    "governance_role",
    "method_boundary_note",
]

EXPECTED_ARTIFACTS = {
    "data_processed/management/management_layer_registry.csv",
    "data_processed/management/management_layer_index.md",
    "data_processed/management/management_layer_package_guide.md",
    "data_processed/management/management_layer_reviewer_checklist.md",
    "data_processed/management/management_layer_traceability_matrix.csv",
}


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def fail(msg: str) -> None:
    print(f"[FAIL] {msg}")
    sys.exit(1)


def ok(msg: str) -> None:
    print(f"[OK] {msg}")


def main() -> None:
    root = repo_root()
    fp = root / "data_processed" / "management" / "management_layer_traceability_matrix.csv"

    if not fp.exists():
        fail(f"Missing file: {fp}")

    df = pd.read_csv(fp)
    ok(f"Loaded: {fp}")
    print(f"[INFO] shape={df.shape}")

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        fail(f"Missing required columns: {missing}")
    ok("Required columns present")

    if df.empty:
        fail("Dataset is empty")
    ok("Dataset is non-empty")

    if df["artifact_path"].duplicated().any():
        fail("Duplicate artifact_path values detected")
    ok("artifact_path values are unique")

    missing_expected = EXPECTED_ARTIFACTS - set(df["artifact_path"].tolist())
    if missing_expected:
        fail(f"Expected artifacts missing: {sorted(missing_expected)}")
    ok("Expected governance artifacts covered")

    seq = pd.to_numeric(df["review_sequence_order"], errors="coerce")
    if seq.isna().any():
        fail("review_sequence_order contains non-numeric values")
    if (seq <= 0).any():
        fail("review_sequence_order must be positive")
    if len(seq.unique()) != len(seq):
        fail("review_sequence_order must be unique")
    ok("review_sequence_order is valid and unique")

    text_cols = [
        "reviewer_usage",
        "governance_role",
        "method_boundary_note",
        "artifact_type",
        "primary_build_script",
        "primary_qa_script",
    ]
    for col in text_cols:
        s = df[col].astype(str).str.strip()
        if s.eq("").any():
            fail(f"{col} contains blank values")
    ok("Text fields are non-empty")

    banned_patterns = [
        "pseudo-daypart",
        "roster-by-hour",
        "hourly roster inference",
        "live production system",
    ]
    boundary_text = " ".join(df["method_boundary_note"].astype(str).tolist()).lower()
    # pseudo-daypart is allowed only if negated / boundary framed
    if "pseudo-daypart" in boundary_text and "does not authorize pseudo-daypart" not in boundary_text:
        fail("Boundary notes mention pseudo-daypart without explicit guardrail framing")
    if "roster-by-hour" in boundary_text and "does not" not in boundary_text:
        fail("Boundary notes mention roster-by-hour without explicit guardrail framing")
    ok("Boundary notes preserve methodological guardrails")

    for col in ["primary_build_script", "primary_qa_script"]:
        for path_str in df[col].astype(str):
            target = root / path_str
            if not target.exists():
                fail(f"Referenced script does not exist: {path_str}")
    ok("Referenced build and QA scripts exist")

    print("\n=== TRACEABILITY MATRIX PREVIEW ===")
    print(df[["review_sequence_order", "artifact_type", "artifact_path"]].to_string(index=False))


if __name__ == "__main__":
    main()
