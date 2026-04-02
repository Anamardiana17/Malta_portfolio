from __future__ import annotations

from pathlib import Path
import pandas as pd
import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
REGISTRY_FP = REPO_ROOT / "data_processed" / "management" / "management_layer_registry.csv"

REQUIRED_COLUMNS = [
    "artifact_key",
    "artifact_path",
    "artifact_type",
    "management_layer_role",
    "tracked_in_repo_flag",
    "local_only_output_flag",
    "qa_coverage_flag",
    "qa_script_path",
    "source_dependency_class",
    "methodology_boundary_note",
]

FORBIDDEN_TERMS = [
    "pseudo-daypart",
    "roster by hour",
    "roster-by-hour",
    "hourly roster inference",
]

def fail(msg: str) -> None:
    print(f"[FAIL] {msg}")
    sys.exit(1)

def ok(msg: str) -> None:
    print(f"[OK] {msg}")

def main() -> None:
    if not REGISTRY_FP.exists():
        fail(f"Registry file not found: {REGISTRY_FP}")

    df = pd.read_csv(REGISTRY_FP)
    print(f"[OK] Loaded: {REGISTRY_FP}")
    print(f"[INFO] shape={df.shape}")

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        fail(f"Missing required columns: {missing}")
    ok("Required columns present")

    if df.empty:
        fail("Registry is empty")
    ok("Registry is non-empty")

    if df["artifact_key"].duplicated().any():
        dupes = df.loc[df["artifact_key"].duplicated(), "artifact_key"].tolist()
        fail(f"Duplicate artifact_key found: {dupes}")
    ok("artifact_key values are unique")

    for flag_col in ["tracked_in_repo_flag", "local_only_output_flag", "qa_coverage_flag"]:
        bad = ~df[flag_col].isin([0, 1])
        if bad.any():
            fail(f"Invalid binary values in {flag_col}")
    ok("Binary governance flags valid")

    both_bad = (df["tracked_in_repo_flag"] == 1) & (df["local_only_output_flag"] == 1)
    if both_bad.any():
        rows = df.loc[both_bad, "artifact_key"].tolist()
        fail(f"Artifacts cannot be both tracked and local-only: {rows}")
    ok("Tracked/local-only flags consistent")

    output_tracked = df["artifact_path"].astype(str).str.startswith("output/") & (df["tracked_in_repo_flag"] == 1)
    if output_tracked.any():
        rows = df.loc[output_tracked, "artifact_key"].tolist()
        fail(f"output/ artifacts must not be tracked in repo: {rows}")
    ok("output/ local-only rule preserved")

    blank_note = df["methodology_boundary_note"].fillna("").astype(str).str.strip().eq("")
    if blank_note.any():
        rows = df.loc[blank_note, "artifact_key"].tolist()
        fail(f"Blank methodology_boundary_note found: {rows}")
    ok("Boundary notes are non-empty")

    note_text = " ".join(df["methodology_boundary_note"].fillna("").astype(str).str.lower().tolist())
    if "external proxies remain contextual" not in note_text and "external proxies" not in note_text:
        fail("Boundary notes do not clearly preserve external-proxy contextual framing")
    ok("External proxy contextual framing present")

    if "internal operating proxies" not in note_text and "internal" not in note_text:
        fail("Boundary notes do not clearly preserve internal operating anchor framing")
    ok("Internal operating anchor framing present")

    lowered_notes = df["methodology_boundary_note"].fillna("").astype(str).str.lower()
    for term in FORBIDDEN_TERMS:
        bad = lowered_notes.str.contains(term, regex=False)
        if bad.any():
            rows = df.loc[bad, "artifact_key"].tolist()
            fail(f"Forbidden overclaim term '{term}' found in boundary notes: {rows}")
    ok("No forbidden pseudo-hourly / pseudo-daypart overclaim terms found")

    qa_rows = df["qa_coverage_flag"] == 1
    missing_qa_path = df.loc[qa_rows, "qa_script_path"].fillna("").astype(str).str.strip().eq("")
    if missing_qa_path.any():
        rows = df.loc[qa_rows & missing_qa_path, "artifact_key"].tolist()
        fail(f"QA-covered artifacts missing qa_script_path: {rows}")
    ok("QA-covered artifacts declare qa_script_path")

    for _, row in df.iterrows():
        artifact_path = str(row["artifact_path"]).strip()
        if not artifact_path:
            fail(f"Blank artifact_path for {row['artifact_key']}")

        full_artifact_fp = REPO_ROOT / artifact_path
        if not full_artifact_fp.exists():
            fail(f"Declared artifact_path does not exist: {artifact_path}")

        qa_script_path = str(row["qa_script_path"]).strip()
        if int(row["qa_coverage_flag"]) == 1:
            qa_fp = REPO_ROOT / qa_script_path
            if not qa_fp.exists():
                fail(f"Declared qa_script_path does not exist: {qa_script_path}")

    ok("Declared artifact paths and QA script paths exist")

    print("\n=== MANAGEMENT LAYER ROLE DISTRIBUTION ===")
    print(df["management_layer_role"].value_counts(dropna=False).to_string())

    print("\n=== ARTIFACT TYPE DISTRIBUTION ===")
    print(df["artifact_type"].value_counts(dropna=False).to_string())

    print("\n=== TRACKED VS LOCAL-ONLY ===")
    print(df[["tracked_in_repo_flag", "local_only_output_flag"]].value_counts(dropna=False).to_string())

if __name__ == "__main__":
    main()
