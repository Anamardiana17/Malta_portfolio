from pathlib import Path
import pandas as pd


REQUIRED_COLUMNS = [
    "escalation_code",
    "qa_trigger_type",
    "severity_band",
    "release_action",
    "owner_role",
    "reviewer_action_required",
    "evidence_required",
    "rerun_requirement",
    "closure_condition",
    "handoff_note",
]

VALID_TRIGGER_TYPES = {
    "validator_failure",
    "registry_drift",
    "traceability_drift",
    "missing_artifact",
    "stale_handoff_metadata",
    "manual_review_override",
    "format_integrity_issue",
}

VALID_SEVERITY_BANDS = {"blocker", "high", "medium", "low"}

VALID_RELEASE_ACTIONS = {
    "block_release",
    "hold_pending_fix",
    "allow_with_note",
    "monitor_only",
}

VALID_OWNER_ROLES = {
    "artifact_owner",
    "reviewer",
    "repo_maintainer",
    "qa_owner",
}


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)
    print(f"[OK] {message}")


def _contains_reference(path: Path, needle: str) -> bool:
    if not path.exists():
        return False
    return needle.lower() in path.read_text(encoding="utf-8").lower()


def main() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    artifact_path = (
        repo_root
        / "data_processed"
        / "management"
        / "management_layer_qa_escalation_matrix.csv"
    )
    registry_path = (
        repo_root
        / "data_processed"
        / "management"
        / "management_layer_registry.csv"
    )
    index_path = (
        repo_root
        / "data_processed"
        / "management"
        / "management_layer_index.md"
    )
    traceability_path = (
        repo_root
        / "data_processed"
        / "management"
        / "management_layer_traceability_matrix.csv"
    )

    _assert(artifact_path.exists(), f"Artifact exists: {artifact_path}")

    df = pd.read_csv(artifact_path)
    print(f"[INFO] shape={df.shape}")

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    _assert(len(missing) == 0, "Required columns present")
    _assert(not df.empty, "Dataset is non-empty")

    _assert(df["escalation_code"].notna().all(), "Escalation codes are non-null")
    _assert(df["escalation_code"].astype(str).str.strip().ne("").all(), "Escalation codes are non-blank")
    _assert(df["escalation_code"].is_unique, "Escalation codes are unique")

    _assert(
        set(df["qa_trigger_type"].dropna().astype(str)).issubset(VALID_TRIGGER_TYPES),
        "Trigger types valid",
    )
    _assert(
        set(df["severity_band"].dropna().astype(str)).issubset(VALID_SEVERITY_BANDS),
        "Severity bands valid",
    )
    _assert(
        set(df["release_action"].dropna().astype(str)).issubset(VALID_RELEASE_ACTIONS),
        "Release actions valid",
    )
    _assert(
        set(df["owner_role"].dropna().astype(str)).issubset(VALID_OWNER_ROLES),
        "Owner roles valid",
    )

    text_cols = [
        "reviewer_action_required",
        "evidence_required",
        "rerun_requirement",
        "closure_condition",
        "handoff_note",
    ]
    for col in text_cols:
        _assert(df[col].notna().all(), f"{col} is non-null")
        _assert(df[col].astype(str).str.strip().ne("").all(), f"{col} is non-blank")

    blocker_rows = df[df["severity_band"] == "blocker"]
    _assert(not blocker_rows.empty, "At least one blocker pathway exists")
    _assert(
        (blocker_rows["release_action"] == "block_release").all(),
        "All blocker pathways enforce block_release",
    )

    high_rows = df[df["severity_band"].isin(["blocker", "high"])]
    rerun_text = high_rows["rerun_requirement"].astype(str).str.lower()

    _assert(
        (
            rerun_text.str.contains("run_management_layer_qa.py", regex=False)
            | rerun_text.str.contains("full management-layer qa", regex=False)
            | rerun_text.str.contains("full management layer qa", regex=False)
            | rerun_text.str.contains("full qa", regex=False)
        ).all(),
        "High-severity rows require full management-layer QA rerun",
    )

    _assert(
        _contains_reference(registry_path, "management_layer_qa_escalation_matrix.csv"),
        "Registry references QA escalation matrix",
    )
    _assert(
        _contains_reference(index_path, "qa escalation matrix")
        or _contains_reference(index_path, "management_layer_qa_escalation_matrix.csv"),
        "Index references QA escalation matrix",
    )
    _assert(
        _contains_reference(traceability_path, "management_layer_qa_escalation_matrix.csv")
        or _contains_reference(traceability_path, "qa escalation matrix"),
        "Traceability matrix references QA escalation matrix",
    )

    print("\n=== SEVERITY DISTRIBUTION ===")
    print(df["severity_band"].value_counts(dropna=False).to_string())

    print("\n=== RELEASE ACTION DISTRIBUTION ===")
    print(df["release_action"].value_counts(dropna=False).to_string())

    print("\n[SUCCESS] management_layer_qa_escalation_matrix validation passed")


if __name__ == "__main__":
    main()
