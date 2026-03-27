from pathlib import Path
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
OUT_FP = REPO_ROOT / "data_processed" / "management" / "management_layer_traceability_matrix.csv"

COMMON_BOUNDARY_NOTE = (
    "Governance artifact only; "
    "external proxies remain contextual only; "
    "internal operating proxies remain the primary anchor for actionable decisions; "
    "does not authorize pseudo-daypart; "
    "does not authorize hour-level roster inference without valid granular source support."
)

def main():
    rows = [
        {
            "artifact_name": "monthly_roster_management_interpretation.csv",
            "artifact_path": "data_processed/management/monthly_roster_management_interpretation.csv",
            "artifact_type": "management_readout_dataset",
            "primary_build_script": "scripts/build/build_monthly_roster_management_interpretation.py",
            "primary_qa_script": "scripts/qa/validate_monthly_roster_management_interpretation.py",
            "review_sequence_order": 1,
            "reviewer_usage": "validate core management interpretation layer first",
            "governance_role": "core_interpretation_anchor",
            "method_boundary_note": COMMON_BOUNDARY_NOTE,
        },
        {
            "artifact_name": "management_layer_registry.csv",
            "artifact_path": "data_processed/management/management_layer_registry.csv",
            "artifact_type": "governance_registry",
            "primary_build_script": "scripts/build/build_management_layer_registry.py",
            "primary_qa_script": "scripts/qa/validate_management_layer_registry.py",
            "review_sequence_order": 2,
            "reviewer_usage": "confirm tracked governance artifact inventory",
            "governance_role": "artifact_inventory_control",
            "method_boundary_note": COMMON_BOUNDARY_NOTE,
        },
        {
            "artifact_name": "management_layer_index.md",
            "artifact_path": "data_processed/management/management_layer_index.md",
            "artifact_type": "governance_index",
            "primary_build_script": "scripts/build/build_management_layer_index.py",
            "primary_qa_script": "scripts/qa/validate_management_layer_index.py",
            "review_sequence_order": 3,
            "reviewer_usage": "navigate package structure and artifact references",
            "governance_role": "reviewer_navigation_control",
            "method_boundary_note": COMMON_BOUNDARY_NOTE,
        },
        {
            "artifact_name": "management_layer_package_guide.md",
            "artifact_path": "data_processed/management/management_layer_package_guide.md",
            "artifact_type": "governance_guide",
            "primary_build_script": "scripts/build/build_management_layer_package_guide.py",
            "primary_qa_script": "scripts/qa/validate_management_layer_package_guide.py",
            "review_sequence_order": 4,
            "reviewer_usage": "read package boundary and reviewer guidance",
            "governance_role": "handoff_and_boundary_guidance",
            "method_boundary_note": COMMON_BOUNDARY_NOTE,
        },
        {
            "artifact_name": "management_layer_reviewer_checklist.md",
            "artifact_path": "data_processed/management/management_layer_reviewer_checklist.md",
            "artifact_type": "governance_checklist",
            "primary_build_script": "scripts/build/build_management_layer_reviewer_checklist.py",
            "primary_qa_script": "scripts/qa/validate_management_layer_reviewer_checklist.py",
            "review_sequence_order": 5,
            "reviewer_usage": "perform structured governance review",
            "governance_role": "review_process_control",
            "method_boundary_note": COMMON_BOUNDARY_NOTE,
        },
        {
            "artifact_name": "management_layer_traceability_matrix.csv",
            "artifact_path": "data_processed/management/management_layer_traceability_matrix.csv",
            "artifact_type": "governance_traceability_matrix",
            "primary_build_script": "scripts/build/build_management_layer_traceability_matrix.py",
            "primary_qa_script": "scripts/qa/validate_management_layer_traceability_matrix.py",
            "review_sequence_order": 6,
            "reviewer_usage": "map artifacts to QA and governance roles",
            "governance_role": "traceability_and_coverage_control",
            "method_boundary_note": COMMON_BOUNDARY_NOTE,
        },
        {
            "artifact_name": "management_layer_release_readiness_note.md",
            "artifact_path": "data_processed/management/management_layer_release_readiness_note.md",
            "artifact_type": "governance_release_note",
            "primary_build_script": "scripts/build/build_management_layer_release_readiness_note.py",
            "primary_qa_script": "scripts/qa/validate_management_layer_release_readiness_note.py",
            "review_sequence_order": 7,
            "reviewer_usage": "assess release posture and handoff trust note",
            "governance_role": "release_readiness_and_packaging_trust",
            "method_boundary_note": COMMON_BOUNDARY_NOTE,
        },
        {
            "artifact_name": "management_layer_governance_changelog.md",
            "artifact_path": "data_processed/management/management_layer_governance_changelog.md",
            "artifact_type": "governance_changelog",
            "primary_build_script": "scripts/build/build_management_layer_governance_changelog.py",
            "primary_qa_script": "scripts/qa/validate_management_layer_governance_changelog.py",
            "review_sequence_order": 8,
            "reviewer_usage": "review governance evolution and artifact-level changes",
            "governance_role": "governance_change_control",
            "method_boundary_note": COMMON_BOUNDARY_NOTE,
        },
        {
            "artifact_name": "management_layer_artifact_lifecycle_policy.md",
            "artifact_path": "data_processed/management/management_layer_artifact_lifecycle_policy.md",
            "artifact_type": "governance_policy",
            "primary_build_script": "scripts/build/build_management_layer_artifact_lifecycle_policy.py",
            "primary_qa_script": "scripts/qa/validate_management_layer_artifact_lifecycle_policy.py",
            "review_sequence_order": 9,
            "reviewer_usage": "confirm artifact retention, local-only handling, and repo discipline",
            "governance_role": "artifact_lifecycle_control",
            "method_boundary_note": COMMON_BOUNDARY_NOTE,
        },
        {
            "artifact_name": "management_layer_governance_manifest.md",
            "artifact_path": "data_processed/management/management_layer_governance_manifest.md",
            "artifact_type": "governance_manifest",
            "primary_build_script": "scripts/build/build_management_layer_governance_manifest.py",
            "primary_qa_script": "scripts/qa/validate_management_layer_governance_manifest.py",
            "review_sequence_order": 10,
            "reviewer_usage": "confirm active governance stack scope and control posture",
            "governance_role": "governance_scope_control",
            "method_boundary_note": COMMON_BOUNDARY_NOTE,
        },
        {
            "artifact_name": "management_layer_reviewer_quickstart.md",
            "artifact_path": "data_processed/management/management_layer_reviewer_quickstart.md",
            "artifact_type": "governance_quickstart",
            "primary_build_script": "scripts/build/build_management_layer_reviewer_quickstart.py",
            "primary_qa_script": "scripts/qa/validate_management_layer_reviewer_quickstart.py",
            "review_sequence_order": 11,
            "reviewer_usage": "start review quickly without bypassing governance boundaries",
            "governance_role": "reviewer_onboarding_control",
            "method_boundary_note": COMMON_BOUNDARY_NOTE,
        },
        {
            "artifact_name": "management_layer_review_log_template.csv",
            "artifact_path": "data_processed/management/management_layer_review_log_template.csv",
            "artifact_type": "governance_review_log_template",
            "primary_build_script": "scripts/build/build_management_layer_review_log_template.py",
            "primary_qa_script": "scripts/qa/validate_management_layer_review_log_template.py",
            "review_sequence_order": 12,
            "reviewer_usage": "record reviewer action, evidence reference, status, and handoff readiness",
            "governance_role": "review_execution_evidence_capture",
            "method_boundary_note": COMMON_BOUNDARY_NOTE,
        },
    ]

    df = pd.DataFrame(rows)
    df.to_csv(OUT_FP, index=False)
    print(f"[OK] Wrote: {OUT_FP}")
    print(f"[INFO] shape={df.shape}")

if __name__ == "__main__":
    main()
