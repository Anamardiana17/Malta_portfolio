from pathlib import Path
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
OUT_FP = REPO_ROOT / "data_processed" / "management" / "management_layer_registry.csv"

COMMON_BOUNDARY_NOTE = (
    "External proxies remain contextual regime or market-pressure inputs only; "
    "internal operating proxies remain the primary anchor for actionable decisions; "
    "no unsupported intraday deployment logic is introduced; "
    "no unsupported hourly staffing inference is introduced without valid granular source support; "
    "output/ remains local-only and not tracked."
)

def main():
    rows = [
        {
            "artifact_key": "monthly_roster_management_interpretation",
            "artifact_name": "monthly_roster_management_interpretation.csv",
            "artifact_type": "management_readout_dataset",
            "artifact_path": "data_processed/management/monthly_roster_management_interpretation.csv",
            "management_layer_role": "core_management_interpretation_dataset",
            "tracked_in_repo_flag": 1,
            "local_only_output_flag": 0,
            "qa_coverage_flag": 1,
            "qa_script_path": "scripts/qa/validate_monthly_roster_management_interpretation.py",
            "source_dependency_class": "internal_proxy_anchor_with_contextual_external_regime",
            "methodology_boundary_note": COMMON_BOUNDARY_NOTE,
        },
        {
            "artifact_key": "management_layer_registry",
            "artifact_name": "management_layer_registry.csv",
            "artifact_type": "governance_registry",
            "artifact_path": "data_processed/management/management_layer_registry.csv",
            "management_layer_role": "governance_artifact_inventory",
            "tracked_in_repo_flag": 1,
            "local_only_output_flag": 0,
            "qa_coverage_flag": 1,
            "qa_script_path": "scripts/qa/validate_management_layer_registry.py",
            "source_dependency_class": "governance_metadata",
            "methodology_boundary_note": COMMON_BOUNDARY_NOTE,
        },
        {
            "artifact_key": "management_layer_index",
            "artifact_name": "management_layer_index.md",
            "artifact_type": "governance_index",
            "artifact_path": "data_processed/management/management_layer_index.md",
            "management_layer_role": "reviewer_navigation_index",
            "tracked_in_repo_flag": 1,
            "local_only_output_flag": 0,
            "qa_coverage_flag": 1,
            "qa_script_path": "scripts/qa/validate_management_layer_index.py",
            "source_dependency_class": "governance_metadata",
            "methodology_boundary_note": COMMON_BOUNDARY_NOTE,
        },
        {
            "artifact_key": "management_layer_package_guide",
            "artifact_name": "management_layer_package_guide.md",
            "artifact_type": "governance_guide",
            "artifact_path": "data_processed/management/management_layer_package_guide.md",
            "management_layer_role": "package_handoff_guidance",
            "tracked_in_repo_flag": 1,
            "local_only_output_flag": 0,
            "qa_coverage_flag": 1,
            "qa_script_path": "scripts/qa/validate_management_layer_package_guide.py",
            "source_dependency_class": "governance_metadata",
            "methodology_boundary_note": COMMON_BOUNDARY_NOTE,
        },
        {
            "artifact_key": "management_layer_reviewer_checklist",
            "artifact_name": "management_layer_reviewer_checklist.md",
            "artifact_type": "governance_checklist",
            "artifact_path": "data_processed/management/management_layer_reviewer_checklist.md",
            "management_layer_role": "reviewer_control_checklist",
            "tracked_in_repo_flag": 1,
            "local_only_output_flag": 0,
            "qa_coverage_flag": 1,
            "qa_script_path": "scripts/qa/validate_management_layer_reviewer_checklist.py",
            "source_dependency_class": "governance_metadata",
            "methodology_boundary_note": COMMON_BOUNDARY_NOTE,
        },
        {
            "artifact_key": "management_layer_traceability_matrix",
            "artifact_name": "management_layer_traceability_matrix.csv",
            "artifact_type": "governance_traceability_matrix",
            "artifact_path": "data_processed/management/management_layer_traceability_matrix.csv",
            "management_layer_role": "artifact_traceability_map",
            "tracked_in_repo_flag": 1,
            "local_only_output_flag": 0,
            "qa_coverage_flag": 1,
            "qa_script_path": "scripts/qa/validate_management_layer_traceability_matrix.py",
            "source_dependency_class": "governance_metadata",
            "methodology_boundary_note": COMMON_BOUNDARY_NOTE,
        },
        {
            "artifact_key": "management_layer_release_readiness_note",
            "artifact_name": "management_layer_release_readiness_note.md",
            "artifact_type": "governance_release_note",
            "artifact_path": "data_processed/management/management_layer_release_readiness_note.md",
            "management_layer_role": "release_readiness_and_handoff_trust_note",
            "tracked_in_repo_flag": 1,
            "local_only_output_flag": 0,
            "qa_coverage_flag": 1,
            "qa_script_path": "scripts/qa/validate_management_layer_release_readiness_note.py",
            "source_dependency_class": "governance_metadata",
            "methodology_boundary_note": COMMON_BOUNDARY_NOTE,
        },
        {
            "artifact_key": "management_layer_governance_changelog",
            "artifact_name": "management_layer_governance_changelog.md",
            "artifact_type": "governance_changelog",
            "artifact_path": "data_processed/management/management_layer_governance_changelog.md",
            "management_layer_role": "governance_evolution_and_handoff_trust_log",
            "tracked_in_repo_flag": 1,
            "local_only_output_flag": 0,
            "qa_coverage_flag": 1,
            "qa_script_path": "scripts/qa/validate_management_layer_governance_changelog.py",
            "source_dependency_class": "governance_metadata",
            "methodology_boundary_note": COMMON_BOUNDARY_NOTE,
        },
    ]

    df = pd.DataFrame(rows)
    df.to_csv(OUT_FP, index=False)
    print(f"[OK] Wrote: {OUT_FP}")
    print(f"[INFO] shape={df.shape}")
    print(f"[INFO] columns={list(df.columns)}")

if __name__ == "__main__":
    main()
