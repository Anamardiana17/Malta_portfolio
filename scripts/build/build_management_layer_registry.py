from __future__ import annotations

from pathlib import Path
import pandas as pd


def main() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    out_fp = repo_root / "data_processed" / "management" / "management_layer_registry.csv"
    out_fp.parent.mkdir(parents=True, exist_ok=True)

    rows = [
        {
            "artifact_key": "monthly_roster_management_interpretation",
            "artifact_path": "data_processed/management/monthly_roster_management_interpretation.csv",
            "artifact_type": "dataset",
            "management_layer_role": "decision_interpretation",
            "tracked_in_repo_flag": 1,
            "local_only_output_flag": 0,
            "qa_coverage_flag": 1,
            "qa_script_path": "scripts/qa/validate_monthly_roster_management_interpretation.py",
            "source_dependency_class": "internal_anchor_plus_contextual_external_regime",
            "methodology_boundary_note": "Uses internal operating proxies as primary decision anchor. External proxies remain contextual regime inputs only. No synthetic intra-day staffing segmentation. No hour-level roster inference without valid granular source.",
        },
        {
            "artifact_key": "management_layer_index",
            "artifact_path": "data_processed/management/management_layer_index.md",
            "artifact_type": "documentation_index",
            "management_layer_role": "documentation_index",
            "tracked_in_repo_flag": 1,
            "local_only_output_flag": 0,
            "qa_coverage_flag": 1,
            "qa_script_path": "scripts/qa/validate_management_layer_index.py",
            "source_dependency_class": "registry_derived_documentation",
            "methodology_boundary_note": "Tracked readability and packaging layer derived from the management registry. Documents artifact scope and governance boundaries without adding new modelling logic.",
        },
        {
            "artifact_key": "management_layer_qa_aggregator",
            "artifact_path": "scripts/qa/run_management_layer_qa.py",
            "artifact_type": "qa_orchestration",
            "management_layer_role": "governance_control",
            "tracked_in_repo_flag": 1,
            "local_only_output_flag": 0,
            "qa_coverage_flag": 0,
            "qa_script_path": "",
            "source_dependency_class": "repo_governance",
            "methodology_boundary_note": "QA orchestration layer for management artifacts. Supports governance, packaging discipline, and methodological defensibility.",
        },
        {
            "artifact_key": "monthly_roster_management_readout",
            "artifact_path": "output/management/monthly_roster_management_readout.md",
            "artifact_type": "markdown_readout",
            "management_layer_role": "manager_readout",
            "tracked_in_repo_flag": 0,
            "local_only_output_flag": 1,
            "qa_coverage_flag": 1,
            "qa_script_path": "scripts/qa/validate_monthly_roster_management_markdown_readout.py",
            "source_dependency_class": "derived_readout_from_management_interpretation",
            "methodology_boundary_note": "Local packaging artifact only. Derived from management interpretation layer. Must preserve methodological guardrails and avoid unsupported hour-level staffing claims.",
        },
        {
            "artifact_key": "management_layer_reviewer_checklist",
            "artifact_path": "data_processed/management/management_layer_reviewer_checklist.md",
            "artifact_type": "documentation",
            "management_layer_role": "review_governance",
            "tracked_in_repo_flag": 1,
            "local_only_output_flag": 0,
            "qa_coverage_flag": 1,
            "qa_script_path": "scripts/qa/validate_management_layer_reviewer_checklist.py",
            "source_dependency_class": "repo_governance",
            "methodology_boundary_note": "Reviewer governance artifact only. Supports package integrity, reviewer usability, and methodological defensibility without adding modelling logic or unsupported staffing inference.",
        },
    ]

    df = pd.DataFrame(rows)

    expected_cols = [
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
    df = df[expected_cols].copy()

    for col in ["tracked_in_repo_flag", "local_only_output_flag", "qa_coverage_flag"]:
        df[col] = pd.to_numeric(df[col], errors="raise").astype(int)

    df = df.dropna(subset=["artifact_key"]).copy()

    out_fp.write_text(df.to_csv(index=False), encoding="utf-8")

    print(f"[OK] Wrote: {out_fp}")
    print(f"[INFO] shape={df.shape}")
    print("\n=== MANAGEMENT LAYER REGISTRY PREVIEW ===")
    print(df.to_string(index=False))


if __name__ == "__main__":
    main()
