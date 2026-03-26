from __future__ import annotations

from pathlib import Path
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]

TRACKED_ARTIFACTS = [
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
        "methodology_boundary_note": (
            "Uses internal operating proxies as primary decision anchor. "
            "External proxies remain contextual regime inputs only. "
            "No synthetic intra-day staffing segmentation. No hour-level roster inference without valid granular source."
        ),
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
        "methodology_boundary_note": (
            "Local packaging artifact only. Derived from management interpretation layer. "
            "Must preserve methodological guardrails and avoid unsupported hour-level staffing claims."
        ),
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
        "methodology_boundary_note": (
            "QA orchestration layer for management artifacts. "
            "Supports governance, packaging discipline, and methodological defensibility."
        ),
    },
]

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

def main() -> None:
    out_dir = REPO_ROOT / "data_processed" / "management"
    out_dir.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(TRACKED_ARTIFACTS)[REQUIRED_COLUMNS].sort_values(
        ["management_layer_role", "artifact_key"]
    )

    out_fp = out_dir / "management_layer_registry.csv"
    df.to_csv(out_fp, index=False)

    print(f"[OK] Wrote: {out_fp}")
    print(f"[INFO] shape={df.shape}")
    print("\n=== MANAGEMENT LAYER REGISTRY PREVIEW ===")
    print(df.to_string(index=False))

if __name__ == "__main__":
    main()
