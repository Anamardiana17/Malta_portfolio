from __future__ import annotations

from pathlib import Path
import pandas as pd


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def main() -> None:
    root = repo_root()
    out_dir = root / "data_processed" / "management"
    out_dir.mkdir(parents=True, exist_ok=True)

    rows = [
        {
            "artifact_path": "data_processed/management/management_layer_registry.csv",
            "artifact_type": "registry",
            "primary_build_script": "scripts/build/build_management_layer_registry.py",
            "primary_qa_script": "scripts/qa/validate_management_layer_registry.py",
            "review_sequence_order": 1,
            "reviewer_usage": "Use as authoritative package inventory and governance anchor.",
            "governance_role": "Defines tracked management-layer components and package membership.",
            "method_boundary_note": "Registry documents packaged artifacts only; it does not create new operational inference."
        },
        {
            "artifact_path": "data_processed/management/management_layer_index.md",
            "artifact_type": "index",
            "primary_build_script": "scripts/build/build_management_layer_index.py",
            "primary_qa_script": "scripts/qa/validate_management_layer_index.py",
            "review_sequence_order": 2,
            "reviewer_usage": "Use as navigation entry point before reviewing individual governance documents.",
            "governance_role": "Improves discoverability and review readability across management-layer package artifacts.",
            "method_boundary_note": "Index supports navigation only; it does not change analytical logic or staffing recommendation rules."
        },
        {
            "artifact_path": "data_processed/management/management_layer_package_guide.md",
            "artifact_type": "package_guide",
            "primary_build_script": "scripts/build/build_management_layer_package_guide.py",
            "primary_qa_script": "scripts/qa/validate_management_layer_package_guide.py",
            "review_sequence_order": 3,
            "reviewer_usage": "Use to understand package purpose, intended reading posture, and governance framing.",
            "governance_role": "Explains package scope and reinforces methodological boundaries for reviewer interpretation.",
            "method_boundary_note": "Guide frames how to read the package; it does not authorize pseudo-daypart or hourly roster inference."
        },
        {
            "artifact_path": "data_processed/management/management_layer_reviewer_checklist.md",
            "artifact_type": "reviewer_checklist",
            "primary_build_script": "scripts/build/build_management_layer_reviewer_checklist.py",
            "primary_qa_script": "scripts/qa/validate_management_layer_reviewer_checklist.py",
            "review_sequence_order": 4,
            "reviewer_usage": "Use as final governance check before sharing package outputs externally or across review stages.",
            "governance_role": "Standardizes reviewer checks for defensibility, scope control, and package hygiene.",
            "method_boundary_note": "Checklist confirms review discipline only; it does not validate unsupported granular staffing claims."
        },
        {
            "artifact_path": "data_processed/management/management_layer_traceability_matrix.csv",
            "artifact_type": "traceability_matrix",
            "primary_build_script": "scripts/build/build_management_layer_traceability_matrix.py",
            "primary_qa_script": "scripts/qa/validate_management_layer_traceability_matrix.py",
            "review_sequence_order": 5,
            "reviewer_usage": "Use to trace each governance artifact back to its build script and QA validator.",
            "governance_role": "Creates package lineage and reviewer-ready traceability across management-layer governance artifacts.",
            "method_boundary_note": "Traceability records package lineage only; it does not add modelling logic or new decision variables."
        },
    ]

    df = pd.DataFrame(rows).sort_values(
        by=["review_sequence_order", "artifact_path"],
        kind="stable"
    ).reset_index(drop=True)

    out_fp = out_dir / "management_layer_traceability_matrix.csv"
    df.to_csv(out_fp, index=False)

    print(f"[OK] Wrote: {out_fp}")
    print(f"[INFO] shape={df.shape}")


if __name__ == "__main__":
    main()
