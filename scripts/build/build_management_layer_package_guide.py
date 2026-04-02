from pathlib import Path
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
REGISTRY_FP = REPO_ROOT / "data_processed" / "management" / "management_layer_registry.csv"
OUT_FP = REPO_ROOT / "data_processed" / "management" / "management_layer_package_guide.md"


def main() -> None:
    if not REGISTRY_FP.exists():
        raise FileNotFoundError(f"Registry file not found: {REGISTRY_FP}")

    df = pd.read_csv(REGISTRY_FP)

    required_cols = [
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
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required registry columns: {missing}")

    df = df.sort_values(
        by=["tracked_in_repo_flag", "artifact_type", "artifact_key"],
        ascending=[False, True, True],
        kind="stable"
    ).reset_index(drop=True)

    lines = []
    lines.append("# Management Layer Package Guide")
    lines.append("")
    lines.append("## Purpose")
    lines.append("")
    lines.append(
        "This document summarizes the tracked management-layer package for packaging, governance, and reviewer readability. "
        "It helps explain what each artifact contributes without introducing new modelling logic."
    )
    lines.append("")
    lines.append("")
    lines.append("## Artifact Coverage")
    lines.append("- `monthly_roster_management_interpretation.csv`: decision interpretation dataset anchored to internal operating proxies with contextual external regime support.")
    lines.append("- `monthly_roster_management_readout.md`: local-only markdown management readout derived from the interpretation layer.")
    lines.append("- `management_layer_registry.csv`: tracked artifact registry for governance, packaging control, and reviewer traceability.")
    lines.append("- `management_layer_index.md`: navigation-first documentation index for reviewer usability.")
    lines.append("- `management_layer_reviewer_checklist.md`: reviewer-facing governance checklist for package integrity and methodological defensibility.")

    lines.append("## Method Boundary")

    lines.append("External proxies are retained as contextual regime and market-pressure inputs only.")
    lines.append("Internal operating proxies remain the primary anchor for actionable management interpretation.")
    lines.append("No pseudo-daypart logic is introduced in this package guide.")


    lines.append("External proxies are retained as contextual regime and market-pressure inputs only.")
    lines.append("Internal operating proxies remain the primary anchor for actionable management interpretation.")

    lines.append("")
    lines.append("- External proxies remain contextual regime and market-pressure inputs only.")
    lines.append("- Internal operating proxies remain the anchor for actionable management decisions.")
    lines.append("- No pseudo-daypart logic is introduced in this guide.")
    lines.append("- No roster-by-hour inference is introduced without valid granular source support.")
    lines.append("- This guide is documentation only and does not extend the modelling layer.")
    lines.append("")
    lines.append("## Tracked Artifact Coverage")
    lines.append("")
    lines.append("| Artifact Key | Type | Role | Tracked | QA | Path |")
    lines.append("|---|---|---|---:|---:|---|")

    for _, row in df.iterrows():
        lines.append(
            f"| {str(row['artifact_key']).strip()} "
            f"| {str(row['artifact_type']).strip()} "
            f"| {str(row['management_layer_role']).strip()} "
            f"| {int(row['tracked_in_repo_flag'])} "
            f"| {int(row['qa_coverage_flag'])} "
            f"| `{str(row['artifact_path']).strip()}` |"
        )

    lines.append("")
    lines.append("## Artifact Notes")
    lines.append("")

    for _, row in df.iterrows():
        artifact_key = str(row["artifact_key"]).strip()
        source_dependency_class = str(row["source_dependency_class"]).strip()
        qa_script_path = "" if pd.isna(row["qa_script_path"]) else str(row["qa_script_path"]).strip()
        note = " ".join(str(row["methodology_boundary_note"]).strip().split())

        lines.append(f"### {artifact_key}")
        lines.append("")
        lines.append(f"- Role: {str(row['management_layer_role']).strip()}")
        lines.append(f"- Source dependency class: {source_dependency_class}")
        lines.append(f"- QA script: {qa_script_path if qa_script_path else 'None'}")
        lines.append(f"- Boundary note: {note}")
        lines.append("")

    lines.append("## Reviewer Reading Order")
    lines.append("")
    lines.append("1. Start with the management layer index for navigation.")
    lines.append("2. Review the registry for artifact traceability and governance coverage.")
    lines.append("3. Review interpretation/readout artifacts for management-facing packaging.")
    lines.append("4. Run the QA aggregator before packaging or presenting the stack.")
    lines.append("")
    lines.append("## Governance Notes")

    lines.append("Output artifacts under `output/` remain local-only and are not part of the tracked management-layer package.")

    lines.append("")
    lines.append("- output/ remains local-only and should not be tracked.")
    lines.append("- This package guide should be refreshed when tracked management-layer artifacts change.")
    lines.append("- The guide reinforces methodological defensibility; it does not add new analytical claims.")
    lines.append("")

    OUT_FP.write_text("\n".join(lines), encoding="utf-8")
    print(f"[OK] Wrote package guide: {OUT_FP}")


if __name__ == "__main__":
    main()
