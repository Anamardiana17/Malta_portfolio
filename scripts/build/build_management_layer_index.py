from __future__ import annotations

from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
REGISTRY_FP = ROOT / "data_processed" / "management" / "management_layer_registry.csv"
OUT_FP = ROOT / "data_processed" / "management" / "management_layer_index.md"

ROLE_LABELS = {
    "decision_interpretation": "Decision interpretation",
    "manager_readout": "Manager readout",
    "governance_control": "Governance control",
    "documentation_index": "Documentation index",
}

def yn(flag: int, yes: str, no: str) -> str:
    return yes if int(flag) == 1 else no

def main() -> None:
    if not REGISTRY_FP.exists():
        raise FileNotFoundError(f"Registry not found: {REGISTRY_FP}")

    df = pd.read_csv(REGISTRY_FP).copy()
    df = df.sort_values(["management_layer_role", "artifact_key"]).reset_index(drop=True)

    lines: list[str] = []
    lines.append("# Management Layer Index")
    lines.append("")
    lines.append("This index documents the current management-layer artifacts used for interpretation, packaging, and governance.")
    lines.append("")
    lines.append("## Boundary framing")
    lines.append("")
    lines.append("- Internal operating proxies remain the primary anchor for actionable decisions.")
    lines.append("- External proxies remain contextual regime or market-pressure inputs only.")
    lines.append("- No synthetic intra-day staffing segmentation is introduced.")
    lines.append("- No hour-level roster inference is introduced without valid granular source support.")
    lines.append("")
    lines.append("## Artifact register")
    lines.append("")

    for _, row in df.iterrows():
        role = ROLE_LABELS.get(str(row["management_layer_role"]), str(row["management_layer_role"]))
        lines.append(f"### {row['artifact_key']}")
        lines.append("")
        lines.append(f"- Path: `{row['artifact_path']}`")
        lines.append(f"- Type: `{row['artifact_type']}`")
        lines.append(f"- Role: {role}")
        lines.append(f"- Tracked in repo: {yn(row['tracked_in_repo_flag'], 'yes', 'no')}")
        lines.append(f"- Local-only output: {yn(row['local_only_output_flag'], 'yes', 'no')}")
        lines.append(f"- QA-covered: {yn(row['qa_coverage_flag'], 'yes', 'no')}")
        if str(row['qa_script_path']).strip():
            lines.append(f"- QA script: `{row['qa_script_path']}`")
        else:
            lines.append("- QA script: not applicable")
        lines.append(f"- Dependency class: `{row['source_dependency_class']}`")
        lines.append(f"- Boundary note: {row['methodology_boundary_note']}")
        lines.append("")

    lines.append("## Governance notes")
    lines.append("")
    lines.append("- `output/` remains local-only and should not be tracked.")
    lines.append("- This index is a packaging and readability layer, not a modelling layer.")
    lines.append("- Registry and QA coverage should be updated when management-layer artifacts change.")
    lines.append("")

    OUT_FP.write_text("\n".join(lines), encoding="utf-8")
    print(f"[OK] Wrote: {OUT_FP}")
    print(f"[INFO] artifact_count={len(df)}")
    print(f"[INFO] line_count={len(lines)}")

if __name__ == "__main__":
    main()
