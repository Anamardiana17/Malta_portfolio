from pathlib import Path
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
MANAGEMENT_DIR = REPO_ROOT / "data_processed" / "management"
OUT_FP = MANAGEMENT_DIR / "management_layer_release_readiness_note.md"
TRACE_FP = MANAGEMENT_DIR / "management_layer_traceability_matrix.csv"
REGISTRY_FP = MANAGEMENT_DIR / "management_layer_registry.csv"

REQUIRED_TRACEABILITY_ARTIFACTS = [
    "monthly_roster_management_interpretation.csv",
    "management_layer_registry.csv",
    "management_layer_index.md",
    "management_layer_package_guide.md",
    "management_layer_reviewer_checklist.md",
    "management_layer_traceability_matrix.csv",
]

REQUIRED_QA_VALIDATORS = [
    "validate_monthly_roster_management_interpretation.py",
    "validate_monthly_roster_management_markdown_readout.py",
    "validate_management_layer_registry.py",
    "validate_management_layer_index.py",
    "validate_management_layer_package_guide.py",
    "validate_management_layer_reviewer_checklist.py",
    "validate_management_layer_traceability_matrix.py",
]

def load_optional_csv(fp: Path):
    if not fp.exists():
        return None
    return pd.read_csv(fp)

def yes_no(flag: bool) -> str:
    return "Yes" if flag else "No"

def main():
    MANAGEMENT_DIR.mkdir(parents=True, exist_ok=True)

    trace_df = load_optional_csv(TRACE_FP)
    registry_df = load_optional_csv(REGISTRY_FP)

    trace_available = trace_df is not None and not trace_df.empty
    registry_available = registry_df is not None and not registry_df.empty

    if trace_available and "artifact_name" in trace_df.columns:
        trace_artifacts = set(trace_df["artifact_name"].dropna().astype(str).tolist())
    else:
        trace_artifacts = set()

    if registry_available and "artifact_name" in registry_df.columns:
        registry_artifacts = set(registry_df["artifact_name"].dropna().astype(str).tolist())
    else:
        registry_artifacts = set()

    missing_from_traceability = [
        x for x in REQUIRED_TRACEABILITY_ARTIFACTS if x not in trace_artifacts
    ]
    missing_from_registry = [
        x for x in REQUIRED_TRACEABILITY_ARTIFACTS if x not in registry_artifacts
    ]

    qa_summary = "\n".join([f"- `{x}`" for x in REQUIRED_QA_VALIDATORS])

    trace_summary = "\n".join([f"- `{x}`" for x in REQUIRED_TRACEABILITY_ARTIFACTS])

    missing_trace_text = (
        "\n".join([f"- `{x}`" for x in missing_from_traceability])
        if missing_from_traceability
        else "- None"
    )
    missing_registry_text = (
        "\n".join([f"- `{x}`" for x in missing_from_registry])
        if missing_from_registry
        else "- None"
    )

    release_status = "READY WITH GOVERNANCE NOTE"
    if missing_from_traceability or missing_from_registry:
        release_status = "HOLD FOR GOVERNANCE COMPLETION"

    text = f"""# Management Layer Release Readiness Note

## Release intent
This note is a governance-facing release readiness artifact for the management layer package.  
It is designed to support reviewer trust, handoff clarity, and packaging discipline without introducing any new modelling logic, operational inference, or forecast layer.

## Current release posture
**Release status:** {release_status}

## What this release is intended to demonstrate
- The management layer package is organized for reviewer navigation and governance review.
- Management artifacts can be traced across registry, index, package guide, reviewer checklist, and traceability matrix.
- The package preserves methodological defensibility by keeping internal operating proxies as the anchor for actionability.
- External proxies remain contextual regime or market-pressure framing only, and are not elevated into direct staffing control logic.
- The package does not activate pseudo-daypart logic and does not infer roster-by-hour behavior without valid granular source support.

## Boundary conditions that remain in force
- Internal operating proxies remain the anchor for actionable management decision support.
- External proxies are contextual only.
- No pseudo-daypart logic is introduced.
- No hourly roster inference is introduced without valid granular source support.
- `output/` remains local-only and is not part of the tracked governance package.

## Governance artifact coverage expected in this release
{trace_summary}

## Governance artifact coverage check
**Registry available:** {yes_no(registry_available)}  
**Traceability matrix available:** {yes_no(trace_available)}

### Missing from registry
{missing_registry_text}

### Missing from traceability matrix
{missing_trace_text}

## QA validators expected in reviewer-facing governance stack
{qa_summary}

## Reviewer interpretation guidance
A reviewer should interpret this package as a governance-structured management portfolio layer.  
It should not be interpreted as:
- a live production deployment
- a real-time staffing engine
- a daypart forecasting engine
- a roster-by-hour optimization system
- a direct replacement for source-granular operational controls

## Release readiness decision rubric
### Ready
Use this state when:
- governance artifacts are present
- QA validators pass
- boundary conditions remain explicit
- no new modelling claims are introduced

### Ready with governance note
Use this state when:
- the package is reviewer-usable
- the methodological boundary remains explicit
- minor governance packaging refinements may still exist
- no modelling risk has been introduced

### Hold
Use this state when:
- traceability artifacts are incomplete
- reviewer navigation is unclear
- governance documentation conflicts with actual package contents
- boundary conditions are weakened or ambiguous

## Handoff note
This release candidate should be reviewed as a governance and packaging improvement layer only.  
It increases reviewer usability and trust by making release posture, scope boundaries, and documentation expectations explicit, while preserving the existing methodological guardrails of the management layer stack.
"""
    OUT_FP.write_text(text, encoding="utf-8")
    print(f"[OK] Wrote: {OUT_FP}")

if __name__ == "__main__":
    main()
