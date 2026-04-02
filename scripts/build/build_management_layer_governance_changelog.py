from __future__ import annotations

from pathlib import Path
from textwrap import dedent


REPO_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_PATH = (
    REPO_ROOT
    / "data_processed"
    / "management"
    / "management_layer_governance_changelog.md"
)


def build_markdown() -> str:
    return dedent(
        """
        # Management Layer Governance Changelog

        ## Purpose
        This changelog records the staged expansion of the management-layer governance package.
        It is intended to improve reviewer usability, packaging clarity, and handoff trust.

        ## Boundary Conditions
        - External proxies remain contextual regime and market-pressure signals only.
        - Internal operating proxies remain the primary anchor for actionable decisions.
        - No pseudo-daypart logic is introduced.
        - No hour-level or roster-by-hour inference is introduced without valid granular source support.
        - `output/` remains local-only and is not tracked as a governed repository artifact.

        ## Governance Evolution Summary

        | Order | Governance Artifact | Role in Package | Review Value |
        |---|---|---|---|
        | 1 | monthly_roster_management_interpretation.csv | Structured management interpretation layer | Provides reviewer-readable decision framing grounded in internal operating proxies |
        | 2 | monthly_roster_management_markdown_readout.md | Narrative management readout | Improves readability and handoff usability |
        | 3 | management_layer_registry.csv | Governance inventory | Defines the governed artifact set |
        | 4 | management_layer_index.md | Navigation layer | Helps reviewers locate and understand package contents |
        | 5 | management_layer_package_guide.md | Package usage guide | Clarifies intended reading order and packaging logic |
        | 6 | management_layer_reviewer_checklist.md | Structured review guide | Supports consistent artifact review |
        | 7 | management_layer_traceability_matrix.csv | Cross-artifact traceability | Shows how governance artifacts connect |
        | 8 | management_layer_release_readiness_note.md | Release readiness framing | Summarizes package readiness and remaining boundary conditions |
        | 9 | management_layer_governance_changelog.md | Governance change history | Makes package evolution explicit and reviewer-traceable |

        ## Change Log Entries

        ### Phase 1 — Interpretation Foundation
        Added the structured monthly management interpretation layer to translate internal operating proxy conditions into reviewer-readable management framing.
        This established the base interpretation layer while preserving methodological boundaries.

        ### Phase 2 — Narrative Readout Layer
        Added the markdown management readout to improve human readability and handoff usability.
        This did not introduce new decision logic; it only improved interpretability of the governed output.

        ### Phase 3 — Governance Registry and Indexing
        Added the governance registry and management-layer index to formalize the package structure.
        This made the package easier to audit, review, and maintain.

        ### Phase 4 — Reviewer Packaging Controls
        Added the package guide and reviewer checklist to support consistent package reading, review workflow, and governance usability.
        These additions improved packaging discipline without altering core management logic.

        ### Phase 5 — Traceability and Release Framing
        Added the traceability matrix and release readiness note to strengthen reviewer trust, artifact linkage, and release defensibility.
        This improved auditability and package maturity.

        ### Phase 6 — Governance Changelog
        Added this governance changelog to document the staged evolution of the management-layer governance stack itself.
        This artifact improves handoff trust by making governance growth explicit, reviewable, and historically legible.

        ## Interpretation Notes
        The governance package has been expanded in small, reviewer-oriented increments.
        Additions have focused on readability, traceability, packaging discipline, and methodological defensibility rather than introducing new analytical claims.

        ## Reviewer Use
        Reviewers should use this file to understand how the governance stack matured over time and how each artifact contributes to package trust.
        This file is descriptive and governance-oriented; it does not introduce new operational inference.

        ## Status
        Current status: active governance-support artifact within the management-layer package.
        """
    ).strip() + "\n"


def main() -> None:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(build_markdown(), encoding="utf-8")
    print(f"[OK] Wrote: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
