from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
TARGET_PATH = (
    REPO_ROOT
    / "data_processed"
    / "management"
    / "management_layer_reviewer_quickstart.md"
)


CONTENT = """# Management Layer Reviewer Quickstart

## Purpose
This quickstart provides a fast reviewer entry point into the governed management-layer package.
It is intended to help a new reviewer understand what to read first, how to interpret the package quickly, and which boundaries must remain in force.

## Boundary Conditions
- External proxies remain contextual regime or market-pressure inputs only.
- Internal operating proxies remain the primary anchor for actionable decisions.
- No pseudo-daypart logic is introduced.
- No hour-level or roster-by-hour inference is introduced without valid granular source support.
- `output/` remains local-only and is not tracked as a governed repository artifact.
- Archived and excluded artifact trees are not part of the governed main-repo package.

## What To Read First
Reviewers should begin with:
1. `monthly_roster_management_interpretation.csv`
2. `management_layer_governance_manifest.md`
3. `management_layer_index.md`

These files provide the core management interpretation output, the canonical package summary, and the navigation layer needed to understand the governed package quickly.

## 2-Minute Review Path
A fast review path is:
1. confirm the core interpretation artifact exists and is populated
2. confirm the governed artifact set listed in the manifest is coherent
3. confirm the index reading order matches the active package
4. confirm traceability and QA coverage exist
5. confirm boundary language remains explicit and unchanged

This path is intended to support fast orientation rather than exhaustive review.

## What This Package Does Not Claim
This package should not be interpreted as:
- a live production system
- a real-time staffing engine
- a daypart forecasting engine
- an hourly deployment engine
- a direct authorization for unsupported intraday staffing inference

## Reviewer Decision Cues
A reviewer should be comfortable proceeding when:
- the core management interpretation artifact is readable
- governance artifacts are internally consistent
- QA coverage is present across the governed package
- archive and local-only boundaries are clearly stated
- no new operational overclaim wording has been introduced

A reviewer should pause review if:
- artifact counts no longer align across registry, index, and traceability views
- boundary statements are missing or weakened
- local-only or archived artifacts appear to be treated as governed release artifacts
- unsupported hourly or pseudo-daypart claims appear

## Status
Current status: active governance-support artifact within the management-layer package.
"""


def main() -> None:
    TARGET_PATH.parent.mkdir(parents=True, exist_ok=True)
    TARGET_PATH.write_text(CONTENT, encoding="utf-8")
    print(f"[OK] Wrote: {TARGET_PATH}")


if __name__ == "__main__":
    main()
