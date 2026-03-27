from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
TARGET_PATH = (
    REPO_ROOT
    / "data_processed"
    / "management"
    / "management_layer_reviewer_quickstart.md"
)

REQUIRED_HEADINGS = [
    "# Management Layer Reviewer Quickstart",
    "## Purpose",
    "## Boundary Conditions",
    "## What To Read First",
    "## 2-Minute Review Path",
    "## What This Package Does Not Claim",
    "## Reviewer Decision Cues",
    "## Status",
]

REQUIRED_PHRASES = [
    "External proxies remain contextual regime or market-pressure inputs only.",
    "Internal operating proxies remain the primary anchor for actionable decisions.",
    "No pseudo-daypart logic is introduced.",
    "No hour-level or roster-by-hour inference is introduced without valid granular source support.",
    "`output/` remains local-only and is not tracked as a governed repository artifact.",
    "Archived and excluded artifact trees are not part of the governed main-repo package.",
    "monthly_roster_management_interpretation.csv",
    "management_layer_governance_manifest.md",
    "management_layer_index.md",
]

FORBIDDEN_PHRASES = [
    "production-ready staffing engine",
    "real-time operational decision engine",
]


def main() -> None:
    if not TARGET_PATH.exists():
        raise FileNotFoundError(f"Missing file: {TARGET_PATH}")

    text = TARGET_PATH.read_text(encoding="utf-8")
    print(f"[OK] Loaded: {TARGET_PATH}")

    for heading in REQUIRED_HEADINGS:
        if heading not in text:
            raise AssertionError(f"Missing heading: {heading}")
    print("[OK] Required headings present")

    for phrase in REQUIRED_PHRASES:
        if phrase not in text:
            raise AssertionError(f"Missing required phrase: {phrase}")
    print("[OK] Required governance phrases present")

    for phrase in FORBIDDEN_PHRASES:
        if phrase.lower() in text.lower():
            raise AssertionError(f"Forbidden phrase found: {phrase}")
    print("[OK] No forbidden overclaim wording found")

    if text.count("A reviewer should") < 2:
        raise AssertionError("Expected reviewer decision cue statements")
    print("[OK] Reviewer decision cues present")

    print("\n=== QA RESULT ===")
    print("[PASS] management_layer_reviewer_quickstart.md is valid")


if __name__ == "__main__":
    main()
