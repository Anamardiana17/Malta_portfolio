from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
TARGET_PATH = (
    REPO_ROOT
    / "data_processed"
    / "management"
    / "management_layer_governance_manifest.md"
)

REQUIRED_HEADINGS = [
    "# Management Layer Governance Manifest",
    "## Purpose",
    "## Boundary Conditions",
    "## Governed Artifact Set Summary",
    "## Reviewer Reading Order",
    "## Governance Coverage Summary",
    "## Artifact State Summary",
    "## Reviewer Use",
    "## Status",
]

REQUIRED_PHRASES = [
    "External proxies remain contextual regime or market-pressure inputs only.",
    "Internal operating proxies remain the primary anchor for actionable decisions.",
    "No pseudo-daypart logic is introduced.",
    "No hour-level or roster-by-hour inference is introduced without valid granular source support.",
    "`output/` remains local-only and is not tracked as a governed repository artifact.",
    "Archived and excluded artifact trees are not part of the governed main-repo package.",
    "management_layer_governance_manifest.md",
    "management_layer_artifact_lifecycle_policy.md",
]

FORBIDDEN_PHRASES = [
    "live production system",
    "real-time staffing engine",
    "hourly deployment engine",
    "daypart forecasting engine",
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

    table_header = "| Review Order | Artifact | Type | Primary Role |"
    if table_header not in text:
        raise AssertionError("Governed artifact summary table header missing")
    print("[OK] Governed artifact summary table present")

    review_items = text.count(". `management_layer_") + text.count(". `monthly_roster_management_interpretation.csv`")
    if review_items < 10:
        raise AssertionError(f"Expected at least 10 reading-order items, found {review_items}")
    print("[OK] Reviewer reading order length valid")

    print("\n=== QA RESULT ===")
    print("[PASS] management_layer_governance_manifest.md is valid")


if __name__ == "__main__":
    main()
