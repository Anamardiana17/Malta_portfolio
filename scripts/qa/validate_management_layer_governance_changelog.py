from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
TARGET_PATH = (
    REPO_ROOT
    / "data_processed"
    / "management"
    / "management_layer_governance_changelog.md"
)


REQUIRED_HEADINGS = [
    "# Management Layer Governance Changelog",
    "## Purpose",
    "## Boundary Conditions",
    "## Governance Evolution Summary",
    "## Change Log Entries",
    "## Interpretation Notes",
    "## Reviewer Use",
    "## Status",
]

REQUIRED_PHRASES = [
    "External proxies remain contextual regime and market-pressure signals only.",
    "Internal operating proxies remain the primary anchor for actionable decisions.",
    "No pseudo-daypart logic is introduced.",
    "No hour-level or roster-by-hour inference is introduced without valid granular source support.",
    "`output/` remains local-only and is not tracked as a governed repository artifact.",
    "management_layer_governance_changelog.md",
]

FORBIDDEN_PHRASES = [
    "live production system",
    "real-time roster-by-hour",
    "hourly deployment engine",
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

    phase_count = text.count("### Phase ")
    if phase_count < 6:
        raise AssertionError(f"Expected at least 6 phase entries, found {phase_count}")
    print("[OK] Sufficient phase entries present")

    if "| Order | Governance Artifact | Role in Package | Review Value |" not in text:
        raise AssertionError("Governance summary table header missing")
    print("[OK] Governance summary table present")

    print("\n=== QA RESULT ===")
    print("[PASS] management_layer_governance_changelog.md is valid")


if __name__ == "__main__":
    main()
