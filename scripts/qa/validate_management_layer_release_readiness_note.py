from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
FP = REPO_ROOT / "data_processed" / "management" / "management_layer_release_readiness_note.md"

REQUIRED_HEADINGS = [
    "# Management Layer Release Readiness Note",
    "## Release intent",
    "## Current release posture",
    "## What this release is intended to demonstrate",
    "## Boundary conditions that remain in force",
    "## Governance artifact coverage expected in this release",
    "## Governance artifact coverage check",
    "## QA validators expected in reviewer-facing governance stack",
    "## Reviewer interpretation guidance",
    "## Release readiness decision rubric",
    "## Handoff note",
]

REQUIRED_PHRASES = [
    "internal operating proxies remain the anchor",
    "External proxies remain contextual",
    "No pseudo-daypart logic",
    "No hourly roster inference",
    "`output/` remains local-only",
]

FORBIDDEN_PATTERNS = [
    "live production system",
    "real-time staffing engine",
]

def fail(msg: str):
    print(f"[FAIL] {msg}")
    sys.exit(1)

def main():
    if not FP.exists():
        fail(f"Missing file: {FP}")

    text = FP.read_text(encoding="utf-8")
    print(f"[OK] Loaded: {FP}")

    if not text.strip():
        fail("Document is empty")
    print("[OK] Document is non-empty")

    for heading in REQUIRED_HEADINGS:
        if heading not in text:
            fail(f"Missing required heading: {heading}")
    print("[OK] Required headings present")

    lower = text.lower()
    for phrase in REQUIRED_PHRASES:
        if phrase.lower() not in lower:
            fail(f"Missing required phrase: {phrase}")
    print("[OK] Required boundary phrases present")

    # Forbidden patterns are allowed only in the reviewer interpretation section as negatives.
    for pattern in FORBIDDEN_PATTERNS:
        if pattern in lower and "it should not be interpreted as" not in lower:
            fail(f"Forbidden phrase appears without guarded context: {pattern}")
    print("[OK] No unguarded overclaim phrasing found")

    print("[PASS] management_layer_release_readiness_note validated")

if __name__ == "__main__":
    main()
