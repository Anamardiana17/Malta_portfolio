from __future__ import annotations

from pathlib import Path
import sys


REQUIRED_PHRASES = [
    "external proxies are used only as contextual regime / market-pressure context",
    "internal operating proxies remain the anchor for actionable management interpretation",
    "no pseudo-daypart logic is introduced",
    "no roster-by-hour inference is introduced without valid granular source support",
    "This checklist is a governance and review aid. It does not introduce new modelling, new commercial logic, or new staffing inference.",
]


def main() -> int:
    repo_root = Path(__file__).resolve().parents[2]
    fp = repo_root / "data_processed" / "management" / "management_layer_reviewer_checklist.md"

    if not fp.exists():
        print(f"[FAIL] Missing file: {fp}")
        return 1

    text = fp.read_text(encoding="utf-8").strip()
    print(f"[OK] Loaded: {fp}")

    if not text:
        print("[FAIL] Checklist is empty")
        return 1
    print("[OK] Checklist is non-empty")

    for phrase in REQUIRED_PHRASES:
        if phrase not in text:
            print(f"[FAIL] Missing required phrase: {phrase}")
            return 1
    print("[OK] Required governance phrases present")

    required_sections = [
        "# Management Layer Reviewer Checklist",
        "## Purpose",
        "## Reviewer Checks",
        "## Suggested reviewer reading order",
        "## Sign-off standard",
        "## Boundary note",
    ]
    for section in required_sections:
        if section not in text:
            print(f"[FAIL] Missing section: {section}")
            return 1
    print("[OK] Required sections present")

    banned_phrases = [
        "hourly demand forecast",
        "live daypart optimization",
        "roster by hour recommendation",
        "causal proof",
    ]
    lowered = text.lower()
    for phrase in banned_phrases:
        if phrase in lowered:
            print(f"[FAIL] Found banned phrase: {phrase}")
            return 1
    print("[OK] No obvious overclaim / pseudo-hourly wording found")

    print("[PASS] management_layer_reviewer_checklist validation passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
