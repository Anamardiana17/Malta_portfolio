from __future__ import annotations

from pathlib import Path
import pandas as pd
import sys

ROOT = Path(__file__).resolve().parents[2]
INDEX_FP = ROOT / "data_processed" / "management" / "management_layer_index.md"
REGISTRY_FP = ROOT / "data_processed" / "management" / "management_layer_registry.csv"

REQUIRED_SECTIONS = [
    "# Management Layer Index",
    "## Boundary framing",
    "## Artifact register",
    "## Governance notes",
]

REQUIRED_PHRASES = [
    "Internal operating proxies remain the primary anchor for actionable decisions.",
    "External proxies remain contextual regime or market-pressure inputs only.",
    "No synthetic intra-day staffing segmentation is introduced.",
    "No hour-level roster inference is introduced without valid granular source support.",
    "`output/` remains local-only and should not be tracked.",
]

FORBIDDEN_TERMS = [
    "pseudo-daypart",
    "roster-by-hour",
    "pseudo-hourly",
]

def fail(msg: str) -> None:
    print(f"[FAIL] {msg}")
    sys.exit(1)

def ok(msg: str) -> None:
    print(f"[OK] {msg}")

def main() -> None:
    if not INDEX_FP.exists():
        fail(f"Index file not found: {INDEX_FP}")
    if not REGISTRY_FP.exists():
        fail(f"Registry file not found: {REGISTRY_FP}")

    text = INDEX_FP.read_text(encoding="utf-8")
    df = pd.read_csv(REGISTRY_FP)

    print(f"[OK] Loaded index: {INDEX_FP}")
    print(f"[INFO] char_count={len(text)}")
    print(f"[INFO] line_count={len(text.splitlines())}")

    if not text.strip():
        fail("Index markdown is empty")
    ok("Index markdown is non-empty")

    for section in REQUIRED_SECTIONS:
        if section not in text:
            fail(f"Missing required section: {section}")
    ok("Required sections present")

    for phrase in REQUIRED_PHRASES:
        if phrase not in text:
            fail(f"Missing required phrase: {phrase}")
    ok("Boundary and governance phrasing present")

    lower = text.lower()
    for term in FORBIDDEN_TERMS:
        if term in lower:
            fail(f"Forbidden overclaim term found: {term}")
    ok("No forbidden pseudo-hourly / pseudo-daypart wording found")

    missing_artifacts = []
    for artifact_key in df["artifact_key"].astype(str):
        if f"### {artifact_key}" not in text:
            missing_artifacts.append(artifact_key)
    if missing_artifacts:
        fail(f"Missing artifact sections for: {missing_artifacts}")
    ok("All registry artifacts are represented in the index")

    print("\n=== INDEX VALIDATION SUMMARY ===")
    print(f"[OK] artifact_sections={len(df)}")
    print("[OK] management layer index markdown QA passed")

if __name__ == "__main__":
    main()
