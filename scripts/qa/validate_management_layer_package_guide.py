from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
GUIDE_FP = REPO_ROOT / "data_processed" / "management" / "management_layer_package_guide.md"


def require(text: str, needle: str, label: str) -> None:
    if needle not in text:
        raise AssertionError(f"[FAIL] Missing {label}: {needle}")
    print(f"[OK] Found {label}")


def reject(text_lower: str, needle: str, label: str) -> None:
    if needle in text_lower:
        raise AssertionError(f"[FAIL] Found disallowed wording for {label}: {needle}")
    print(f"[OK] No disallowed wording for {label}")


def main() -> None:
    if not GUIDE_FP.exists():
        raise FileNotFoundError(f"Guide file not found: {GUIDE_FP}")

    text = GUIDE_FP.read_text(encoding="utf-8")
    text_lower = text.lower()

    print(f"[OK] Loaded: {GUIDE_FP}")

    require(text, "# Management Layer Package Guide", "title")
    require(text, "## Purpose", "Purpose section")
    require(text, "## Method Boundary", "Method Boundary section")
    require(text, "## Artifact Coverage", "Artifact Coverage section")
    require(text, "## Reviewer Reading Order", "Reviewer Reading Order section")
    require(text, "## Governance Notes", "Governance Notes section")

    require(
        text,
        "External proxies are retained as contextual regime and market-pressure inputs only.",
        "external proxy boundary",
    )
    require(
        text,
        "Internal operating proxies remain the anchor for actionable management decisions.",
        "internal proxy anchor statement",
    )
    require(
        text,
        "No pseudo-daypart logic is introduced in this package guide.",
        "pseudo-daypart boundary",
    )
    require(
        text,
        "No roster-by-hour inference is introduced without valid granular source support.",
        "roster-by-hour boundary",
    )
    require(
        text,
        "Output artifacts under `output/` remain local-only and are not part of the tracked management-layer package.",
        "output local-only note",
    )

    reject(text_lower, "hour-by-hour roster optimization", "hourly overclaim")
    reject(text_lower, "live production scheduling engine", "production overclaim")
    reject(text_lower, "real-time daypart demand prediction", "predictive overclaim")

    print("[OK] Management layer package guide passed QA")


if __name__ == "__main__":
    main()
