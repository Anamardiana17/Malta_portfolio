from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[2]
TARGET = ROOT / "output" / "management" / "monthly_roster_management_readout.md"

REQUIRED_SECTION_PATTERNS = [
    r"(?im)^#\s+Monthly Roster Management Readout\s*$",
    r"(?im)^##\s+Purpose\s*$",
    r"(?im)^##\s+Management framing\s*$",
    r"(?im)^##\s+Portfolio snapshot\s*$",
    r"(?im)^##\s+Highest-attention monthly cases\s*$",
]

BOUNDARY_SAFE_PATTERNS = [
    r"(?i)anchored to internal operating proxies",
    r"(?i)external market proxies are retained only as context",
    r"(?i)without over-claiming external proxy data as direct hourly spa demand",
    r"(?i)not pseudo-precision",
    r"(?i)not as proof of hourly demand structure",
    r"(?i)no hourly/daypart truth claims",
]

FORBIDDEN_PATTERNS = [
    r"(?i)\bhourly roster\b",
    r"(?i)\broster by hour\b",
    r"(?i)\bhour-by-hour staffing\b",
    r"(?i)\bintraday staffing inference\b",
    r"(?i)\btherapist-by-hour deployment\b",
]

ALLOWED_GUARDRAIL_PATTERNS = [
    r"(?i)without over-claiming external proxy data as direct hourly spa demand",
    r"(?i)not as proof of hourly demand structure",
    r"(?i)no hourly/daypart truth claims",
    r"(?i)not pseudo-precision",
]

def fail(msg: str) -> None:
    print(f"[FAIL] {msg}")
    sys.exit(1)

def ok(msg: str) -> None:
    print(f"[OK] {msg}")

def info(msg: str) -> None:
    print(f"[INFO] {msg}")

def main() -> None:
    if not TARGET.exists():
        fail(f"Markdown output not found: {TARGET}")
    ok(f"Markdown output exists: {TARGET}")

    text = TARGET.read_text(encoding="utf-8").strip()

    if not text:
        fail("Markdown output is empty")
    ok("Markdown output is non-empty")

    info(f"char_count={len(text)}")
    info(f"line_count={len(text.splitlines())}")

    for pattern in REQUIRED_SECTION_PATTERNS:
        if not re.search(pattern, text):
            fail(f"Missing required section pattern: {pattern}")
    ok("Required sections present")

    for pattern in BOUNDARY_SAFE_PATTERNS:
        if not re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL):
            fail(f"Missing methodological boundary wording: {pattern}")
    ok("Boundary-safe wording present")

    sanitized_text = text
    for pattern in ALLOWED_GUARDRAIL_PATTERNS:
        sanitized_text = re.sub(pattern, "", sanitized_text, flags=re.IGNORECASE | re.DOTALL)

    violations = []
    for pattern in FORBIDDEN_PATTERNS:
        for m in re.finditer(pattern, sanitized_text, flags=re.IGNORECASE):
            violations.append((pattern, m.group(0)))

    if violations:
        info("Forbidden wording hits:")
        for pattern, hit in violations:
            print(f" - pattern={pattern} hit={hit}")
        fail("Found hourly / pseudo-granular overclaim wording outside approved guardrail context")
    ok("No pseudo-daypart / hourly overclaim wording found")

    print("\n=== FINAL STATUS ===")
    print("[OK] monthly_roster_management_readout markdown QA passed")

if __name__ == "__main__":
    main()
