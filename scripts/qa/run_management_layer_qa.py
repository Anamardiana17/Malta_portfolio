from __future__ import annotations

import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
QA_DIR = ROOT / "scripts" / "qa"

VALIDATORS = [
    QA_DIR / "validate_monthly_roster_management_interpretation.py",
    QA_DIR / "validate_monthly_roster_management_markdown_readout.py",
    QA_DIR / "validate_management_layer_registry.py",
    QA_DIR / "validate_management_layer_index.py",
    QA_DIR / "validate_management_layer_package_guide.py",
]


def run_validator(script_path: Path) -> int:
    print(f"\n[RUN] {script_path.name}")
    result = subprocess.run([sys.executable, str(script_path)], cwd=ROOT)
    if result.returncode == 0:
        print(f"[PASS] {script_path.name}")
    else:
        print(f"[FAIL] {script_path.name} (exit_code={result.returncode})")
    return result.returncode


def main() -> int:
    print("=== MANAGEMENT LAYER QA ===")
    print(f"[INFO] repo_root={ROOT}")

    passed = 0
    total = len(VALIDATORS)

    for script_path in VALIDATORS:
        if not script_path.exists():
            print(f"[FAIL] Missing validator: {script_path}")
            return 1

        exit_code = run_validator(script_path)
        if exit_code != 0:
            print("\n=== MANAGEMENT LAYER QA SUMMARY ===")
            print(f"[RESULT] FAILED at {script_path.name}")
            print(f"[SUMMARY] passed={passed} failed=1 total={total}")
            return exit_code

        passed += 1

    print("\n=== MANAGEMENT LAYER QA SUMMARY ===")
    print("[RESULT] ALL CHECKS PASSED")
    print(f"[SUMMARY] passed={passed} failed=0 total={total}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
