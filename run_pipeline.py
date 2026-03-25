from pathlib import Path
import subprocess
import sys

BASE = Path(__file__).resolve().parent

STEPS = [
    ("Build management insight marts", BASE / "scripts/build/build_management_insight_marts.py"),
    ("Build dashboard export layer", BASE / "scripts/build/build_dashboard_export_layer.py"),
    ("Validate dashboard export layer", BASE / "scripts/qa/validate_dashboard_export_layer.py"),
]

def run_step(label: str, script_path: Path) -> None:
    print(f"\n=== {label} ===")
    print(f"[RUN] {script_path}")
    result = subprocess.run([sys.executable, str(script_path)], cwd=BASE)
    if result.returncode != 0:
        raise SystemExit(f"[FAIL] Step failed: {label}")

def main() -> None:
    print("[INFO] Starting Malta portfolio local pipeline")
    print(f"[INFO] Base directory: {BASE}")

    for label, script_path in STEPS:
        if not script_path.exists():
            raise SystemExit(f"[FAIL] Missing script: {script_path}")
        run_step(label, script_path)

    print("\n[OK] Pipeline completed successfully.")

if __name__ == "__main__":
    main()
