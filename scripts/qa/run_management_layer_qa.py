from pathlib import Path
import subprocess
import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
QA_DIR = REPO_ROOT / "scripts" / "qa"

VALIDATORS = [
    "validate_monthly_roster_management_interpretation.py",
    "validate_monthly_roster_management_markdown_readout.py",
    "validate_management_layer_registry.py",
    "validate_management_layer_index.py",
    "validate_management_layer_package_guide.py",
    "validate_management_layer_reviewer_checklist.py",
    "validate_management_layer_traceability_matrix.py",
    "validate_management_layer_release_readiness_note.py",
    "validate_management_layer_governance_changelog.py",
    "validate_management_layer_artifact_lifecycle_policy.py",
    "validate_management_layer_governance_manifest.py",
    "validate_management_layer_reviewer_quickstart.py",
    "validate_management_layer_review_log_template.py",
    "validate_management_layer_review_disposition_matrix.py",
]

def main():
    print("=== MANAGEMENT LAYER QA ===")
    print(f"[INFO] repo_root={REPO_ROOT}")

    passed = 0
    failed = 0

    for script_name in VALIDATORS:
        fp = QA_DIR / script_name
        print(f"\n--- RUN {script_name} ---")
        result = subprocess.run([sys.executable, str(fp)])
        if result.returncode == 0:
            passed += 1
        else:
            failed += 1

    total = passed + failed
    print("\n=== MANAGEMENT LAYER QA SUMMARY ===")
    print(f"passed={passed} failed={failed} total={total}")

    if failed:
        sys.exit(1)

if __name__ == "__main__":
    main()
