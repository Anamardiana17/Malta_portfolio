from __future__ import annotations

from pathlib import Path
import csv


REPO_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_PATH = REPO_ROOT / "data_processed" / "management" / "management_layer_review_log_template.csv"

FIELDNAMES = [
    "review_cycle_id",
    "artifact_name",
    "artifact_type",
    "review_scope",
    "reviewer_name",
    "review_date",
    "review_status",
    "evidence_reference",
    "follow_up_action",
    "handoff_ready_flag",
    "notes",
]

TEMPLATE_ROWS = [
    {
        "review_cycle_id": "TEMPLATE",
        "artifact_name": "management_layer_reviewer_quickstart.md",
        "artifact_type": "markdown",
        "review_scope": "template_demonstration_only",
        "reviewer_name": "",
        "review_date": "",
        "review_status": "pending",
        "evidence_reference": "Use relative repo path, commit hash, or QA output reference",
        "follow_up_action": "",
        "handoff_ready_flag": "no",
        "notes": "Replace or append with actual review records; keep this row only if using template mode.",
    }
]


def main() -> None:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    with OUTPUT_PATH.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(TEMPLATE_ROWS)

    print(f"[OK] Wrote review log template: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
