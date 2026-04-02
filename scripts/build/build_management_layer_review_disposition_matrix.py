from __future__ import annotations

from pathlib import Path
import csv


ROWS = [
    {
        "review_disposition_code": "accept",
        "review_disposition_label": "Accept",
        "finding_severity": "none",
        "release_eligibility": "eligible",
        "requires_artifact_change": "no",
        "requires_build_rerun": "no",
        "requires_qa_rerun": "no",
        "reviewer_evidence_expectation": "Reviewer confirms artifact is usable as-is and governance checks remain satisfied.",
        "handoff_implication": "Artifact can proceed without follow-up condition.",
        "audit_note_guidance": "Log brief confirmation of scope reviewed and no material issue identified.",
    },
    {
        "review_disposition_code": "accept_with_followup",
        "review_disposition_label": "Accept with Follow-up",
        "finding_severity": "low",
        "release_eligibility": "eligible_with_followup",
        "requires_artifact_change": "optional",
        "requires_build_rerun": "no",
        "requires_qa_rerun": "no",
        "reviewer_evidence_expectation": "Reviewer records minor clarity, wording, or usability issue that does not reduce governance defensibility.",
        "handoff_implication": "Artifact may be handed off now, but follow-up should be logged for next maintenance cycle.",
        "audit_note_guidance": "Describe issue precisely and record why release is still acceptable.",
    },
    {
        "review_disposition_code": "revise_before_release",
        "review_disposition_label": "Revise Before Release",
        "finding_severity": "medium",
        "release_eligibility": "not_eligible_until_revised",
        "requires_artifact_change": "yes",
        "requires_build_rerun": "yes",
        "requires_qa_rerun": "yes",
        "reviewer_evidence_expectation": "Reviewer identifies issue that weakens usability, auditability, traceability, or governance clarity enough to require correction before release.",
        "handoff_implication": "Artifact must not be treated as ready until revision and QA rerun are complete.",
        "audit_note_guidance": "Record defect, impacted artifact(s), expected correction, and validation required after fix.",
    },
    {
        "review_disposition_code": "block_release",
        "review_disposition_label": "Block Release",
        "finding_severity": "high",
        "release_eligibility": "blocked",
        "requires_artifact_change": "yes",
        "requires_build_rerun": "yes",
        "requires_qa_rerun": "yes",
        "reviewer_evidence_expectation": "Reviewer identifies material governance failure, missing required artifact content, broken traceability, or issue that undermines handoff trust.",
        "handoff_implication": "Release must be blocked and escalated until issue is corrected and evidence is revalidated.",
        "audit_note_guidance": "Record blocking rationale, escalation owner, correction path, and explicit unblock condition.",
    },
]


def main() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    output_path = repo_root / "data_processed" / "management" / "management_layer_review_disposition_matrix.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = list(ROWS[0].keys())

    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(ROWS)

    print(f"[OK] Wrote: {output_path}")
    print(f"[INFO] rows={len(ROWS)} cols={len(fieldnames)}")


if __name__ == "__main__":
    main()
