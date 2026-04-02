from pathlib import Path
import pandas as pd


def build_management_layer_qa_escalation_matrix() -> Path:
    repo_root = Path(__file__).resolve().parents[2]
    output_path = (
        repo_root
        / "data_processed"
        / "management"
        / "management_layer_qa_escalation_matrix.csv"
    )

    rows = [
        {
            "escalation_code": "ML_QA_001",
            "qa_trigger_type": "validator_failure",
            "severity_band": "blocker",
            "release_action": "block_release",
            "owner_role": "qa_owner",
            "reviewer_action_required": (
                "Stop release motion, isolate failing validator, confirm affected artifact scope, "
                "and open remediation entry in the review log."
            ),
            "evidence_required": (
                "Passing rerun of failed validator, updated artifact if needed, and review-log entry "
                "documenting root cause and remediation."
            ),
            "rerun_requirement": "Rerun failing validator and full run_management_layer_qa.py after remediation.",
            "closure_condition": "Failure cleared and full management-layer QA passes.",
            "handoff_note": "Do not hand off package while blocker QA failure remains open.",
        },
        {
            "escalation_code": "ML_QA_002",
            "qa_trigger_type": "registry_drift",
            "severity_band": "high",
            "release_action": "hold_pending_fix",
            "owner_role": "repo_maintainer",
            "reviewer_action_required": (
                "Reconcile artifact registration, build/validator references, and package visibility "
                "before reviewer signoff proceeds."
            ),
            "evidence_required": (
                "Updated registry row, matching index visibility, and clean QA rerun showing no governance drift."
            ),
            "rerun_requirement": "Rerun affected validator and full management-layer QA.",
            "closure_condition": "Registry references are aligned with artifact reality and QA passes.",
            "handoff_note": "Do not present governance stack as complete while registry drift exists.",
        },
        {
            "escalation_code": "ML_QA_003",
            "qa_trigger_type": "traceability_drift",
            "severity_band": "high",
            "release_action": "hold_pending_fix",
            "owner_role": "repo_maintainer",
            "reviewer_action_required": (
                "Restore traceability links between artifact, build script, validator, and review pathway."
            ),
            "evidence_required": (
                "Updated traceability matrix plus QA evidence confirming linked artifact references resolve cleanly."
            ),
            "rerun_requirement": "Rerun traceability-related validator and full management-layer QA.",
            "closure_condition": "Traceability row is restored and linked governance references are consistent.",
            "handoff_note": "Traceability gaps reduce audit trust and should be closed before release.",
        },
        {
            "escalation_code": "ML_QA_004",
            "qa_trigger_type": "missing_artifact",
            "severity_band": "blocker",
            "release_action": "block_release",
            "owner_role": "artifact_owner",
            "reviewer_action_required": (
                "Rebuild or restore missing artifact, then confirm the package is materially complete."
            ),
            "evidence_required": (
                "Artifact restored at canonical path, validator pass, and registry/index presence confirmed."
            ),
            "rerun_requirement": "Rerun artifact-specific validator and full management-layer QA.",
            "closure_condition": "Artifact exists at expected path and governance references resolve cleanly.",
            "handoff_note": "Missing canonical artifact invalidates handoff completeness.",
        },
        {
            "escalation_code": "ML_QA_005",
            "qa_trigger_type": "stale_handoff_metadata",
            "severity_band": "medium",
            "release_action": "allow_with_note",
            "owner_role": "reviewer",
            "reviewer_action_required": (
                "Refresh handoff-facing metadata and document the refresh in the review log before final packaging."
            ),
            "evidence_required": (
                "Updated metadata-bearing artifact and review-log note confirming freshness check."
            ),
            "rerun_requirement": "Rerun affected validator if metadata is validated programmatically.",
            "closure_condition": "Reviewer-facing metadata is current and logged.",
            "handoff_note": "May proceed only with explicit note if not release-blocking.",
        },
        {
            "escalation_code": "ML_QA_006",
            "qa_trigger_type": "manual_review_override",
            "severity_band": "medium",
            "release_action": "allow_with_note",
            "owner_role": "reviewer",
            "reviewer_action_required": (
                "Record why manual override was necessary, what residual risk remains, and what follow-up is required."
            ),
            "evidence_required": (
                "Review-log entry with rationale, risk statement, and named follow-up action."
            ),
            "rerun_requirement": "Rerun relevant QA where feasible after override-related changes.",
            "closure_condition": "Override is documented and residual risk is explicitly accepted.",
            "handoff_note": "Override must never be silent; audit trail is mandatory.",
        },
        {
            "escalation_code": "ML_QA_007",
            "qa_trigger_type": "format_integrity_issue",
            "severity_band": "low",
            "release_action": "monitor_only",
            "owner_role": "artifact_owner",
            "reviewer_action_required": (
                "Correct minor format inconsistency if it affects readability, otherwise log and monitor."
            ),
            "evidence_required": (
                "Updated artifact or logged rationale for deferral."
            ),
            "rerun_requirement": "Rerun artifact-specific validator if formatting is validator-sensitive.",
            "closure_condition": "Formatting issue corrected or explicitly deferred with rationale.",
            "handoff_note": "Low-severity readability issue should still be visible in the audit trail.",
        },
    ]

    df = pd.DataFrame(rows)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)

    print(f"[OK] Wrote: {output_path}")
    print(f"[INFO] shape={df.shape}")
    return output_path


if __name__ == "__main__":
    build_management_layer_qa_escalation_matrix()
