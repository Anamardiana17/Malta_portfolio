# Management Layer Index

## Purpose
This index provides a reviewer-facing navigation layer for the management package.
It is designed to improve governance readability, package traceability, and handoff usability without introducing any new modelling logic.

## Boundary framing
- External proxies remain contextual regime or market-pressure inputs only.
- Internal operating proxies remain the primary anchor for actionable decisions.
- No synthetic intra-day staffing segmentation is introduced.
- No hour-level roster inference is introduced without valid granular source support.
- `output/` remains local-only and should not be tracked.

## Reviewer Reading Order
1. `monthly_roster_management_interpretation.csv`
2. `management_layer_registry.csv`
3. `management_layer_index.md`
4. `management_layer_package_guide.md`
5. `management_layer_reviewer_checklist.md`
6. `management_layer_traceability_matrix.csv`
7. `management_layer_release_readiness_note.md`
8. `management_layer_governance_changelog.md`
9. `management_layer_artifact_lifecycle_policy.md`

## Artifact register

### monthly_roster_management_interpretation
- File: `data_processed/management/monthly_roster_management_interpretation.csv`
- Role: Core management interpretation layer
- Reviewer use: Read the core management-facing interpretation output first
- QA: `scripts/qa/validate_monthly_roster_management_interpretation.py`

### management_layer_registry
- File: `data_processed/management/management_layer_registry.csv`
- Role: Governance artifact inventory
- Reviewer use: Confirm tracked artifact coverage and package composition
- QA: `scripts/qa/validate_management_layer_registry.py`

### management_layer_index
- File: `data_processed/management/management_layer_index.md`
- Role: Reviewer navigation layer
- Reviewer use: Navigate package structure and reading order
- QA: `scripts/qa/validate_management_layer_index.py`

### management_layer_package_guide
- File: `data_processed/management/management_layer_package_guide.md`
- Role: Package handoff guidance
- Reviewer use: Understand package scope, boundaries, and review posture
- QA: `scripts/qa/validate_management_layer_package_guide.py`

### management_layer_reviewer_checklist
- File: `data_processed/management/management_layer_reviewer_checklist.md`
- Role: Reviewer control checklist
- Reviewer use: Apply structured review and governance checks
- QA: `scripts/qa/validate_management_layer_reviewer_checklist.py`

### management_layer_traceability_matrix
- File: `data_processed/management/management_layer_traceability_matrix.csv`
- Role: Artifact-to-QA traceability map
- Reviewer use: Confirm traceability coverage and governance mapping
- QA: `scripts/qa/validate_management_layer_traceability_matrix.py`

### management_layer_release_readiness_note
- File: `data_processed/management/management_layer_release_readiness_note.md`
- Role: Release readiness and handoff trust note
- Reviewer use: Assess release posture and packaging trust
- QA: `scripts/qa/validate_management_layer_release_readiness_note.py`

### management_layer_governance_changelog
- File: `data_processed/management/management_layer_governance_changelog.md`
- Role: Governance evolution and handoff trust log
- Reviewer use: Understand staged governance expansion and package maturity
- QA: `scripts/qa/validate_management_layer_governance_changelog.py`

### management_layer_artifact_lifecycle_policy
- File: `data_processed/management/management_layer_artifact_lifecycle_policy.md`
- Role: Artifact lifecycle and archive boundary policy
- Reviewer use: Distinguish governed artifacts, local-only outputs, and archived artifacts excluded from the main repo
- QA: `scripts/qa/validate_management_layer_artifact_lifecycle_policy.py`

## Governance notes
This management layer stack should be interpreted as a governance-structured management portfolio layer.
It should not be interpreted as a live production system, a real-time staffing engine, a daypart forecasting engine, or an hourly staffing optimization system.
