# Management Layer Index

This index documents the current management-layer artifacts used for interpretation, packaging, and governance.

## Boundary framing

- Internal operating proxies remain the primary anchor for actionable decisions.
- External proxies remain contextual regime or market-pressure inputs only.
- No synthetic intra-day staffing segmentation is introduced.
- No hour-level roster inference is introduced without valid granular source support.

## Artifact register

### monthly_roster_management_interpretation

- Path: `data_processed/management/monthly_roster_management_interpretation.csv`
- Type: `dataset`
- Role: Decision interpretation
- Tracked in repo: yes
- Local-only output: no
- QA-covered: yes
- QA script: `scripts/qa/validate_monthly_roster_management_interpretation.py`
- Dependency class: `internal_anchor_plus_contextual_external_regime`
- Boundary note: Uses internal operating proxies as primary decision anchor. External proxies remain contextual regime inputs only. No synthetic intra-day staffing segmentation. No hour-level roster inference without valid granular source.

### management_layer_index

- Path: `data_processed/management/management_layer_index.md`
- Type: `documentation_index`
- Role: Documentation index
- Tracked in repo: yes
- Local-only output: no
- QA-covered: yes
- QA script: `scripts/qa/validate_management_layer_index.py`
- Dependency class: `registry_derived_documentation`
- Boundary note: Tracked readability and packaging layer derived from the management registry. Documents artifact scope and governance boundaries without adding new modelling logic.

### management_layer_qa_aggregator

- Path: `scripts/qa/run_management_layer_qa.py`
- Type: `qa_orchestration`
- Role: Governance control
- Tracked in repo: yes
- Local-only output: no
- QA-covered: no
- QA script: `nan`
- Dependency class: `repo_governance`
- Boundary note: QA orchestration layer for management artifacts. Supports governance, packaging discipline, and methodological defensibility.

### monthly_roster_management_readout

- Path: `output/management/monthly_roster_management_readout.md`
- Type: `markdown_readout`
- Role: Manager readout
- Tracked in repo: no
- Local-only output: yes
- QA-covered: yes
- QA script: `scripts/qa/validate_monthly_roster_management_markdown_readout.py`
- Dependency class: `derived_readout_from_management_interpretation`
- Boundary note: Local packaging artifact only. Derived from management interpretation layer. Must preserve methodological guardrails and avoid unsupported hour-level staffing claims.

### management_layer_reviewer_checklist

- Path: `data_processed/management/management_layer_reviewer_checklist.md`
- Type: `documentation`
- Role: review_governance
- Tracked in repo: yes
- Local-only output: no
- QA-covered: yes
- QA script: `scripts/qa/validate_management_layer_reviewer_checklist.py`
- Dependency class: `repo_governance`
- Boundary note: Reviewer governance artifact only. Supports package integrity, reviewer usability, and methodological defensibility without adding modelling logic or unsupported staffing inference.

### management_layer_traceability_matrix

- Path: `data_processed/management/management_layer_traceability_matrix.csv`
- Type: `documentation_dataset`
- Role: traceability_governance
- Tracked in repo: yes
- Local-only output: no
- QA-covered: yes
- QA script: `scripts/qa/validate_management_layer_traceability_matrix.py`
- Dependency class: `repo_governance`
- Boundary note: Governance traceability artifact only. Maps package lineage across management-layer artifacts, QA coverage, and reviewer usability without adding modelling logic, synthetic intra-day segmentation, or unsupported hour-level roster inference.

## Governance notes

- `output/` remains local-only and should not be tracked.
- This index is a packaging and readability layer, not a modelling layer.
- Registry and QA coverage should be updated when management-layer artifacts change.
