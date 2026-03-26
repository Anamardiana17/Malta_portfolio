# Management Layer Package Guide

## Purpose

This document summarizes the tracked management-layer package for packaging, governance, and reviewer readability. It helps explain what each artifact contributes without introducing new modelling logic.

## Method Boundary

- External proxies are retained as contextual regime and market-pressure inputs only.
- Internal operating proxies remain the anchor for actionable management decisions.
- No pseudo-daypart logic is introduced in this package guide.
- No roster-by-hour inference is introduced without valid granular source support.
- This guide is documentation only and does not extend the modelling layer.

## Artifact Coverage

| Artifact Key | Type | Role | Tracked | QA | Path |
|---|---|---|---:|---:|---|
| monthly_roster_management_interpretation | dataset | decision_interpretation | 1 | 1 | `data_processed/management/monthly_roster_management_interpretation.csv` |
| management_layer_index | documentation_index | documentation_index | 1 | 1 | `data_processed/management/management_layer_index.md` |
| management_layer_qa_aggregator | qa_orchestration | governance_control | 1 | 0 | `scripts/qa/run_management_layer_qa.py` |
| monthly_roster_management_readout | markdown_readout | manager_readout | 0 | 1 | `output/management/monthly_roster_management_readout.md` |

## Artifact Notes

### monthly_roster_management_interpretation

- Role: decision_interpretation
- Source dependency class: internal_anchor_plus_contextual_external_regime
- QA script: scripts/qa/validate_monthly_roster_management_interpretation.py
- Boundary note: Uses internal operating proxies as primary decision anchor. External proxies remain contextual regime inputs only. No synthetic intra-day staffing segmentation. No hour-level roster inference without valid granular source.

### management_layer_index

- Role: documentation_index
- Source dependency class: registry_derived_documentation
- QA script: scripts/qa/validate_management_layer_index.py
- Boundary note: Tracked readability and packaging layer derived from the management registry. Documents artifact scope and governance boundaries without adding new modelling logic.

### management_layer_qa_aggregator

- Role: governance_control
- Source dependency class: repo_governance
- QA script: None
- Boundary note: QA orchestration layer for management artifacts. Supports governance, packaging discipline, and methodological defensibility.

### monthly_roster_management_readout

- Role: manager_readout
- Source dependency class: derived_readout_from_management_interpretation
- QA script: scripts/qa/validate_monthly_roster_management_markdown_readout.py
- Boundary note: Local packaging artifact only. Derived from management interpretation layer. Must preserve methodological guardrails and avoid unsupported hour-level staffing claims.

## Reviewer Reading Order

1. Start with the management layer index for navigation.
2. Review the registry for artifact traceability and governance coverage.
3. Review interpretation/readout artifacts for management-facing packaging.
4. Run the QA aggregator before packaging or presenting the stack.

## Governance Notes

- Output artifacts under `output/` remain local-only and are not part of the tracked management-layer package.

- output/ remains local-only and should not be tracked.
- This package guide should be refreshed when tracked management-layer artifacts change.
- The guide reinforces methodological defensibility; it does not add new analytical claims.
