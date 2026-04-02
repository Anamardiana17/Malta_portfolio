# Management Layer Reviewer Checklist

## Purpose
This checklist helps a reviewer verify that the management layer remains readable, governed, and methodologically defensible without introducing unsupported operating claims.

## Reviewer Checks

### 1. Scope discipline
- Confirm the management layer remains a decision-support and interpretation layer, not a live production system claim.
- Confirm external proxies are used only as contextual regime / market-pressure context.
- Confirm internal operating proxies remain the anchor for actionable management interpretation.
- Confirm no pseudo-daypart logic is introduced.
- Confirm no roster-by-hour inference is introduced without valid granular source support.

### 2. Package integrity
- Confirm `management_layer_registry.csv` exists and is readable.
- Confirm `management_layer_index.md` exists and is readable.
- Confirm `management_layer_package_guide.md` exists and is readable.
- Confirm this checklist file exists and is readable.
- Confirm the files referenced in the registry align with the documented package structure.

### 3. Readability and reviewer usability
- Confirm each documented artifact has a clear purpose statement.
- Confirm the package guide explains how a reviewer should read the layer in order.
- Confirm the index is structured for navigation rather than narrative overclaim.
- Confirm documentation language avoids unsupported certainty and avoids pseudo-operational precision.

### 4. QA discipline
- Confirm management-layer QA runs successfully from the repo script entrypoint.
- Confirm the QA stack includes checks for package structure and documentation integrity.
- Confirm output artifacts remain local-only and are not tracked when they belong in `output/`.

### 5. Methodological defensibility
- Confirm wording preserves the distinction between:
  - contextual external pressure
  - internal operating signal
  - management interpretation
- Confirm no claim implies causal certainty where only structured interpretation is supported.
- Confirm management readouts remain anchored to observable or derived internal operating proxies.

## Suggested reviewer reading order
1. `management_layer_index.md`
2. `management_layer_package_guide.md`
3. `management_layer_registry.csv`
4. management interpretation outputs and markdown readouts
5. QA scripts under `scripts/qa/`

## Sign-off standard
A reviewer can consider the management layer package ready for portfolio or repository review only if:
- package files are present,
- cross-references are consistent,
- QA passes,
- no unsupported operational overclaim is introduced,
- and reviewer navigation remains clear.

## Boundary note
This checklist is a governance and review aid. It does not introduce new modelling, new commercial logic, or new staffing inference.
