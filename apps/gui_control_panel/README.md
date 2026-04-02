# Malta GUI Control Panel

## Purpose
This GUI is a governed control layer above the Malta portfolio pipeline. It is designed to help a reviewer, manager, or operator understand how uploaded input batches are controlled before downstream processing and how execution activity is logged in a traceable way.

## What this GUI currently demonstrates
- governed batch intake through the data input panel
- schema profiling before downstream use
- manual acceptance review with reviewer-facing rationale
- accepted / rejected / hold batch movement
- accepted-only processing eligibility gate
- controlled processing trigger logging
- controlled execution wrapper with status capture
- visibility into artifact readiness, QA summary, and processing history

## Why this matters operationally
This GUI is not positioned as a generic dashboard shell. It demonstrates an operations-oriented workflow:
1. new data enters through an intake layer
2. the batch is profiled and reviewed
3. unreviewed or rejected batches are prevented from entering downstream processing
4. only accepted batches are eligible for processing
5. processing actions are logged for traceability and reviewer follow-up

This structure supports operational discipline, reviewability, and controlled execution rather than ad hoc processing.

## Management-facing value
This layer is intended to show how KPI-oriented operations can be governed through a control workflow:
- prevent unreviewed batch processing
- separate intake, review, acceptance, and execution stages
- preserve an audit trail for processing actions
- create a reviewer-facing bridge between technical processing and operational accountability

## Current panel structure
- Data Input Panel
- Processing / QA Panel
- KPI / Executive Dashboard Panel
- Decision Support Panel
- Export / Reviewer Pack Panel

## Boundary notes
- the GUI remains a control and presentation layer above the repository logic
- the GUI does not replace the core repo pipeline
- the GUI should not introduce synthetic intra-day staffing logic
- the GUI should not introduce unsupported staffing inference
- output territory remains separate from raw upload territory

## Current positioning
This GUI supports the broader Malta portfolio by showing that the project is not only about KPI outputs, but also about governed operational handling of inputs, processing readiness, and execution traceability.

## Next logical extensions
- connect controlled execution to downstream processing runners
- surface execution outcome summaries more prominently
- strengthen reviewer pack and management-facing explanation layers
- improve recruiter-facing screenshots and guided walkthrough materials
