# Malta Portfolio

## What this portfolio solves

This portfolio is designed to help spa management teams move from KPI reporting to governed operational action.

It demonstrates how internal operating data can be translated into:
- outlet-level business health visibility
- leakage and strain detection
- prioritized management action queues
- staffing and control signals
- reviewer-facing governance and traceability

It is not positioned as a live production deployment or a direct export of real operating company data.  
It is a governed decision-support portfolio built to show analytical rigor, management applicability, and methodological discipline.

## Reviewer fast path

If you are reviewing this portfolio quickly, start here:

1. Read the repo positioning and portfolio scope in this README.
2. Open the GUI screenshot index in `docs/gui_screenshot_index.md`.
3. Review how governed inputs, KPI visibility, and action prioritization are shown in the screenshot pack.
4. Use the documentation layer to confirm governance, methodological boundaries, and reviewer-facing traceability.

This repo is designed to be understandable in a short screening pass while still supporting deeper reviewer validation.

## Why this is more than a dashboard

This portfolio is not only a visual reporting layer.

It is designed to show that governed processing comes before display:
- inputs are reviewed through acceptance and control logic
- processing and QA evidence are part of the reviewer-facing story
- KPI outputs are translated into management interpretation
- action prioritization is treated as a decision-support layer, not just a chart layer
- governance and traceability remain visible to reviewers

This positioning matters because the portfolio is intended to demonstrate operational decision support, not dashboard styling alone.

## Repo Positioning
Malta_portfolio is a management-oriented spa operations and decision-support portfolio for multi-outlet environments. It shows how governed operational data can be translated into KPI guardrails, business interpretation, and management action for reviewer-facing and hiring-facing evaluation.

This repository is best suited for spa management, spa operations, and analytically literate reviewers who want to see more than a dashboard: a governed decision-support workflow with clear methodological boundaries.

It is not positioned as a live production deployment or a generic analytics showcase.

## Analytics Layer
This portfolio is designed as a governed analytics and decision-support layer, not just a presentation surface.

It highlights:
- governed batch intake before interpretation
- explicit separation between batch processing context and analytical month context
- KPI and guardrail logic for business-health reading
- cross-outlet comparative monitoring
- trend and portfolio composition reading
- prioritized management action rather than dashboard-only display

The result is a reviewer-facing analytics interface that connects governed processing, KPI visibility, portfolio monitoring, and management prioritization.

## Malta_bigger_picture
The core logic of this project is simple:

- internal POS and operating data act as the primary operational truth
- external Malta demand proxies act as contextual guardrails, not exact demand truth

This means the project does not treat tourism, mobility, or market context as a substitute for real operational performance. External signals are used to frame pressure, seasonality, and commercial context around the spa business, while internal operating data remains the main anchor for action.

## System Architecture
The project is organized across six layers:

1. Input Layer  
   Governed intake of raw operational data, including upload structure, registry logic, and controlled batch handling.

2. External Context Layer  
   Malta-specific contextual demand proxies used to frame market pressure, seasonality, and commercial environment.

3. Processing Layer  
   Structured transformation of raw and intermediate data into governed analytical outputs.

4. KPI / Guardrail Layer  
   Business and operational KPIs combined with methodological guardrails to prevent overclaiming and weak inference.

5. Interpretation Layer  
   Translation of KPI signals into managerial interpretation, business health reading, and recommended actions.

6. Presentation Layer  
   Executive dashboard views, decision-support outputs, reviewer-facing evidence, and governed GUI surfaces.

## Methodological Guardrails
This project is intentionally constrained by clear methodological boundaries:

- internal POS and operational data remain the primary truth for action
- external proxies are contextual decision-support inputs only
- no unsupported intra-day staffing inference is introduced
- no claim is made that external demand signals represent exact spa demand truth
- no promotional recommendation should damage yield balance or service stability
- no revenue framing should ignore therapist sustainability or operating strain

## GUI Positioning
The GUI is positioned as a governed operations control layer above validated Malta_portfolio artifacts.

It is not the core engine.  
It does not replace the validated processing pipeline.  
Its role is to help non-technical reviewers and operators move from governed intake to controlled processing, KPI visibility, decision support, and downloadable evidence more clearly.

## Output Layer
The project is designed to produce management-oriented outputs such as:

- executive dashboard views
- business health summaries
- action-oriented decision support
- processed CSV outputs
- market interaction context
- spa customer capture context

These outputs are intended to support structured review and operational interpretation rather than function as a live production control system.

## Demo Data Disclaimer
The raw POS-style datasets used for the 2017–2025 demonstration workflow are generated/demo data only. They are included to demonstrate governed processing, KPI interpretation, and management-facing output structure. They are not live company operating data.

## Current Direction
The current repo direction is to strengthen:

- governed intake and controlled processing
- management-facing KPI interpretation
- reviewer-facing evidence visibility
- GUI-supported access to validated outputs
- clearer public-facing framing of the full Malta_bigger_picture

## Data Sources and Provenance

External context sources used in this project are documented in `docs/data_sources_and_rights.md` and `docs/dataset_provenance_register.csv`.

Official statistics are cited to their original publishers. Demo operational datasets are generated for portfolio demonstration and are not live company data. External contextual proxies are used as decision-support context only and not as direct spa demand truth.
