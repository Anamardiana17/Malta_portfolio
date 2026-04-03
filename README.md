# Malta Portfolio

## Repo Positioning
Malta_portfolio is a spa operations and management portfolio project designed to support business interpretation and operational decision-making across a multi-outlet environment. It is built as a management-oriented decision-support model, not as a live production deployment or a generic analytics showcase.

It is intended for reviewers, operators, and decision-makers who need to understand business health, demand context, operational trade-offs, and action priorities in a structured and defensible way.


## Repository Map / Where to Start
For a fast review, use this path:

1. `README.md`  
   Start here for project positioning, methodological guardrails, and the full Malta_bigger_picture.

2. `docs/executive_portfolio_summary.md`  
   Use this for a compact management-facing summary of the portfolio.

3. `apps/gui_control_panel/README.md`  
   Use this to understand the governed GUI control layer and its reviewer-facing workflow.

4. `deliverables/`  
   Use this to inspect selected executive and operating-model outputs.

This order is designed to help a reviewer move from overall positioning to executive interpretation, then to GUI workflow and supporting deliverables.

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
