# Malta Spa Portfolio

A management-focused portfolio project for evaluating a Malta spa network through pricing, staffing, therapist structure, operating-role design, and executive decision support.

## What this project demonstrates
This repository shows how a spa manager can combine commercial thinking, operational control, structured analytics, and people-management logic into a recruiter-facing management portfolio.

Rather than stopping at descriptive analysis, the project translates network conditions into practical management outputs such as outlet positioning, staffing interpretation, therapist coaching priorities, treatment opportunity signals, retail-selling and upsell logic, reward-recognition signals, refresh-training needs, and executive operating readouts.

## Portfolio scope
This portfolio is built to demonstrate a structured management lens across:
- outlet strategy and network positioning
- pricing and treatment economics
- staffing and roster control layers
- therapist segmentation and coaching logic
- operating-role framework for receptionist, spa attendant, assistant manager, and spa manager
- management KPI signal layer
- treatment health and management exception views
- retail-selling score and therapist upsell score logic
- reward-recognition and refresh-training support signals
- executive operating readout and manager action queue

## Key management outputs
This repository is designed to produce reviewer-friendly outputs, including:
- executive portfolio readouts
- outlet-level management summaries
- therapist coaching summaries
- treatment opportunity summaries
- staffing management views
- manager action queues
- staff commercial scoring outputs
- therapist reward / top-bottom performance outputs

## Why Malta
Malta provides a compact but commercially meaningful spa-market context shaped by tourism exposure, premium positioning, locality differences, staffing pressure, and service-demand variation across outlets.

That makes it a strong setting for demonstrating how a spa network can be interpreted through a structured management and operating model.

## Repository structure
- `docs/` -> recruiter-facing project narrative and README support sections
- `deliverables/` -> presentation-ready outputs for portfolio review
- `scripts/build/` -> main build pipeline for management, pricing, and operating-model layers
- `scripts/patch/` -> controlled patch logic used to refine selected decision layers
- `scripts/qa/` -> validation checks for management, pricing, roster, governance, and commercial scoring outputs
- `scripts/research/` -> Malta spa market and outlet research build steps
- `scripts/transform/` -> external proxy preparation and transformation logic
- `data_raw/` -> source inputs and external reference captures
- `data_processed/` -> processed analytical outputs and management-facing layers
- `backups/` -> archived work, versioned variants, and recovery artifacts
- `snapshots/` -> preserved stable project snapshots

## Build flow
`research inputs / source captures` -> `transformation and proxy preparation` -> `build pipelines` -> `patch logic where needed` -> `QA / validation checks` -> `management-facing deliverables`

## Project basis
This project is a management portfolio and decision-support model built to translate structured analysis into practical operating decisions.

It is based on market research, operating assumptions, modeled management logic, and management-facing outputs rather than on a live production system or a direct internal export from one spa operator.

## What this is not
This repository is not presented as a live production deployment, a direct export from one spa operator, or a demand-forecasting engine.

It is a management portfolio built to demonstrate structured commercial reasoning, operating-model interpretation, staffing realism checks, reward/coaching governance, and decision-support discipline under controlled assumptions.

## Operating model / management layer
The operating-model layer translates the Malta spa network into a management-facing decision framework.

It connects outlet staffing position, therapist structural segmentation, non-therapist operating-role architecture, commercial contribution logic, and outlet-level people-management priorities into one executive read. The aim is to show how a multi-outlet spa network can be interpreted through pricing, staffing, service mix, control discipline, commercial execution, and operating risk.

### Why it matters
- shows outlet-level differentiation instead of treating the network as operationally uniform
- converts therapist structure into actionable management priorities
- adds receptionist, spa attendant, assistant manager, and spa manager logic into the operating framework
- links commercial contribution to reward recognition and refresh-training signals
- demonstrates that the portfolio can be read through a decision lens, not only a reporting lens

### Sample executive scan
Illustrative management reads from the portfolio include:
- **Central Malta Spa** -> efficient / balanced | develop commercial uplift
- **Gozo Spa** -> buffered growth-ready | protect operating backbone
- **Qawra / St Paul’s Bay Spa** -> tight watchlist | stabilize people risk
- **Sliema / Balluta Spa** -> buffered growth-ready | protect premium core
- **Valletta Spa** -> buffered growth-ready | protect premium core

## Commercial coaching and reward layer
The latest operating extension adds a commercial contribution layer across both therapist and non-therapist roles.

This includes:
- retail-selling score across receptionist, spa attendant, assistant manager, and spa manager roles
- therapist upsell score combining treatment upsell, retail contribution, and service-quality guardrails
- reward-recognition eligibility signals
- top-group and bottom-group therapist identification
- refresh-training and coaching triggers for commercial underperformance or operating drift

This layer is designed to support decisions about recognition, coaching, replication of strong selling behavior, and refresh-training needs without collapsing the portfolio into a pure sales-incentive model.

## Deliverable highlights
Key reviewer-facing outputs in this repository include:
- `deliverables/executive_readout/malta_operating_model_executive_readout.md` -> executive management readout summarizing outlet posture, operating signals, and portfolio implications
- `deliverables/executive_readout/malta_operating_model_headline_strip.md` -> fast portfolio headline layer for quick executive scanning
- `deliverables/operating_model/outlet_staffing_management_view.csv` -> outlet-level staffing interpretation layer tied to management posture
- `deliverables/operating_model/outlet_manager_action_queue_enriched.csv` -> manager action queue translating operating signals into prioritised follow-up
- `data_processed/management/outlet_operating_role_framework.csv` -> operating-role framework covering receptionist, spa attendant, assistant manager, and spa manager baseline support structure
- `data_processed/management/staff_commercial_scoring_layer.csv` -> retail-selling scoring layer across non-therapist staff roles
- `data_processed/management/therapist_top_bottom_performance_layer.csv` -> therapist upsell, reward, and refresh-training decision layer
- `data_processed/management/manager_action_queue.csv` -> integrated management action queue including commercial coaching and reward-recognition logic

## What to review first
For a fast portfolio review:
1. `README.md`
2. `docs/executive_portfolio_summary.md`
3. `deliverables/executive_readout/malta_operating_model_executive_readout.md`
4. `data_processed/management/outlet_operating_role_framework.csv`
5. `data_processed/management/staff_commercial_scoring_layer.csv`
6. `data_processed/management/therapist_top_bottom_performance_layer.csv`
7. `data_processed/management/manager_action_queue.csv`

## How to read this repo

### For recruiters and non-technical reviewers
1. Start with `README.md`
2. Review `docs/executive_portfolio_summary.md`
3. Review `deliverables/executive_readout/`
4. Review `data_processed/management/` for management-facing outputs
5. Use `docs/` for additional project framing

### For technical reviewers
1. Review `scripts/build/`, `scripts/patch/`, and `scripts/qa/`
2. Trace outputs through `data_processed/`
3. Inspect validation and governance logic around pricing, roster realism, management interpretation, commercial scoring, reward logic, and coaching triggers

## Core supporting files
- `deliverables/executive_readout/malta_operating_model_executive_readout.md`
- `deliverables/executive_readout/malta_operating_model_headline_strip.md`
- `deliverables/executive_readout/malta_operating_model_quick_scan.csv`
- `deliverables/operating_model/outlet_manager_action_queue_enriched.csv`
- `deliverables/operating_model/outlet_staffing_management_view.csv`
- `deliverables/operating_model/therapist_segmentation_management_pack.xlsx`
- `deliverables/operating_model/therapist_structural_segmentation_summary.csv`
- `data_processed/management/outlet_operating_role_framework.csv`
- `data_processed/management/staff_commercial_scoring_layer.csv`
- `data_processed/management/therapist_top_bottom_performance_layer.csv`
- `data_processed/management/manager_action_queue.csv`
