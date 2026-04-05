# GUI Control Panel

## Overview
This GUI sits on top of the validated Malta_portfolio workflow as a governed control layer. It helps reviewers, managers, and non-technical stakeholders move from raw batch intake through controlled processing, KPI visibility, decision support, and downloadable reviewer-ready outputs.

The GUI presents governed intake evidence, controlled execution evidence, and analytical dashboard context as distinct but connected layers. It also makes clear that analytical `month_id` selection comes from active validated artifacts rather than direct upload-batch provenance.

## What It Supports
- governed upload, profiling, and review
- accepted-gate controlled processing
- KPI and business health visibility
- management-facing decision support
- reviewer-facing evidence and downloadable outputs

## User Flow
upload -> profile -> review -> accepted gate -> controlled execution -> KPI dashboard -> decision support -> export / reviewer pack

## Why This Is More Than a Dashboard
This GUI is not a dashboard-first layer. It is a governed control surface built on top of validated Malta_portfolio artifacts.

What makes it analytically stronger than a display-only interface:
- governed processing and review happen before visual presentation
- context-integrity notes constrain interpretation of analytical month selection
- visuals summarize multi-layer outputs rather than raw charting alone
- action queues prioritize management intervention rather than simply showing data
- external context remains decision-support input, not operational truth

## Guardrails
- does not replace the core pipeline
- does not override portfolio methodology
- does not introduce unsupported intra-day staffing inference
- does not treat external proxies as exact operational demand truth
- does not function as a live production deployment

## Management Value
The GUI helps managers and reviewers quickly understand business health, check governed processing evidence, identify operational priorities, and access validated outputs without having to read code or inspect raw pipeline files.

## Analytical Context Integrity Guardrail

The dashboard and decision-support views include an active context integrity / coverage summary for the selected analytical `month_id` across the active governed artifacts used by each panel.

This summary is a reviewer-facing guardrail. It confirms whether the selected analytical month is present across active processed artifacts, and does not claim that the month came directly from the latest uploaded batch.

## GUI Screenshot Pack

This screenshot pack shows the Malta GUI as a **governed operations control layer** on top of validated portfolio artifacts.

In under 30 seconds, a reviewer can see the full control story:

**upload → profiling → manual acceptance → controlled processing → KPI visibility → decision support → reviewer-ready export**

It shows that the portfolio is more than analysis output: it is a management-facing operating layer that helps users:
- understand business health quickly
- review governed processing evidence
- translate KPI signals into actions
- inspect recruiter-friendly, reviewer-ready outputs

### Positioning
- internal operational data remains the primary decision anchor
- external context is used as decision-support guardrails, not exact demand truth
- the GUI does not replace the core pipeline
- the GUI does not weaken methodological controls

For the full screenshot-by-screenshot list, see `docs/gui_screenshot_index.md`.
