# GUI Control Panel

## Overview
This GUI sits on top of the validated Malta_portfolio workflow as a governed control layer. It helps reviewers, managers, and non-technical stakeholders work from raw batch intake through controlled processing, KPI visibility, decision support, and downloadable reviewer-ready outputs.

It is meant to make the portfolio easier to review and use in practice by showing business health signals, processing evidence, and action-oriented outputs in one place.

## What It Supports
- governed upload, profiling, and review
- accepted-gate controlled processing
- KPI and business health visibility
- management-facing decision support
- reviewer-facing evidence and downloadable outputs

## User Flow
upload -> profile -> review -> accepted gate -> controlled execution -> KPI dashboard -> decision support -> export / reviewer pack

## Guardrails
- does not replace the core pipeline
- does not override portfolio methodology
- does not introduce unsupported intra-day staffing inference
- does not treat external proxies as exact operational demand truth
- does not function as a live production deployment

## Management Value
The GUI helps managers and reviewers quickly understand business health, check governed processing evidence, identify operational priorities, and access validated outputs without having to read code or inspect raw pipeline files.
