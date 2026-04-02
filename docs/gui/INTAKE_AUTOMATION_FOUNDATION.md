# Intake Automation Foundation

## Objective
Provide a governed intake automation layer for raw dataset uploads before any processing pipeline is allowed to run.

## Current Scope
- schema registry
- column alias registry
- schema loader
- supported dataset visibility in GUI

## Not Yet Implemented
- file profiling
- automatic dataset type detection
- automatic column standardization
- validation decision engine
- accepted/rejected transition logic
- processing trigger

## Design Principle
The GUI should assist structured intake, not guess unsupported business logic.
