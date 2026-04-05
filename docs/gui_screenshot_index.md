# GUI Screenshot Index

## How to Read the Stage 2 Visuals
These visuals are intended as analytics evidence, not decoration.

- **Outlet health heatmap**: cross-outlet comparative signal surface
- **Business-health trend**: time-series portfolio monitoring
- **Portfolio condition mix**: portfolio composition and state distribution
- **Ranked action chart / action queue view**: prioritization logic for management intervention


This screenshot pack presents the GUI layer of `Malta_portfolio` as a governed operations control panel above validated portfolio artifacts.
The flow shown here follows the clean reviewer-facing story centered on `DEMO_BATCH_20260331_001`.

In one scan, this pack shows the full reviewer-facing chain: governed intake -> controlled processing -> analytics evidence -> prioritized action -> traceable export.

---

## 01. Data Input Panel

### 01a — Data Input: Acceptance Evidence
Shows the governed intake summary for the active demo batch, including profiling recommendation, manual review outcome, and acceptance status before processing.

### 01b — Data Input: Batch Registration
Shows how uploaded files are registered into the governed intake layer so reviewers can see batch identity, intake structure, and control-point visibility before execution.

### 01c — Data Input: Schema Profiling
Shows schema profiling and dataset matching evidence used to assess whether the uploaded batch is structurally ready for governed review.

### 01d — Data Input: Manual Acceptance Review
Shows the manual review checkpoint where the batch is accepted, held, or rejected before entering the controlled processing gate.

### 01e — Data Input: Processing History and Governance Flow
Shows the governed movement from intake review into processing history so the reviewer can follow batch status across the control flow.

---

## 02. Processing / QA Panel

### 02a — Processing / QA: Gate and Execution Evidence
Shows accepted-gate logic and execution evidence for the active demo batch.

### 02b — Processing / QA: Execution History and Artifact Readiness
Shows execution history and downstream artifact readiness so reviewers can see whether the governed run produced usable outputs.

### 02c — Processing / QA: Governance Source Policy
Shows the evidence-source policy used by the GUI so processing interpretation remains traceable and methodologically defensible.

---

## 03. KPI / Executive Dashboard Panel

### 03a — Executive Dashboard: Context and Integrity
Shows the active analytical context and integrity framing used by the dashboard, clarifying that displayed `month_id` comes from validated active artifacts rather than direct upload-batch lineage.

### 03b — Executive Dashboard: Outlet Business Health
Shows management-facing business health visibility across the active governed context, helping reviewers read signal quality and business performance direction.

---

## 04. Decision Support Panel

### 04a — Decision Support: Context and Integrity
Shows the analytical context used for decision support so action recommendations remain aligned to validated artifacts and guarded interpretation.

### 04b — Decision Support: Action Queue and Filters
Shows prioritized management actions and filtering controls that help translate KPI signals into reviewer-friendly and manager-friendly next steps.

### 04c — Decision Support: Therapist Detail and Management Snapshot
Shows a more detailed management view connecting therapist-level signals and operational interpretation to practical action visibility.

---

## 05. Export / Reviewer Pack Panel

### 05 — Export / Reviewer Pack: Traceability
Shows export and traceability visibility, helping confirm that downstream outputs are governed, inspectable, and aligned with the validated portfolio workflow.

---

## Reviewer Framing

This GUI is positioned as a governed control layer above validated `Malta_portfolio` artifacts. It does not replace the core pipeline, override portfolio methodology, or present external context or active analytical months as direct operational truth from raw upload events.

The screenshot sequence is intended to show this reviewer-facing flow:

`governed intake -> profiling -> manual review -> accepted gate -> controlled processing -> KPI visibility -> decision support -> reviewer-facing traceability`
