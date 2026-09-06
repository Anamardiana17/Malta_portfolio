# Malta Portfolio — five-to-ten-minute reviewer guide

## 1. Business concept

Read `docs/valletta_thesis_alignment.md` for the controlled-launch Valletta flagship concept, the scope boundary and the source hierarchy. The flagship case is an academic planning model; the older multi-outlet operating files are synthetic demonstrations.

## 2. Market context

Open `assets/figures/figure_2_market_context_and_segmentation.png`, then inspect `data_processed/thesis_alignment/market_context_2025.csv`. Official NSO values are labelled **OFFICIAL**. Calculated combinations are labelled **DERIVED**. Tourism evidence is contextual and does not measure spa demand.

## 3. Operating assumptions

Open `data_processed/thesis_alignment/operating_assumptions.csv`. Focus on the 15-person team, 9 permanent therapist FTE, explicit relief hours, 365 operating days, 45 paid hours per therapist per week, 22% non-bookable allowance and 45% Year 1 paid utilisation.

## 4. Treatment costing

Use `data_processed/pricing_research/` to review recipe, material, direct-cost and price-governance logic. These detailed operating files demonstrate the calculation method; final Valletta commercial rates remain subject to quotations and executed contracts.

## 5. Financial outputs

Open `data_processed/thesis_alignment/financial_headlines.csv`. The Year 1 bridge is €1,151,000 net revenue less €302,372.24 variable operating cost and €667,000 fixed cash operating cost, producing €181,627.76 EBITDA.

## 6. Scenario and risk analysis

Read the management, conservative and severe-case rows in `data_processed/thesis_alignment/reconciliation_register.csv`. The management case is an execution target, the conservative case is the preferred investment-discussion range, and the severe case is a liquidity stress test rather than a forecast.

## 7. Sources and QA checks

Review `docs/data_sources_and_rights.md`, `docs/dataset_provenance_register.csv` and `docs/evidence_status_dictionary.md`. Run:

Controlled thesis and workbook filenames and hashes are listed in `docs/controlled_document_register.csv`; the large binary files are not duplicated in the public repository.

```bash
python scripts/qa/validate_thesis_alignment.py
```

The check validates source values, final capacity conventions, the EBITDA bridge, break-even revenue, fee definitions and the visitor arrival-capture proxy.
