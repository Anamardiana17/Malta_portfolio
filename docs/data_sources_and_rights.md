# Malta_portfolio — Data Sources, Provenance, and Rights Register

_Last updated: 2026-04-06_

## Purpose
This document centralizes the provenance, access route, attribution guidance, and reuse/copyright notes for datasets and external reference materials used in `Malta_portfolio`.

Its goals are to:
- strengthen academic defensibility
- make external-data provenance reviewer-visible
- reduce copyright and reuse risk
- distinguish clearly between demo/generated data, official statistics, and website reference material

---

## 1) Internal Demo / Generated Operational Data

These datasets support the management and KPI workflow demonstration and are **not live company operating data**.

Typical examples in this repo:
- POS-style demo operational inputs
- staffing / roster demo inputs
- derived management-facing outputs generated from the demo pipeline

### Rights / usage treatment
- must be labeled clearly as synthetic, generated, or demo
- must not be described as live business records
- must not imply an external copyright owner unless real external data was used

### Preferred citation
`Source: Author-generated demo data for Malta_portfolio.`

---

## 2) Official Statistics and Public Statistical Releases

These are the strongest external sources in the repo because they are official statistical outputs and are the best basis for contextual guardrails.

### A. National Statistics Office (NSO) Malta — Inbound Tourism
**Used for**
- inbound tourism context
- tourist volume
- nights spent
- expenditure context
- visitor mix / broad market pressure framing

**Repo evidence**
- `data_raw/nso_inbound_tourism/`
- e.g. `nso_inbound_tourism_dec_2024_page.html`

**Preferred citation**
`Source: National Statistics Office, Malta — Inbound Tourism.`

**Notes**
- use NSO as the named source in charts, tables, and README references
- when possible, include the exact release title and release date

---

### B. National Statistics Office (NSO) Malta — Collective Accommodation Establishments
**Used for**
- accommodation activity
- guests
- nights spent
- occupancy / utilization context
- accommodation-side seasonality proxy

**Repo evidence**
- `data_raw/nso_accommodation/`
- e.g. `nso_collective_accommodation_q4_2024_page.html`
- `data_raw/nso_accommodation_reports/`

**Preferred citation**
`Source: National Statistics Office, Malta — Collective Accommodation Establishments.`

**Notes**
- useful for contextual demand pressure and accommodation regime framing
- must remain explicitly labeled as contextual support, not spa demand truth

---

### C. National Statistics Office (NSO) Malta — Passenger Traffic at Malta International Airport
**Used for**
- air transport / passenger movement context
- tourism-access and mobility context
- high-level arrival pressure proxy

**Preferred citation**
`Source: National Statistics Office, Malta — Passenger Traffic at Malta International Airport (compiled from administrative information provided by Malta International Airport plc).`

**Notes**
- this is the safest public-facing citation line when using airport traffic context
- if MIA annual reports are also used directly, they should be listed separately as supporting references

---

## 3) Malta International Airport (MIA) Annual Reports / Summary PDFs

**Used for**
- airport traffic context
- yearly passenger movement summaries
- traffic development context

**Repo evidence**
- `data_raw/mia/`
- multiple annual summary PDFs for 2017–2025

### Important rights caution
MIA materials should be treated more carefully than NSO and Eurostat sources.

### Safer public-repo treatment
- keep bibliographic references and URLs
- keep a file manifest
- avoid re-hosting copyrighted PDFs in the public repo unless licence/permission is clear
- prefer citing the official source page and storing metadata in the repo

### Preferred citation
`Source: Malta International Airport plc, Annual Summary Report [year].`

---

## 4) Eurostat Datasets

**Used for**
- supporting macro / tourism / price / labour context
- cross-checking Malta context at the European statistical system level

**Repo evidence**
- `data_raw/eurostat/`
- e.g.
  - `estat_ttr00011.tsv`
  - `estat_ttr00012.tsv`
  - `estat_ttr00016.tsv`
  - `estat_ttr00017.tsv`
  - `prc_hicp_manr.json`
  - `prc_hicp_midx.json`
  - `tour_occ_nim.json`
  - `une_rt_m.json`

### Preferred citation
`Source: Eurostat, dataset code [CODE], extracted for Malta on [DATE].`

### Notes
- attribution should always be present
- use dataset code when available
- keep extraction/access date where possible

---

## 5) Spa Market Reference URLs / Competitive Landscaping References

**Used for**
- market scanning
- outlet/location discovery
- treatment/pricing reference checks
- competitor and venue mapping

**Repo evidence**
- `data_raw/spa_research/malta_spa_seed_urls.csv`
- `data_raw/spa_research/malta_spa_seed_urls_filtered.csv`

### Rights / method caution
These should be treated as **reference URLs**, not as reusable owned datasets.

### Safer public-repo treatment
- keep URL lists / seed manifests
- keep short manual notes and access dates
- avoid bulk scraped content
- avoid redistributing reviews, photos, or large copied text blocks

### Preferred citation
`Source: [website / operator], page title, accessed [DATE].`

---

## Recommended Repo Structure

### Best central location
Create:
- `docs/data_sources_and_rights.md`
- `docs/dataset_provenance_register.csv`

### Good secondary structure
Add lightweight local README files near raw-source folders:
- `data_raw/README.md`
- `data_raw/eurostat/README.md`
- `data_raw/mia/README.md`
- `data_raw/nso_accommodation/README.md`
- `data_raw/nso_inbound_tourism/README.md`
- `data_raw/spa_research/README.md`

---

## Strong Recommendation on Public Repo Risk

### Keep in public repo
- provenance register
- source manifests
- access dates
- dataset codes
- official source titles
- transformed analytical outputs created by the project
- clearly marked demo/generated internal data

### Reconsider keeping in public repo
- full third-party PDFs when the licence is unclear or restrictive
- copied website pages from commercial sites
- bulk scraped marketplace/review content
- any data containing customer-like personal information

### Especially important
For the `data_raw/mia/` folder, the safer academic and copyright-aware move is:
1. create a manifest / citation register
2. replace public PDF hosting with official links where possible
3. keep archival copies outside the public GitHub repo if needed for personal workflow

---

## Minimal Citation Standard for the Repo

- **NSO**: `Source: National Statistics Office, Malta — [release or section title].`
- **Eurostat**: `Source: Eurostat, dataset [code], extracted for Malta on [date].`
- **MIA**: `Source: Malta International Airport plc, Annual Summary Report [year].`
- **Demo data**: `Source: Author-generated demo data for Malta_portfolio.`
- **Website references**: `Source: [website / operator], accessed [date].`

---

## Recommended README Note

A short root-README note is recommended:

> External context sources used in this project are documented in `docs/data_sources_and_rights.md` and `docs/dataset_provenance_register.csv`. Official statistics are cited to their original publishers. Demo operational datasets are generated for portfolio demonstration and are not live company data. External contextual proxies are used as decision-support context only and not as direct spa demand truth.
