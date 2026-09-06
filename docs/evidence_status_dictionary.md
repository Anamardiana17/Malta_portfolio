# Evidence status dictionary

| Status | Meaning | Permitted use |
|---|---|---|
| `OFFICIAL` | Published by an identified official authority | Cite with release title, date and URL |
| `DERIVED` | Transparently calculated from identified source values | Show the formula or component rows |
| `PLANNING ASSUMPTION` | Management or academic input used to test the concept | Do not present as an operating actual |
| `MODEL OUTPUT` | Formula-driven result produced by the thesis-aligned financial model | Present with scenario, period and source sheet |
| `SYNTHETIC DEMO` | Author-generated operating data used to demonstrate workflow | Restrict to portfolio demonstration claims |
| `VALIDATION REQUIRED` | Input requiring a current quotation, contract or professional review | Do not use for an unconditional launch decision |

Every thesis-alignment dataset must include `evidence_status`. Where a planning assumption also requires external confirmation, `validation_status` is set to `VALIDATION REQUIRED`.
