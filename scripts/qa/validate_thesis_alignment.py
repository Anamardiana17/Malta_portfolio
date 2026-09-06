from __future__ import annotations

import csv
import math
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data_processed" / "thesis_alignment"


def load_metric(path: Path, key: str = "metric") -> dict[str, float]:
    with path.open(newline="", encoding="utf-8") as handle:
        return {row[key]: float(row["value"]) for row in csv.DictReader(handle)}


def load_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def close(actual: float, expected: float, tolerance: float = 0.01) -> None:
    if not math.isclose(actual, expected, rel_tol=0, abs_tol=tolerance):
        raise AssertionError(f"Expected {expected}, received {actual}")


market = load_metric(DATA / "market_context_2025.csv")
finance = load_metric(DATA / "financial_headlines.csv")
assumptions = load_metric(DATA / "operating_assumptions.csv", "driver")
market_rows = load_rows(DATA / "market_context_2025.csv")
assumption_rows = load_rows(DATA / "operating_assumptions.csv")
financial_rows = load_rows(DATA / "financial_headlines.csv")
reconciliation_rows = load_rows(DATA / "reconciliation_register.csv")

allowed_statuses = {
    "OFFICIAL", "DERIVED", "PLANNING ASSUMPTION", "MODEL OUTPUT",
    "SYNTHETIC DEMO", "VALIDATION REQUIRED",
}
for row in market_rows + assumption_rows + financial_rows:
    if row["evidence_status"] not in allowed_statuses:
        raise AssertionError(f"Unsupported evidence status: {row['evidence_status']}")

for row in market_rows:
    for field in ("release_date", "access_date", "source_url", "model_version", "next_review"):
        if not row[field].strip():
            raise AssertionError(f"Missing {field} for market metric {row['metric']}")

for row in assumption_rows:
    for field in ("data_as_of", "model_version", "owner", "next_review"):
        if not row[field].strip():
            raise AssertionError(f"Missing {field} for assumption {row['driver']}")

for row in financial_rows:
    for field in ("data_as_of", "model_version", "owner", "next_review"):
        if not row[field].strip():
            raise AssertionError(f"Missing {field} for financial metric {row['metric']}")

close(market["inbound_tourists"], 4_022_310, 0)
close(market["total_tourist_expenditure"], 3_904_400_000, 0)
close(market["expenditure_per_capita"], 971, 0)

close(assumptions["permanent_therapist_capacity"], 9, 0)
close(assumptions["operating_days"], 365, 0)
close(assumptions["paid_hours_per_therapist"], 45, 0)
close(assumptions["payment_processing_fee"], 0.022, 0)
close(assumptions["booking_channel_fee"], 0.03, 0)

close(
    finance["total_net_revenue"]
    - finance["variable_operating_cost"]
    - finance["fixed_cash_operating_cost"],
    finance["EBITDA"],
)

contribution_margin = 1 - finance["variable_operating_cost"] / finance["total_net_revenue"]
close(
    finance["fixed_cash_operating_cost"] / contribution_margin,
    finance["break_even_total_net_revenue"],
)

visitor_proxy = finance["visitor_related_occasions"] / market["inbound_tourists"]
close(visitor_proxy, finance["visitor_arrival_capture_proxy"], 0.00000001)

if not 0.70 <= market["age_25_64_combined_share"] <= 0.75:
    raise AssertionError("Selected monthly combined age share falls outside the disclosed range")

if len(reconciliation_rows) < 15:
    raise AssertionError("Reconciliation register is incomplete")

figure = ROOT / "assets" / "figures" / "figure_2_market_context_and_segmentation.png"
if not figure.exists() or figure.stat().st_size < 100_000:
    raise AssertionError("Figure 2 is missing or unexpectedly small")

print("PASS: thesis alignment data reconciles to the controlled source conventions.")
