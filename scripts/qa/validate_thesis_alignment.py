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
local_rows = load_rows(DATA / "valletta_local_market_context.csv")
local_by_metric = {row["metric"]: row for row in local_rows}

allowed_statuses = {
    "OFFICIAL", "DERIVED", "PLANNING ASSUMPTION", "MODEL OUTPUT",
    "SYNTHETIC DEMO", "VALIDATION REQUIRED",
}
for row in market_rows + assumption_rows + financial_rows + local_rows:
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

national_comparison = finance["visitor_related_occasions"] / market["inbound_tourists"]
close(
    national_comparison,
    finance["national_inbound_arrival_comparison_ratio"],
    0.00000001,
)

close(float(local_by_metric["valletta_tourism_intensity"]["value"]), 69.4, 0)
close(float(local_by_metric["valletta_august_tourism_intensity"]["value"]), 82.0, 0)
close(float(local_by_metric["valletta_december_tourism_intensity"]["value"]), 49.5, 0)

for row in local_rows:
    if not row["access_date"].strip() or not row["model_version"].strip():
        raise AssertionError(f"Missing local-context metadata for {row['metric']}")
    if not row["interpretation_boundary"].strip():
        raise AssertionError(f"Missing interpretation boundary for {row['metric']}")

for metric in (
    "independent_valletta_visitor_share",
    "organised_excursion_share",
    "staying_in_valletta_share",
    "cruise_day_visitor_share",
):
    if local_by_metric[metric]["temporal_relevance"] != "HISTORICAL":
        raise AssertionError(f"Historical MTA row is not controlled: {metric}")

for metric in (
    "reported_malta_tourists_visiting_valletta_floor",
    "indicative_valletta_visitor_floor_proxy",
    "indicative_local_visitor_volume_comparison_ratio",
):
    if local_by_metric[metric]["evidence_status"] != "VALIDATION REQUIRED":
        raise AssertionError(f"Unvalidated local proxy is overstated: {metric}")

for metric in ("valletta_unique_annual_visitors", "valletta_actual_capture_rate"):
    row = local_by_metric[metric]
    if row["value"].strip():
        raise AssertionError(f"Unsupported Valletta denominator/capture value populated: {metric}")
    if row["evidence_status"] != "VALIDATION REQUIRED":
        raise AssertionError(f"Unavailable local metric lacks validation control: {metric}")

for row in financial_rows:
    if row["metric"] == "visitor_arrival_capture_proxy":
        raise AssertionError("Legacy arrival-capture proxy label remains")
if "national_inbound_arrival_comparison_ratio" not in finance:
    raise AssertionError("National comparison ratio is missing")

if not 0.70 <= market["age_25_64_combined_share"] <= 0.75:
    raise AssertionError("Selected monthly combined age share falls outside the disclosed range")

if len(reconciliation_rows) < 15:
    raise AssertionError("Reconciliation register is incomplete")

figure = ROOT / "assets" / "figures" / "figure_2_market_context_and_segmentation.png"
if not figure.exists() or figure.stat().st_size < 100_000:
    raise AssertionError("Figure 2 is missing or unexpectedly small")

print("PASS: thesis alignment data reconciles to the controlled source conventions.")
