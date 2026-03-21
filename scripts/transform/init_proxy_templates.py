from pathlib import Path
import pandas as pd

BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")
SPINE = BASE / "data_processed" / "monthly_proxy_blocks" / "monthly_spine_2017_2025.csv"
OUTDIR = BASE / "data_processed" / "monthly_proxy_blocks"

spine = pd.read_csv(SPINE)

templates = {
    "proxy_airport_monthly_2017_2025.csv": [
        "month",
        "airport_passengers_total",
        "airport_passengers_international",
        "airport_source",
        "airport_series_note",
    ],
    "proxy_tourism_monthly_2017_2025.csv": [
        "month",
        "tourist_arrivals_total",
        "tourist_nights_total",
        "avg_length_of_stay_nights",
        "tourism_source",
        "tourism_series_note",
    ],
    "proxy_accommodation_monthly_2017_2025.csv": [
        "month",
        "accom_guests_total",
        "accom_nights_total",
        "hotel_occupancy_rate_percent",
        "bed_places_or_capacity",
        "accommodation_source",
        "accommodation_series_note",
    ],
    "proxy_cpi_monthly_2017_2025.csv": [
        "month",
        "cpi_index",
        "hicp_index",
        "cpi_yoy_percent",
        "cpi_source",
        "cpi_series_note",
    ],
    "proxy_labour_monthly_2017_2025.csv": [
        "month",
        "unemployment_rate_percent",
        "labour_force_participation_rate_percent",
        "labour_source",
        "labour_series_note",
    ],
}

for fname, cols in templates.items():
    df = spine[["month"]].copy()
    for c in cols:
        if c != "month":
            df[c] = pd.NA
    df = df[cols]
    out = OUTDIR / fname
    df.to_csv(out, index=False)
    print("saved:", out)

print("done")
