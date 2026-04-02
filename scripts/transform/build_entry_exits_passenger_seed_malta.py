from pathlib import Path
import pandas as pd

BASE_DIR = Path("/Users/ambakinanti/Desktop/Malta_portfolio")
OUT_DIR = BASE_DIR / "data_processed" / "entry_exits_passenger"
OUT_DIR.mkdir(parents=True, exist_ok=True)

registry = pd.DataFrame([{
    "dataset_name": "entry_exits_passenger",
    "project_scope": "Malta",
    "dataset_stage": "seed_dataset",
    "intended_use": "contextual passenger and service-access layer for daypart decision-support",
    "methodology_note": "official page-level metrics only; not direct hourly spa demand",
    "status": "usable_seed",
}])

air = pd.DataFrame([{
    "source_group": "air",
    "period_label": "2024",
    "metric_scope": "annual",
    "mia_total_passengers": 8968286,
    "italy_passenger_movements": 2048407,
    "uk_passenger_movements": 1742321,
    "source_note": "official NSO Malta air transport page metrics",
}])

sea = pd.DataFrame([{
    "source_group": "sea",
    "period_label": "2025Q4",
    "metric_scope": "quarterly",
    "total_trips": 11829,
    "total_passengers": 1839008,
    "mgarr_cirkewwa_passengers": 1526411,
    "mgarr_cirkewwa_vehicles": 529930,
    "mgarr_valletta_passengers": 312597,
    "source_note": "official NSO Malta sea transport between Malta and Gozo Q4 2025 metrics",
}])

cruise = pd.DataFrame([
    {
        "source_group": "cruise",
        "period_label": "2025Q4",
        "metric_scope": "quarterly",
        "total_cruise_passengers": 179299,
        "cruise_liner_calls": 93,
        "transit_passengers": 169152,
        "source_note": "official NSO Malta cruise passengers Q4 2025 metrics",
    },
    {
        "source_group": "cruise",
        "period_label": "2025",
        "metric_scope": "annual",
        "total_cruise_passengers": 870560,
        "cruise_liner_calls": 387,
        "transit_passengers": None,
        "source_note": "official NSO Malta cruise passengers full year 2025 metrics",
    }
])

bus = pd.DataFrame([{
    "source_group": "bus",
    "period_label": "current_context",
    "metric_scope": "service_context",
    "bus_passenger_count_available_flag": 0,
    "bus_routes_timetables_context_available_flag": 1,
    "bus_monitoring_context_available_flag": 1,
    "source_note": "bus included as service/access context, not passenger count dataset",
}])

registry.to_csv(OUT_DIR / "entry_exits_passenger_registry.csv", index=False)
air.to_csv(OUT_DIR / "entry_exits_passenger_air_seed_metrics.csv", index=False)
sea.to_csv(OUT_DIR / "entry_exits_passenger_sea_seed_metrics.csv", index=False)
cruise.to_csv(OUT_DIR / "entry_exits_passenger_cruise_seed_metrics.csv", index=False)
bus.to_csv(OUT_DIR / "entry_exits_passenger_bus_context_seed.csv", index=False)

combined = pd.concat([
    air.assign(dataset_block="air"),
    sea.assign(dataset_block="sea"),
    cruise.assign(dataset_block="cruise"),
    bus.assign(dataset_block="bus"),
], ignore_index=True, sort=False)

combined.to_csv(OUT_DIR / "entry_exits_passenger_malta_seed_combined.csv", index=False)

print("[OK] saved files:")
for fp in sorted(OUT_DIR.glob("entry_exits_passenger*.csv")):
    print(" -", fp.name)
