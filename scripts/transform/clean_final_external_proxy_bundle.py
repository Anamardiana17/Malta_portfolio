from pathlib import Path
import pandas as pd

BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")
IN_FP = BASE / "data_processed/final_bundle/malta_external_proxy_monthly_2017_2025.csv"
OUT_FP = BASE / "data_processed/final_bundle/malta_external_proxy_monthly_2017_2025_clean.csv"

df = pd.read_csv(IN_FP)

# replace Eurostat placeholder
for c in ["source_value_raw_airport", "source_value_raw_tourism"]:
    if c in df.columns:
        df[c] = df[c].replace(":", pd.NA)

# drop redundant column
if "month_num" in df.columns:
    df = df.drop(columns=["month_num"])
    
# optional: reorder a bit cleaner
front = [
    "year",
    "month",
    "month_id",
    "month_label",
    "quarter",
    "airport_passengers_international",
    "tourism_demand_air_passengers_international",
    "accommodation_nights_spent",
    "unemployment_rate_percent",
    "hicp_restaurants_hotels_index",
    "hicp_restaurants_hotels_yoy",
]
front = [c for c in front if c in df.columns]
rest = [c for c in df.columns if c not in front]
df = df[front + rest]

OUT_FP.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(OUT_FP, index=False)

print("saved:", OUT_FP)
print("shape:", df.shape)
print("\nnon-null summary:")
for c in df.columns:
    print(f"{c}: {df[c].notna().sum()}/{len(df)}")
