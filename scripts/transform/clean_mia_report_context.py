from pathlib import Path
import pandas as pd

BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")
IN_FP = BASE / "data_processed/final_bundle/mia_report_context_2017_2024.csv"
OUT_FP = BASE / "data_processed/final_bundle/mia_report_context_2017_2024_clean.csv"

df = pd.read_csv(IN_FP)

keep = [
    "year",
    "month",
    "month_id",
    "source_file",
    "top_market",
    "reported_passenger_total",
]
df = df[keep]

df.to_csv(OUT_FP, index=False)

print("saved:", OUT_FP)
print(df.to_string(index=False))
