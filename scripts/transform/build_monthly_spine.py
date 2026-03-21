from pathlib import Path
import pandas as pd

BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")
OUT = BASE / "data_processed" / "monthly_proxy_blocks" / "monthly_spine_2017_2025.csv"

months = pd.date_range("2017-01-01", "2025-12-01", freq="MS")

df = pd.DataFrame({"month": months})
df["year"] = df["month"].dt.year
df["month_num"] = df["month"].dt.month
df["month_label"] = df["month"].dt.strftime("%Y-%m")
df["quarter"] = df["month"].dt.quarter

OUT.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(OUT, index=False)

print(f"saved: {OUT}")
print("rows:", len(df))
print(df.head())
print(df.tail())
