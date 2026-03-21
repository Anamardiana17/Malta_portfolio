import requests
from pathlib import Path
import pandas as pd
from itertools import product

out_dir = Path("output")
out_dir.mkdir(exist_ok=True)

url = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/tour_occ_nim"

params = {
    "geo": "MT",
    "sinceTimePeriod": "2017-01",
    "untilTimePeriod": "2025-12",
    "lang": "EN"
}

resp = requests.get(url, params=params, timeout=60)
resp.raise_for_status()

data = resp.json()

raw_path = out_dir / "tour_occ_nim_malta_raw.json"
raw_path.write_text(resp.text, encoding="utf-8")
print(f"Saved raw JSON: {raw_path}")

dim_ids = data["id"]
dim_sizes = data["size"]
dimensions = data["dimension"]
values = data["value"]

dim_categories = {}
for dim in dim_ids:
    cat_index = dimensions[dim]["category"]["index"]
    cat_label = dimensions[dim]["category"].get("label", {})
    ordered = sorted(cat_index.items(), key=lambda x: x[1])
    dim_categories[dim] = [
        {"code": code, "label": cat_label.get(code, code)}
        for code, _ in ordered
    ]

all_positions = [range(size) for size in dim_sizes]
rows = []

for flat_idx, combo in enumerate(product(*all_positions)):
    val = values.get(str(flat_idx))
    if val is None:
        continue

    row = {}
    for i, pos in enumerate(combo):
        dim = dim_ids[i]
        row[f"{dim}_code"] = dim_categories[dim][pos]["code"]
        row[f"{dim}_label"] = dim_categories[dim][pos]["label"]
    row["value"] = val
    rows.append(row)

df = pd.DataFrame(rows)

csv_path = out_dir / "tour_occ_nim_malta_2017_2025.csv"
xlsx_path = out_dir / "tour_occ_nim_malta_2017_2025.xlsx"

df.to_csv(csv_path, index=False, encoding="utf-8-sig")
df.to_excel(xlsx_path, index=False)

print(f"Saved CSV: {csv_path}")
print(f"Saved Excel: {xlsx_path}")
print("Shape:", df.shape)
print("Columns:", df.columns.tolist())
print(df.head(10))