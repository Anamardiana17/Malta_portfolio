from __future__ import annotations

import json
from pathlib import Path
import pandas as pd


BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")
SRC = BASE / "data_raw" / "eurostat" / "une_rt_m.json"
OUT = BASE / "data_processed" / "monthly_proxy_blocks" / "proxy_labour_monthly_2017_2025.csv"


def build_monthly_spine(start: str = "2017-01", end: str = "2025-12") -> pd.DataFrame:
    periods = pd.period_range(start=start, end=end, freq="M")
    return pd.DataFrame({
        "year": periods.year.astype(int),
        "month": periods.month.astype(int),
        "month_id": periods.astype(str),
    })


def dataset_to_long(obj: dict) -> pd.DataFrame:
    ids = obj["id"]
    sizes = obj["size"]
    dim = obj["dimension"]
    values = obj.get("value", {})
    status = obj.get("status", {})

    cats = []
    for dim_id in ids:
        idx_to_cat = dim[dim_id]["category"]["index"]
        cat_by_pos = {pos: cat for cat, pos in idx_to_cat.items()}
        cats.append([cat_by_pos[i] for i in range(len(cat_by_pos))])

    rows = []
    total = 1
    for s in sizes:
        total *= s

    for flat_idx in range(total):
        rem = flat_idx
        coords = []
        for s in reversed(sizes):
            coords.append(rem % s)
            rem //= s
        coords = list(reversed(coords))

        row = {ids[i]: cats[i][coords[i]] for i in range(len(ids))}
        key = str(flat_idx)
        row["value"] = values.get(key)
        row["status"] = status.get(key)
        rows.append(row)

    return pd.DataFrame(rows)


def main() -> None:
    obj = json.loads(SRC.read_text(encoding="utf-8"))
    df = dataset_to_long(obj)

    print("columns:", df.columns.tolist())
    print(df.head(5).to_string(index=False))

    if "geo" not in df.columns or "time" not in df.columns:
        raise ValueError(f"Expected geo and time columns. Found: {df.columns.tolist()}")

    mt = df.loc[df["geo"] == "MT"].copy()
    mt["month_id"] = mt["time"].astype(str)
    mt = mt.loc[mt["month_id"].str.match(r"^\d{4}-\d{2}$", na=False)].copy()

    candidate = mt.copy()
    for col, preferred in [
        ("sex", ["T"]),
        ("age", ["Y15-74", "TOTAL"]),
        ("unit", ["PC_ACT"]),
    ]:
        if col in candidate.columns:
            sub = candidate.loc[candidate[col].isin(preferred)].copy()
            if not sub.empty:
                candidate = sub

    candidate["year"] = candidate["month_id"].str[:4].astype(int)
    candidate["month"] = candidate["month_id"].str[-2:].astype(int)
    candidate["value"] = pd.to_numeric(candidate["value"], errors="coerce")
    candidate = candidate.loc[(candidate["year"] >= 2017) & (candidate["year"] <= 2025)].copy()

    agg = candidate.groupby(["year", "month"], as_index=False).agg(
        unemployment_rate_percent=("value", "mean")
    )

    spine = build_monthly_spine("2017-01", "2025-12")
    out = spine.merge(agg, on=["year", "month"], how="left")

    out["source_dataset"] = "une_rt_m"
    out["source_geo"] = "MT"
    out["source_scope"] = "Eurostat monthly unemployment rate proxy, Malta"

    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT, index=False)

    print("saved:", OUT)
    print("rows:", len(out))
    print("non_null unemployment_rate_percent:", out["unemployment_rate_percent"].notna().sum())
    print(out.loc[out["unemployment_rate_percent"].notna(), ["year", "month", "unemployment_rate_percent"]].head(20).to_string(index=False))


if __name__ == "__main__":
    main()
