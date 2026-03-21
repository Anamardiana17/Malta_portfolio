from __future__ import annotations

import re
from pathlib import Path

import pandas as pd


BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")

SRC = BASE / "data_raw" / "eurostat" / "estat_ttr00016.tsv"
OUT = BASE / "data_processed" / "monthly_proxy_blocks" / "proxy_tourism_monthly_2017_2025.csv"


def clean_colname(col: str) -> str:
    return str(col).replace("\ufeff", "").strip()


def extract_numeric(cell: object) -> float | None:
    if pd.isna(cell):
        return None
    s = str(cell).strip()
    if s in {"", ":"}:
        return None
    m = re.search(r"-?\d[\d,]*\.?\d*", s)
    if not m:
        return None
    try:
        return float(m.group(0).replace(",", ""))
    except ValueError:
        return None


def build_monthly_spine(start: str = "2017-01", end: str = "2025-12") -> pd.DataFrame:
    periods = pd.period_range(start=start, end=end, freq="M")
    spine = pd.DataFrame({"month_id": periods.astype(str)})
    spine["year"] = periods.year.astype(int)
    spine["month"] = periods.month.astype(int)
    return spine[["year", "month", "month_id"]]


def main() -> None:
    if not SRC.exists():
        raise FileNotFoundError(f"Source file not found: {SRC}")

    df = pd.read_csv(SRC, sep="\t", dtype=str, low_memory=False)
    df.columns = [clean_colname(c) for c in df.columns]

    first_col = df.columns[0]
    df = df.rename(columns={first_col: "dim_blob"})

    parts = df["dim_blob"].str.split(",", expand=True)
    if parts.shape[1] != 6:
        raise ValueError(
            f"Expected 6 dimensions in dim_blob, got {parts.shape[1]}. "
            f"Sample value: {df['dim_blob'].dropna().iloc[0]}"
        )

    parts.columns = ["freq", "unit", "schedule", "tra_cov", "tra_meas", "geo"]
    df = pd.concat([parts, df.drop(columns=["dim_blob"])], axis=1)
    df.columns = [clean_colname(c) for c in df.columns]

    month_pattern = re.compile(r"^\d{4}-\d{2}$")
    time_cols = sorted([c for c in df.columns if month_pattern.match(c)])

    print("source:", SRC)
    print("rows raw:", len(df))
    print("time_cols count:", len(time_cols))
    print("time_cols sample:", time_cols[:12])
    print("geo sample:", sorted(df["geo"].dropna().unique().tolist())[:20])

    tourism = df.loc[
        (df["freq"] == "M")
        & (df["unit"] == "PAS")
        & (df["schedule"] == "TOT")
        & (df["tra_cov"] == "INTL")
        & (df["tra_meas"] == "PAS_CRD")
        & (df["geo"] == "MT")
    ].copy()

    print("candidate rows:", len(tourism))

    if tourism.empty:
        raise ValueError("Filtered Malta tourism proxy slice is empty.")
    if len(tourism) != 1:
        raise ValueError(f"Expected exactly 1 Malta row, got {len(tourism)}")

    row = tourism.iloc[0]

    records = []
    for col in time_cols:
        year = int(col[:4])
        month = int(col[5:7])

        if 2017 <= year <= 2025:
            value_raw = row[col]
            records.append(
                {
                    "year": year,
                    "month": month,
                    "tourism_demand_air_passengers_international": extract_numeric(value_raw),
                    "source_value_raw": value_raw,
                }
            )

    long_df = pd.DataFrame(records).sort_values(["year", "month"]).reset_index(drop=True)

    print("long_df rows:", len(long_df))
    print("long_df non_null:", long_df["tourism_demand_air_passengers_international"].notna().sum())
    print(long_df.to_string(index=False))

    spine = build_monthly_spine("2017-01", "2025-12")

    out = spine.merge(long_df, on=["year", "month"], how="left")

    out["source_dataset"] = "Eurostat estat_ttr00016"
    out["source_geo"] = "MT"
    out["source_scope"] = "Monthly international air passengers carried, country-level Malta proxy"

    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT, index=False)

    print("saved:", OUT)
    print("rows:", len(out))
    print("non_null tourism_demand_air_passengers_international:",
          out["tourism_demand_air_passengers_international"].notna().sum())
    print(
        out.loc[out["tourism_demand_air_passengers_international"].notna(),
                ["year", "month", "tourism_demand_air_passengers_international"]].to_string(index=False)
    )


if __name__ == "__main__":
    main()
