from __future__ import annotations

import re
from pathlib import Path

import pandas as pd


BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")

SRC = BASE / "data_raw" / "eurostat" / "estat_ttr00017.tsv"
OUT = BASE / "data_processed" / "monthly_proxy_blocks" / "proxy_airport_monthly_2017_2025.csv"


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

    parts.columns = ["freq", "unit", "tra_meas", "tra_cov", "pas_type", "airport_code"]
    df = pd.concat([parts, df.drop(columns=["dim_blob"])], axis=1)
    df.columns = [clean_colname(c) for c in df.columns]

    month_pattern = re.compile(r"^\d{4}-\d{2}$")
    time_cols = sorted([c for c in df.columns if month_pattern.match(c)])

    print("source:", SRC)
    print("rows raw:", len(df))
    print("time_cols count:", len(time_cols))
    print("time_cols sample:", time_cols[:12])

    airport = df.loc[
        (df["freq"] == "M")
        & (df["unit"] == "PAS")
        & (df["tra_meas"] == "TOT")
        & (df["tra_cov"] == "INTL")
        & (df["pas_type"] == "PAS_CRD")
        & (df["airport_code"] == "MT_LMML")
    ].copy()

    print("candidate rows:", len(airport))

    if airport.empty:
        raise ValueError("Filtered Malta airport slice is empty.")
    if len(airport) != 1:
        raise ValueError(f"Expected exactly 1 Malta airport row, got {len(airport)}")

    row = airport.iloc[0]

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
                    "airport_passengers_international": extract_numeric(value_raw),
                    "source_value_raw": value_raw,
                }
            )

    long_df = pd.DataFrame(records).sort_values(["year", "month"]).reset_index(drop=True)

    print("long_df rows:", len(long_df))
    print("long_df non_null:", long_df["airport_passengers_international"].notna().sum())
    print(long_df.to_string(index=False))

    spine = build_monthly_spine("2017-01", "2025-12")
    print("spine rows:", len(spine))
    print("spine head:")
    print(spine.head(12).to_string(index=False))

    out = spine.merge(long_df, on=["year", "month"], how="left")

    out["source_dataset"] = "Eurostat estat_ttr00017"
    out["source_airport_code"] = "MT_LMML"
    out["source_scope"] = "Passengers carried, total, international"

    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT, index=False)

    print("saved:", OUT)
    print("rows:", len(out))
    print("non_null airport_passengers_international:", out["airport_passengers_international"].notna().sum())
    print(
        out.loc[out["airport_passengers_international"].notna(),
                ["year", "month", "airport_passengers_international"]].to_string(index=False)
    )


if __name__ == "__main__":
    main()
