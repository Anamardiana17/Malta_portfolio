from __future__ import annotations

from pathlib import Path
import pandas as pd


BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")

SRC_INDEX = BASE / "data_raw" / "ecb" / "malta_hicp_restaurants_hotels_index_2017_2025.csv"
SRC_YOY = BASE / "data_raw" / "ecb" / "malta_hicp_restaurants_hotels_yoy_2017_2025.csv"
OUT = BASE / "data_processed" / "monthly_proxy_blocks" / "proxy_cpi_monthly_2017_2025.csv"


def clean_colname(col: str) -> str:
    return str(col).replace("\ufeff", "").strip()


def build_monthly_spine(start: str = "2017-01", end: str = "2025-12") -> pd.DataFrame:
    periods = pd.period_range(start=start, end=end, freq="M")
    spine = pd.DataFrame({"month_id": periods.astype(str)})
    spine["year"] = periods.year.astype(int)
    spine["month"] = periods.month.astype(int)
    return spine[["year", "month", "month_id"]]


def normalize_monthly_frame(df: pd.DataFrame, value_name: str) -> pd.DataFrame:
    df = df.copy()
    df.columns = [clean_colname(c) for c in df.columns]

    rename_map = {}
    for c in df.columns:
        lc = c.lower()
        if lc in {"date", "time_period", "time period", "period", "month", "month_id"}:
            rename_map[c] = "month_id"
        elif lc in {"value", "obs_value", "obs value", "index", "yoy", "rate"}:
            rename_map[c] = value_name

    if rename_map:
        df = df.rename(columns=rename_map)

    if "month_id" not in df.columns:
        df = df.rename(columns={df.columns[0]: "month_id"})

    if value_name not in df.columns:
        candidate_value_cols = [c for c in df.columns if c != "month_id"]
        if not candidate_value_cols:
            raise ValueError(f"Could not infer value column for {value_name}. Columns: {df.columns.tolist()}")
        df = df.rename(columns={candidate_value_cols[0]: value_name})

    df["month_id"] = df["month_id"].astype(str).str.strip()
    df = df.loc[df["month_id"].str.match(r"^\d{4}-\d{2}$", na=False)].copy()

    df["year"] = pd.to_numeric(df["month_id"].str[:4], errors="coerce")
    df["month"] = pd.to_numeric(df["month_id"].str[-2:], errors="coerce")
    df[value_name] = pd.to_numeric(df[value_name], errors="coerce")

    df = df.loc[
        df["year"].notna() &
        df["month"].notna() &
        (df["year"] >= 2017) &
        (df["year"] <= 2025)
    ].copy()

    df["year"] = df["year"].astype(int)
    df["month"] = df["month"].astype(int)

    return df[["year", "month", "month_id", value_name]].sort_values(["year", "month"]).reset_index(drop=True)


def main() -> None:
    if not SRC_INDEX.exists():
        raise FileNotFoundError(f"Index source file not found: {SRC_INDEX}")
    if not SRC_YOY.exists():
        raise FileNotFoundError(f"YoY source file not found: {SRC_YOY}")

    raw_index = pd.read_csv(SRC_INDEX)
    raw_yoy = pd.read_csv(SRC_YOY)

    print("index columns:", raw_index.columns.tolist())
    print("yoy columns:", raw_yoy.columns.tolist())

    idx = normalize_monthly_frame(raw_index, "hicp_restaurants_hotels_index")
    yoy = normalize_monthly_frame(raw_yoy, "hicp_restaurants_hotels_yoy")

    print("idx rows:", len(idx), "non_null:", idx["hicp_restaurants_hotels_index"].notna().sum())
    print(idx.head(12).to_string(index=False))
    print("yoy rows:", len(yoy), "non_null:", yoy["hicp_restaurants_hotels_yoy"].notna().sum())
    print(yoy.head(12).to_string(index=False))

    spine = build_monthly_spine("2017-01", "2025-12")

    out = (
        spine
        .merge(idx[["year", "month", "hicp_restaurants_hotels_index"]], on=["year", "month"], how="left")
        .merge(yoy[["year", "month", "hicp_restaurants_hotels_yoy"]], on=["year", "month"], how="left")
    )

    out["source_dataset_index"] = SRC_INDEX.name
    out["source_dataset_yoy"] = SRC_YOY.name
    out["source_scope"] = "ECB / HICP Restaurants and Hotels Malta monthly proxy"

    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT, index=False)

    print("saved:", OUT)
    print("rows:", len(out))
    print("non_null index:", out["hicp_restaurants_hotels_index"].notna().sum())
    print("non_null yoy:", out["hicp_restaurants_hotels_yoy"].notna().sum())
    print(out.head(20).to_string(index=False))


if __name__ == "__main__":
    main()
