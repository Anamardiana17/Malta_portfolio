from pathlib import Path
import pandas as pd

BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")

SPINE_FP = BASE / "data_processed/monthly_proxy_blocks/monthly_spine_2017_2025.csv"
AIRPORT_FP = BASE / "data_processed/monthly_proxy_blocks/proxy_airport_monthly_2017_2025.csv"
TOURISM_FP = BASE / "data_processed/monthly_proxy_blocks/proxy_tourism_monthly_2017_2025.csv"
ACCOM_FP = BASE / "data_processed/monthly_proxy_blocks/proxy_accommodation_monthly_2017_2025.csv"
LABOUR_FP = BASE / "data_processed/monthly_proxy_blocks/proxy_labour_monthly_2017_2025.csv"
CPI_FP = BASE / "data_processed/monthly_proxy_blocks/proxy_cpi_monthly_2017_2025.csv"

OUT_DIR = BASE / "data_processed/final_bundle"
OUT_FP = OUT_DIR / "malta_external_proxy_monthly_2017_2025.csv"

MONTH_MAP = {
    "jan": 1, "january": 1,
    "feb": 2, "february": 2,
    "mar": 3, "march": 3,
    "apr": 4, "april": 4,
    "may": 5,
    "jun": 6, "june": 6,
    "jul": 7, "july": 7,
    "aug": 8, "august": 8,
    "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10,
    "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}


def read_csv_safe(fp: Path, name: str) -> pd.DataFrame:
    if not fp.exists():
        raise FileNotFoundError(f"missing {name}: {fp}")
    df = pd.read_csv(fp)
    print(f"[ok] {name}: {fp}")
    print(f"     shape={df.shape}")
    print(f"     cols={df.columns.tolist()}")
    return df


def parse_month_name(val):
    if pd.isna(val):
        return pd.NA
    s = str(val).strip().lower()
    return MONTH_MAP.get(s, pd.NA)


def normalize_keys(df: pd.DataFrame, name: str) -> pd.DataFrame:
    df = df.copy()

    if "year" not in df.columns:
        raise ValueError(f"{name} missing year column")

    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")

    month_candidates = []

    if "month_num" in df.columns:
        month_candidates.append(("month_num_numeric", pd.to_numeric(df["month_num"], errors="coerce")))
    if "month" in df.columns:
        month_candidates.append(("month_numeric", pd.to_numeric(df["month"], errors="coerce")))
    if "month_label" in df.columns:
        month_candidates.append(("month_label_name", df["month_label"].map(parse_month_name)))
    if "month" in df.columns:
        month_candidates.append(("month_name", df["month"].map(parse_month_name)))

    chosen = None
    chosen_name = None

    for cname, series in month_candidates:
        valid = pd.to_numeric(series, errors="coerce").notna().sum()
        if valid == len(df):
            chosen = pd.to_numeric(series, errors="coerce").astype("Int64")
            chosen_name = cname
            break

    if chosen is None:
        temp = pd.Series([pd.NA] * len(df), index=df.index, dtype="object")
        for cname, series in month_candidates:
            series = pd.to_numeric(series, errors="coerce")
            temp = temp.where(temp.notna(), series)
        chosen = pd.to_numeric(temp, errors="coerce").astype("Int64")
        chosen_name = "combined_fallback"

    df["month"] = chosen
    print(f"[normalize] {name} month source: {chosen_name}")

    bad_rows = df[df["year"].isna() | df["month"].isna()]
    if not bad_rows.empty:
        debug_cols = [c for c in ["year", "month", "month_num", "month_label", "quarter"] if c in df.columns]
        print(f"[debug] invalid year/month rows in {name}:")
        print(bad_rows[debug_cols].to_string(index=False))
        raise ValueError(f"{name} has invalid year/month values after normalization")

    df["year"] = df["year"].astype(int)
    df["month"] = df["month"].astype(int)
    return df


def dedupe_or_fail(df: pd.DataFrame, name: str) -> pd.DataFrame:
    dup = df.duplicated(subset=["year", "month"]).sum()
    print(f"[check] duplicates {name} on (year, month): {dup}")
    if dup > 0:
        dups = df[df.duplicated(subset=["year", "month"], keep=False)].sort_values(["year", "month"])
        print(f"[debug] duplicate rows in {name}:")
        print(dups.head(20).to_string(index=False))
        raise ValueError(f"{name} has duplicate (year, month) keys")
    return df


def add_month_id(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["month_id"] = df["year"].astype(str) + "-" + df["month"].astype(str).str.zfill(2)
    return df


def prefix_metadata(df: pd.DataFrame, block_name: str) -> pd.DataFrame:
    df = df.copy()
    rename_map = {}
    for c in df.columns:
        if c in ["year", "month"]:
            continue
        if c.startswith("source_"):
            rename_map[c] = f"{c}_{block_name}"
    if rename_map:
        df = df.rename(columns=rename_map)
    return df


def main():
    spine = normalize_keys(read_csv_safe(SPINE_FP, "spine"), "spine")
    airport = normalize_keys(read_csv_safe(AIRPORT_FP, "airport"), "airport")
    tourism = normalize_keys(read_csv_safe(TOURISM_FP, "tourism"), "tourism")
    accom = normalize_keys(read_csv_safe(ACCOM_FP, "accommodation"), "accommodation")
    labour = normalize_keys(read_csv_safe(LABOUR_FP, "labour"), "labour")
    cpi = normalize_keys(read_csv_safe(CPI_FP, "cpi"), "cpi")

    spine = dedupe_or_fail(spine, "spine")
    airport = dedupe_or_fail(airport, "airport")
    tourism = dedupe_or_fail(tourism, "tourism")
    accom = dedupe_or_fail(accom, "accommodation")
    labour = dedupe_or_fail(labour, "labour")
    cpi = dedupe_or_fail(cpi, "cpi")

    merged = spine.copy()

    merge_blocks = [
        ("airport", airport),
        ("tourism", tourism),
        ("accommodation", accom),
        ("labour", labour),
        ("cpi", cpi),
    ]

    for name, df in merge_blocks:
        df2 = df.drop(columns=["month_id", "month_num"], errors="ignore")
        df2 = prefix_metadata(df2, name)
        merged = merged.merge(df2, on=["year", "month"], how="left")
        print(f"[merge] after {name}: {merged.shape}")

    merged = add_month_id(merged)

    preferred_front = ["year", "month", "month_id", "month_label", "quarter"]
    preferred_front = [c for c in preferred_front if c in merged.columns]
    rest_cols = [c for c in merged.columns if c not in preferred_front]
    merged = merged[preferred_front + rest_cols]

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    merged.to_csv(OUT_FP, index=False)

    print("\nSaved final merged dataset:")
    print(OUT_FP)
    print("shape:", merged.shape)

    print("\nNon-null summary:")
    for col in merged.columns:
        if col in ["year", "month", "month_id"]:
            continue
        print(f"{col}: {merged[col].notna().sum()}/{len(merged)}")

    print("\nTail preview:")
    print(merged.tail(12).to_string(index=False))


if __name__ == "__main__":
    main()
