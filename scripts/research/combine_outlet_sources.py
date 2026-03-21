import pandas as pd
from pathlib import Path
from pandas.errors import EmptyDataError

BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")
A_FP = BASE / "data_processed/spa_research/malta_spa_outlets_raw.csv"
B_FP = BASE / "data_processed/spa_research/fresha_malta_listing_outlets.csv"
OUT_FP = BASE / "data_processed/spa_research/malta_spa_outlets_combined.csv"

def safe_read_csv(fp: Path) -> pd.DataFrame:
    if not fp.exists() or fp.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(fp)
    except EmptyDataError:
        return pd.DataFrame()

a = safe_read_csv(A_FP)
b = safe_read_csv(B_FP)

print("A shape:", a.shape)
print("B shape:", b.shape)

common_cols = sorted(set(a.columns).union(set(b.columns)))
if not common_cols:
    raise SystemExit("Both source files are empty; nothing to combine.")

for df in [a, b]:
    for c in common_cols:
        if c not in df.columns:
            df[c] = None

out = pd.concat([a[common_cols], b[common_cols]], ignore_index=True)

if "outlet_name" not in out.columns:
    raise SystemExit("Combined data has no outlet_name column.")

out["outlet_name_norm"] = out["outlet_name"].fillna("").astype(str).str.strip().str.lower()
out = out[out["outlet_name_norm"] != ""].copy()

bad_names = {"404 page", "sorry", "tripadvisor.com", "spa treatments", "spa locations"}
out = out[~out["outlet_name_norm"].isin(bad_names)].copy()

if "rating" not in out.columns:
    out["rating"] = None
if "review_count" not in out.columns:
    out["review_count"] = None

out["rank_proxy"] = (
    pd.to_numeric(out["rating"], errors="coerce").fillna(0) * 10
    + pd.to_numeric(out["review_count"], errors="coerce").fillna(0).clip(upper=10000) ** 0.5
)

out = out.sort_values(["outlet_name_norm", "rank_proxy"], ascending=[True, False])
out = out.drop_duplicates(subset=["outlet_name_norm"], keep="first").copy()

out = out.drop(columns=["outlet_name_norm", "rank_proxy"])
out.to_csv(OUT_FP, index=False)

print("saved:", OUT_FP)
print("shape:", out.shape)
print(out.head(50).to_string(index=False))
