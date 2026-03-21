import pandas as pd
from pathlib import Path

BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")
RAW_A = BASE / "data_processed/spa_research/malta_spa_outlets_raw.csv"
RAW_B = BASE / "data_processed/spa_research/malta_spa_breadth_outlets.csv"
OUT_FP = BASE / "data_processed/spa_research/malta_spa_outlet_master.csv"

def safe_read(fp):
    if fp.exists() and fp.stat().st_size > 0:
        return pd.read_csv(fp)
    return pd.DataFrame()

a = safe_read(RAW_A)
b = safe_read(RAW_B)

if not a.empty:
    a = a.rename(columns={"source_type": "source_name"})
    if "source_name" not in a.columns:
        a["source_name"] = "official_raw"

if not b.empty and "source_type" not in b.columns:
    b["source_type"] = "breadth"

cols = sorted(set(a.columns).union(set(b.columns)))
for df in [a, b]:
    for c in cols:
        if c not in df.columns:
            df[c] = None

df = pd.concat([a[cols], b[cols]], ignore_index=True)

df["outlet_name_norm"] = df["outlet_name"].fillna("").astype(str).str.strip().str.lower()
df = df[df["outlet_name_norm"] != ""].copy()

bad_contains = [
    "404 page", "we’re sorry", "we're sorry", "sorry", "tripadvisor.com",
    "best spa treatments near me in malta", "spa treatments", "spa locations",
    "discover the art of wellness, rooted in malta"
]
for pat in bad_contains:
    df = df[~df["outlet_name_norm"].str.contains(pat, case=False, regex=False)].copy()

df["rating_num"] = pd.to_numeric(df.get("rating"), errors="coerce")
df["review_count_num"] = pd.to_numeric(df.get("review_count"), errors="coerce")

df["priority"] = (
    df["rating_num"].fillna(0) * 20 +
    df["review_count_num"].fillna(0).clip(upper=10000) ** 0.5
)

df = df.sort_values(["outlet_name_norm", "priority"], ascending=[True, False])
df = df.drop_duplicates(subset=["outlet_name_norm"], keep="first").copy()

drop_cols = [c for c in ["outlet_name_norm", "rating_num", "review_count_num", "priority"] if c in df.columns]
df = df.drop(columns=drop_cols)

df.to_csv(OUT_FP, index=False)
print("saved:", OUT_FP)
print("shape:", df.shape)
print("unique outlets:", df["outlet_name"].nunique())
print(df.head(100).to_string(index=False))
