import pandas as pd
from pathlib import Path

BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")
IN_FP = BASE / "data_processed/spa_research/malta_spa_treatments_raw.csv"
OUT_FP = BASE / "data_processed/spa_research/malta_spa_treatments_clean.csv"

df = pd.read_csv(IN_FP)

bad_exact = {
    "price",
    "in 3 easy steps",
    "how much does a massage cost in malta?",
    "8 spa locations",
    "two hours of free parking",
    "sorry",
    "404 page",
}

bad_contains = [
    "limited time offer",
    "select a gift voucher amount",
    "free parking",
    "complimentary",
    "delivered by qualified practitioners",
    "book your session",
    "pair your ritual",
]

df["treatment_name_norm"] = df["treatment_name"].fillna("").astype(str).str.strip().str.lower()

mask_bad = df["treatment_name_norm"].isin(bad_exact)

for pat in bad_contains:
    mask_bad = mask_bad | df["treatment_name_norm"].str.contains(pat, case=False, regex=False)

df2 = df.loc[~mask_bad].copy()

# keep rows with some evidence this is a real treatment row
mask_keep = (
    df2["duration_min"].notna()
    | df2["raw_line"].fillna("").str.contains("€", regex=False)
)

df2 = df2.loc[mask_keep].copy()

drop_cols = [c for c in ["treatment_name_norm"] if c in df2.columns]
df2 = df2.drop(columns=drop_cols)

df2.to_csv(OUT_FP, index=False)

print("saved:", OUT_FP)
print("shape before:", df.shape)
print("shape after :", df2.shape)
print(df2.head(40).to_string(index=False))
