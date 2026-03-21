import pandas as pd
from pathlib import Path

BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")
IN_FP = BASE / "data_raw/spa_research/malta_spa_seed_urls.csv"
OUT_FP = BASE / "data_raw/spa_research/malta_spa_seed_urls_filtered.csv"

df = pd.read_csv(IN_FP)

bad_patterns = [
    "tripadvisor.com",
    "/hilton-malta",
    "/malta-marriott-resort-spa",
    "/doubletree-by-hilton-malta",
    "/radisson-blu-resort-and-spa-malta-golden-sands",
    "corinthia.com/malta/spa",
]

mask_bad = df["url"].fillna("").str.contains("|".join(bad_patterns), case=False, regex=True)

df_good = df.loc[~mask_bad].copy()

df_good.to_csv(OUT_FP, index=False)

print("saved:", OUT_FP)
print("original:", df.shape)
print("filtered:", df_good.shape)
print(df_good.to_string(index=False))
