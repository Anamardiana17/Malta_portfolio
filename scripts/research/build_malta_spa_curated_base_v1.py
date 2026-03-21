from __future__ import annotations

from pathlib import Path
import pandas as pd
import re

BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")
CLEAN_V2 = BASE / "data_processed/spa_research/malta_spa_outlet_master_clean_v2.csv"
WHITELIST = BASE / "data_processed/spa_research/malta_spa_outlet_whitelist_v1.csv"
OUT = BASE / "data_processed/spa_research/malta_spa_outlet_curated_base_v1.csv"

def norm(x: str) -> str:
    x = "" if pd.isna(x) else str(x)
    x = x.replace("’", "'").replace("–", "-").replace("—", "-")
    x = re.sub(r"\s+", " ", x).strip()
    return x

def key(x: str) -> str:
    return norm(x).lower()

def main():
    df = pd.read_csv(CLEAN_V2).copy()
    wl = pd.read_csv(WHITELIST).copy()

    wl["wl_key"] = wl["outlet_name_final"].map(key)
    df["name_key"] = df["outlet_name_final"].map(key)

    out = df[df["name_key"].isin(set(wl["wl_key"]))].copy()

    # if duplicates exist, keep first non-empty source
    if "source_url" in out.columns:
        out["source_url_nonempty"] = out["source_url"].fillna("").astype(str).str.strip().ne("")
        out = out.sort_values(["outlet_name_final", "source_url_nonempty"], ascending=[True, False])
        out = out.drop_duplicates(subset=["name_key"], keep="first")

    out = out.drop(columns=[c for c in ["name_key", "source_url_nonempty"] if c in out.columns])
    out = out.sort_values("outlet_name_final").reset_index(drop=True)

    out.to_csv(OUT, index=False)
    print("saved:", OUT)
    print("shape:", out.shape)
    print(out[["outlet_name_final"]].to_string(index=False))

if __name__ == "__main__":
    main()
