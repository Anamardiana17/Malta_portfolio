from __future__ import annotations

from pathlib import Path
import pandas as pd
import re

BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")
CURATED = BASE / "data_processed/spa_research/malta_spa_outlet_curated_base_v1.csv"
MANUAL = BASE / "data_processed/spa_research/malta_spa_outlet_source_map_manual_v2.csv"
OUT = BASE / "data_processed/spa_research/malta_spa_outlet_curated_base_manual_v2.csv"

def norm(x: str) -> str:
    x = "" if pd.isna(x) else str(x)
    x = x.replace("’", "'").replace("–", "-").replace("—", "-")
    x = re.sub(r"\s+", " ", x).strip()
    return x

def key(x: str) -> str:
    return norm(x).lower()

def main():
    curated = pd.read_csv(CURATED).copy()
    manual = pd.read_csv(MANUAL).copy()

    curated["k"] = curated["outlet_name_final"].map(key)
    manual["k"] = manual["outlet_name_final"].map(key)

    out = curated.merge(
        manual[["k", "official_url", "notes"]],
        on="k",
        how="left"
    )

    if "source_url" not in out.columns:
        out["source_url"] = ""

    out["source_url_manual"] = out["official_url"].fillna("").astype(str).str.strip()
    out["source_url_final"] = out["source_url_manual"]
    out.loc[out["source_url_final"] == "", "source_url_final"] = out["source_url"].fillna("").astype(str).str.strip()

    out = out.drop(columns=["k"])
    out.to_csv(OUT, index=False)

    print("saved:", OUT)
    print("shape:", out.shape)
    cols = [c for c in ["outlet_name_final", "source_url", "source_url_manual", "source_url_final", "notes"] if c in out.columns]
    print(out[cols].to_string(index=False))

if __name__ == "__main__":
    main()
