from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import urlparse
import pandas as pd

BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")

WHITELIST = BASE / "data_processed/spa_research/malta_spa_outlet_whitelist_v1.csv"
FILES = [
    BASE / "data_processed/spa_research/malta_spa_outlet_master.csv",
    BASE / "data_processed/spa_research/malta_spa_outlet_master_clean.csv",
    BASE / "data_processed/spa_research/malta_spa_outlet_master_clean_v2.csv",
    BASE / "data_processed/spa_research/malta_spa_outlets_raw.csv",
]
OUT = BASE / "data_processed/spa_research/malta_spa_outlet_source_map_v1.csv"

BLOCKED_URL_SUBSTRINGS = [
    "/contact-us",
    "/contact",
    "/locations",
    "carisma-spa-locations-in-malta",
]

STOPWORDS = {"spa", "resort", "hotel", "wellness", "malta", "the", "and", "by"}

def norm(x: str) -> str:
    x = "" if pd.isna(x) else str(x)
    x = x.replace("’", "'").replace("–", "-").replace("—", "-")
    x = re.sub(r"\s+", " ", x).strip(" ,;")
    return x

def slug(x: str) -> str:
    x = norm(x).lower()
    x = re.sub(r"[^a-z0-9\s]", " ", x)
    x = re.sub(r"\s+", " ", x).strip()
    return x

def brand_tokens(name: str):
    return [t for t in slug(name).split() if t not in STOPWORDS and len(t) >= 3]

def url_ok(url: str) -> bool:
    u = norm(url)
    if not u.startswith("http"):
        return False
    lu = u.lower()
    if any(b in lu for b in BLOCKED_URL_SUBSTRINGS):
        return False
    return True

def candidate_texts(row: pd.Series) -> str:
    vals = []
    for c in row.index:
        lc = c.lower()
        if any(k in lc for k in ["outlet", "name", "title", "location", "address", "text"]):
            vals.append(norm(row[c]))
    return " | ".join([v for v in vals if v])

def row_matches_outlet(row: pd.Series, outlet_name: str) -> bool:
    txt = slug(candidate_texts(row))
    toks = brand_tokens(outlet_name)
    outlet_slug = slug(outlet_name)

    if outlet_slug and outlet_slug in txt:
        return True

    hits = sum(1 for t in toks if t in txt)
    if len(toks) >= 2 and hits >= 2:
        return True
    if len(toks) == 1 and hits >= 1:
        return True
    return False

def collect_urls_from_row(row: pd.Series):
    urls = []
    for c in row.index:
        if "url" in c.lower():
            v = norm(row[c])
            if url_ok(v):
                urls.append(v)
    return urls

def main():
    wl = pd.read_csv(WHITELIST)
    outlets = [norm(x) for x in wl["outlet_name_final"].dropna().tolist()]

    frames = []
    for fp in FILES:
        if fp.exists():
            df = pd.read_csv(fp)
            df["__source_file"] = fp.name
            frames.append(df)

    if not frames:
        raise FileNotFoundError("No source files found")

    big = pd.concat(frames, ignore_index=True, sort=False)

    records = []
    for outlet in outlets:
        urls = []
        src_files = []

        sub = big[big.apply(lambda r: row_matches_outlet(r, outlet), axis=1)].copy()

        for _, row in sub.iterrows():
            row_urls = collect_urls_from_row(row)
            if row_urls:
                urls.extend(row_urls)
                src_files.append(norm(row.get("__source_file", "")))

        # unique preserve order
        out_urls, seen = [], set()
        for u in urls:
            if u not in seen:
                out_urls.append(u)
                seen.add(u)

        # rank: prefer non-homepage-ish first, then homepage
        def rank_url(u: str):
            p = urlparse(u)
            path = p.path.strip("/").lower()
            is_home = path == ""
            depth = len([x for x in path.split("/") if x])
            return (is_home, -depth, u)

        out_urls = sorted(out_urls, key=rank_url)
        out_urls = out_urls[:8]

        rec = {"outlet_name_final": outlet, "matched_rows": len(sub), "source_files_hit": " | ".join(sorted(set([s for s in src_files if s])))}
        for i, u in enumerate(out_urls, start=1):
            rec[f"source_url_{i}"] = u
        records.append(rec)

    out = pd.DataFrame(records).sort_values("outlet_name_final").reset_index(drop=True)
    out.to_csv(OUT, index=False)

    print("saved:", OUT)
    print("shape:", out.shape)
    show_cols = ["outlet_name_final", "matched_rows"] + [c for c in out.columns if c.startswith("source_url_")]
    print(out[show_cols].to_string(index=False))

if __name__ == "__main__":
    main()
