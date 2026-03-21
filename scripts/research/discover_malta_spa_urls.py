from duckduckgo_search import DDGS
import pandas as pd

queries = [
    "site:fresha.com Malta spa",
    "site:fresha.com Malta massage spa",
    "site:tripadvisor.com Malta spas wellness centers",
    "site:carismaspa.com Malta spa locations",
    "site:myoka.com Malta spa locations",
    "site:phoeniciamalta.com Malta spa treatments",
    "site:inaspa.com.mt Malta spa packages",
    "site:nataraya.com.mt Malta spa",
    "site:damareresort.mt Malta spa",
]

rows = []
seen = set()

with DDGS() as ddgs:
    for q in queries:
        for r in ddgs.text(q, max_results=20):
            url = r.get("href") or r.get("url")
            title = r.get("title")
            body = r.get("body")
            if not url:
                continue
            if url in seen:
                continue
            seen.add(url)
            rows.append({
                "query": q,
                "title": title,
                "url": url,
                "snippet": body,
            })

df = pd.DataFrame(rows)

if not df.empty:
    mask = (
        df["title"].fillna("").str.contains("malta|sliema|gzira|ġzira|st julian|mellieha|valletta|bugibba|gozo|qawra", case=False, regex=True)
        | df["snippet"].fillna("").str.contains("malta|sliema|gzira|ġzira|st julian|mellieha|valletta|bugibba|gozo|qawra", case=False, regex=True)
        | df["url"].fillna("").str.contains("malta|.mt|fresha|tripadvisor|myoka|carisma|phoenicia|nataraya|damare", case=False, regex=True)
    )
    df = df.loc[mask].drop_duplicates(subset=["url"]).reset_index(drop=True)

out_fp = "/Users/ambakinanti/Desktop/Malta_portfolio/data_raw/spa_research/malta_spa_seed_urls.csv"
df.to_csv(out_fp, index=False)

print("saved:", out_fp)
print("rows:", len(df))
if not df.empty:
    print(df.head(30).to_string(index=False))
