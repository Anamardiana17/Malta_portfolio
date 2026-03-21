from pathlib import Path
import re
import pandas as pd

BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")
TXT_DIR = BASE / "data_processed/mia_text"
OUT_FP = BASE / "data_processed/final_bundle/mia_report_context_2017_2024.csv"

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

BAD_MARKETS = {
    "strongest", "growth", "strongest growth", "top", "market",
    "highest", "share", "top airline", "highest slf"
}

MARKET_CANDIDATES = [
    "Italy",
    "United Kingdom",
    "UK",
    "Germany",
    "France",
    "Poland",
    "Spain",
    "Ireland",
    "Belgium",
    "Netherlands",
]

def infer_year_month_from_stem(stem: str):
    s = stem.lower()
    year = None
    m_year = re.search(r"(20\d{2})", s)
    if m_year:
        year = int(m_year.group(1))

    month = None
    for k, v in MONTH_MAP.items():
        if re.search(rf"(^|[_\-\s]){k}([_\-\s]|$)", s):
            month = v
            break
    return year, month

def extract_top_market(text: str):
    text1 = " ".join(text.split())

    # 1) Hard fallback first for noisy PDF layouts like:
    # PASSENGERS ITALY 822,810 passengers
    for candidate in MARKET_CANDIDATES:
        if re.search(rf"PASSENGERS\s+{re.escape(candidate)}\b", text1, flags=re.I):
            return candidate

    # 2) Another noisy-layout fallback:
    # ITALY ... MARKET SHARE
    for candidate in MARKET_CANDIDATES:
        if re.search(rf"\b{re.escape(candidate)}\b.*?MARKET SHARE", text1, flags=re.I):
            return candidate

    # 3) Normal patterns
    patterns = [
        r"Top Market\s+([A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+)?)",
        r"([A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+)?)\s+was the top market",
        r"([A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+)?)\s+remained the top market",
    ]
    for pat in patterns:
        m = re.search(pat, text1, flags=re.I)
        if m:
            candidate = m.group(1).strip()
            if candidate.lower() not in BAD_MARKETS:
                return candidate

    return None

def extract_yoy_growth(text: str):
    text1 = " ".join(text.split())
    patterns = [
        r"up\s+([0-9]+(?:\.[0-9]+)?)\s*%",
        r"increase(?:d)?\s+by\s+([0-9]+(?:\.[0-9]+)?)\s*%",
        r"grew\s+by\s+([0-9]+(?:\.[0-9]+)?)\s*%",
        r"growth(?: of)?\s+([0-9]+(?:\.[0-9]+)?)\s*%",
    ]
    for pat in patterns:
        m = re.search(pat, text1, flags=re.I)
        if m:
            try:
                return float(m.group(1))
            except:
                pass
    return None

def extract_passenger_total(text: str):
    text1 = " ".join(text.split())
    patterns = [
        r"([0-9][0-9,]{4,})\s+PASSENGERS",
        r"([0-9][0-9,]{4,})\s+passengers",
        r"passenger traffic (?:reached|amounted to) ([0-9][0-9,]{4,})",
        r"welcomed\s+([0-9][0-9,]{4,})\s+passengers",
    ]
    for pat in patterns:
        m = re.search(pat, text1, flags=re.I)
        if m:
            try:
                return int(m.group(1).replace(",", ""))
            except:
                pass
    return None

def make_snippet(text: str):
    text1 = " ".join(text.split())
    patterns = [
        r".{0,80}PASSENGERS.{0,200}",
        r".{0,80}Top Market.{0,200}",
        r".{0,80}passenger traffic.{0,200}",
        r".{0,80}growth.{0,200}",
    ]
    for pat in patterns:
        m = re.search(pat, text1, flags=re.I)
        if m:
            return m.group(0)
    return text1[:260]

rows = []
for fp in sorted(TXT_DIR.glob("*.txt")):
    txt = fp.read_text(encoding="utf-8", errors="ignore")
    year, month = infer_year_month_from_stem(fp.stem)

    rows.append({
        "year": year,
        "month": month,
        "month_id": f"{year}-{str(month).zfill(2)}" if year and month else None,
        "source_file": fp.name,
        "top_market": extract_top_market(txt),
        "reported_passenger_total": extract_passenger_total(txt),
        "reported_yoy_growth_percent": extract_yoy_growth(txt),
        "narrative_snippet": make_snippet(txt),
    })

df = pd.DataFrame(rows).sort_values(["year", "month", "source_file"], na_position="last")
df.to_csv(OUT_FP, index=False)

print("saved:", OUT_FP)
print(df.to_string(index=False))
