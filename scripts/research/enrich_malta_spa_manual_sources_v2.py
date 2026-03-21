from __future__ import annotations

import re
import time
from pathlib import Path
import pandas as pd
import requests
from bs4 import BeautifulSoup
import trafilatura

BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")
INPUT = BASE / "data_processed/spa_research/malta_spa_outlet_curated_base_manual_v2.csv"
OUTPUT = BASE / "data_processed/spa_research/malta_spa_outlet_master_manual_enriched_v2.csv"
CACHE_DIR = BASE / "data_processed/spa_research/page_cache_manual_v2"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
}

BLOCKED_URL_SUBSTRINGS = [
    "/contact-us",
    "/contact",
    "/locations",
    "carisma-spa-locations-in-malta",
]

PRICE_RE = re.compile(r"(€|eur\s?)(\s?\d{1,4}(?:[.,]\d{1,2})?)", re.I)
PHONE_RE = re.compile(r"(\+\d[\d\s()./-]{6,}\d)")
EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)
HOURS_LINE_RE = re.compile(
    r"(monday|tuesday|wednesday|thursday|friday|saturday|sunday|mon[- ]?sun|daily|opening hours?|business hours?|operating hours?)",
    re.I
)

FACILITY_HINTS = [
    "sauna", "steam room", "steam", "indoor pool", "outdoor pool", "jacuzzi",
    "hammam", "gym", "fitness", "relaxation area", "relaxation room",
    "heated pool", "hydrotherapy", "thermal", "wellness area",
    "massage room", "massage rooms", "treatment room", "treatment rooms",
    "beauty salon", "salt room", "cold plunge",
]

SERVICE_HINTS = [
    "massage", "facial", "body treatment", "scrub", "wrap", "manicure",
    "pedicure", "aromatherapy", "deep tissue", "swedish", "couples massage",
    "hot stone", "reflexology", "spa package", "wellness ritual",
]

ADDRESS_HINT_WORDS = [
    "street", "st.", "st ", "road", "triq", "avenue", "bay", "coast",
    "floriana", "valletta", "sliema", "st julian", "saint julian",
    "st paul", "mellieha", "gozo", "malta", "qawra", "bugibba", "gzira",
]

def norm(x: str) -> str:
    x = "" if pd.isna(x) else str(x)
    x = x.replace("’", "'").replace("–", "-").replace("—", "-")
    x = re.sub(r"\s+", " ", x).strip(" \n\r\t,;")
    return x

def cache_path(url: str) -> Path:
    safe = re.sub(r"[^a-zA-Z0-9._-]", "_", url)[:220]
    return CACHE_DIR / f"{safe}.html"

def url_is_blocked(url: str) -> bool:
    u = norm(url).lower()
    return any(x in u for x in BLOCKED_URL_SUBSTRINGS)

def fetch_html(url: str, timeout: int = 25) -> str:
    cp = cache_path(url)
    if cp.exists():
        return cp.read_text(encoding="utf-8", errors="ignore")
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout)
        r.raise_for_status()
        html = r.text
        cp.write_text(html, encoding="utf-8")
        time.sleep(1.0)
        return html
    except Exception:
        return ""

def soupify(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "lxml") if html else BeautifulSoup("", "lxml")

def extract_text(html: str) -> str:
    if not html:
        return ""
    txt = trafilatura.extract(html, include_comments=False, include_links=False, no_fallback=False)
    if txt:
        return norm(txt)
    return norm(soupify(html).get_text(" ", strip=True))

def unique_join(values: list[str], sep: str = " | ") -> str:
    out, seen = [], set()
    for v in values:
        v = norm(v)
        if not v:
            continue
        k = v.lower()
        if k not in seen:
            out.append(v)
            seen.add(k)
    return sep.join(out)

def parse_price_values(text: str):
    vals = []
    for m in PRICE_RE.finditer(text):
        raw = m.group(2).replace(",", ".").strip()
        try:
            v = float(raw)
            if 5 <= v <= 1000:
                vals.append(v)
        except Exception:
            pass
    vals = sorted(set(vals))
    if not vals:
        return "", None, None
    return (
        ", ".join([f"€{int(v) if float(v).is_integer() else v}" for v in vals[:12]]),
        min(vals),
        max(vals),
    )

def extract_hours(text: str, soup: BeautifulSoup) -> str:
    hits = []
    for line in re.split(r"[\n\r]|(?<=[.])\s+", text):
        line = norm(line)
        if HOURS_LINE_RE.search(line) and len(line) <= 220:
            hits.append(line)
    for el in soup.find_all(["li", "p", "div", "span", "td", "th"]):
        t = norm(el.get_text(" ", strip=True))
        if HOURS_LINE_RE.search(t) and len(t) <= 220:
            hits.append(t)
    return unique_join(hits[:20])

def extract_contacts(text: str, soup: BeautifulSoup) -> str:
    hits = []
    hits.extend(PHONE_RE.findall(text))
    hits.extend(EMAIL_RE.findall(text))
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("tel:"):
            hits.append(href.replace("tel:", "").strip())
        elif href.startswith("mailto:"):
            hits.append(href.replace("mailto:", "").strip())
    return unique_join(hits[:20])

def looks_addressish(line: str) -> bool:
    t = norm(line).lower()
    digit = bool(re.search(r"\d", t))
    hint = any(w in t for w in ADDRESS_HINT_WORDS)
    postcode = bool(re.search(r"\b[a-z]{2,4}\s?\d{3,5}\b", t))
    commas = t.count(",")
    return postcode or (hint and (digit or commas >= 1))

def extract_address(text: str, soup: BeautifulSoup) -> str:
    hits = []
    for el in soup.find_all(["address", "p", "div", "span", "li"]):
        t = norm(el.get_text(" ", strip=True))
        if 8 <= len(t) <= 220 and looks_addressish(t):
            hits.append(t)
    for line in re.split(r"[\n\r]|(?<=[.])\s+", text):
        t = norm(line)
        if 8 <= len(t) <= 220 and looks_addressish(t):
            hits.append(t)
    return unique_join(hits[:10])

def extract_facilities(text: str) -> str:
    s = text.lower()
    return unique_join([f for f in FACILITY_HINTS if f in s])

def extract_services(text: str) -> str:
    s = text.lower()
    return unique_join([f for f in SERVICE_HINTS if f in s])

def enrich_row(row: pd.Series) -> dict:
    url = norm(row.get("source_url_final", ""))

    if (not url.startswith("http")) or url_is_blocked(url):
        return {
            "outlet_name_final": row["outlet_name_final"],
            "matched_source_count": 0,
            "used_url": "",
            "address_raw": "",
            "opening_hours_raw": "",
            "treatment_examples_raw": "",
            "price_eur_raw": "",
            "price_eur_min": None,
            "price_eur_max": None,
            "facilities_raw": "",
            "contact_raw": "",
        }

    html = fetch_html(url)
    text = extract_text(html)
    soup = soupify(html)

    if not text:
        return {
            "outlet_name_final": row["outlet_name_final"],
            "matched_source_count": 0,
            "used_url": url,
            "address_raw": "",
            "opening_hours_raw": "",
            "treatment_examples_raw": "",
            "price_eur_raw": "",
            "price_eur_min": None,
            "price_eur_max": None,
            "facilities_raw": "",
            "contact_raw": "",
        }

    price_raw, price_min, price_max = parse_price_values(text)

    return {
        "outlet_name_final": row["outlet_name_final"],
        "matched_source_count": 1,
        "used_url": url,
        "address_raw": extract_address(text, soup),
        "opening_hours_raw": extract_hours(text, soup),
        "treatment_examples_raw": extract_services(text),
        "price_eur_raw": price_raw,
        "price_eur_min": price_min,
        "price_eur_max": price_max,
        "facilities_raw": extract_facilities(text),
        "contact_raw": extract_contacts(text, soup),
    }

def main():
    df = pd.read_csv(INPUT).copy()
    recs = []

    for i, (_, row) in enumerate(df.iterrows(), start=1):
        rec = enrich_row(row)
        recs.append(rec)
        print(f"[{i}/{len(df)}] {rec['outlet_name_final']} | matched={rec['matched_source_count']} | url={rec['used_url']}")

    out = pd.DataFrame(recs).sort_values("outlet_name_final").reset_index(drop=True)
    out.to_csv(OUTPUT, index=False)

    print("saved:", OUTPUT)
    print("shape:", out.shape)
    print(out.to_string(index=False))

if __name__ == "__main__":
    main()
