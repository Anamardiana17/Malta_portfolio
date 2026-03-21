from __future__ import annotations

import re
import time
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup
import trafilatura

BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")
INPUT = BASE / "data_processed/spa_research/malta_spa_outlet_source_map_v1.csv"
OUTPUT = BASE / "data_processed/spa_research/malta_spa_outlet_master_enriched_v6.csv"
CACHE_DIR = BASE / "data_processed/spa_research/page_cache_v6"
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

GENERIC_PAGE_PATTERNS = [
    r"\bcontact us\b",
    r"\bour locations\b",
    r"\bspa locations\b",
    r"\ball locations\b",
    r"\bfind a location\b",
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

STOPWORDS = {"spa", "resort", "hotel", "wellness", "malta", "the", "and", "by"}

def norm(x: str) -> str:
    x = "" if pd.isna(x) else str(x)
    x = x.replace("’", "'").replace("–", "-").replace("—", "-")
    x = re.sub(r"\s+", " ", x).strip(" \n\r\t,;")
    return x

def slug(x: str) -> str:
    x = norm(x).lower()
    x = re.sub(r"[^a-z0-9\s]", " ", x)
    x = re.sub(r"\s+", " ", x).strip()
    return x

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

def brand_tokens(name: str) -> list[str]:
    return [t for t in slug(name).split() if t not in STOPWORDS and len(t) >= 3]

def cache_path(url: str) -> Path:
    safe = re.sub(r"[^a-zA-Z0-9._-]", "_", url)[:220]
    return CACHE_DIR / f"{safe}.html"

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

def url_is_blocked(url: str) -> bool:
    u = url.lower()
    return any(x in u for x in BLOCKED_URL_SUBSTRINGS)

def text_is_generic(text: str) -> bool:
    s = slug(text[:3000])
    return any(re.search(p, s) for p in GENERIC_PAGE_PATTERNS)

def page_matches_outlet(outlet_name: str, url: str, text: str) -> bool:
    if url_is_blocked(url):
        return False
    if text_is_generic(text):
        return False

    out_slug = slug(outlet_name)
    toks = brand_tokens(outlet_name)
    combined = f"{urlparse(url).netloc.lower()} {slug(url)} {slug(text[:10000])}"

    if out_slug and out_slug in combined:
        return True

    hits = sum(1 for t in toks if t in combined)
    if len(toks) >= 2 and hits >= 1:
        return True
    if len(toks) == 1 and hits >= 1:
        return True
    return False

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

def collect_urls(row: pd.Series):
    urls = []
    for c in row.index:
        if c.startswith("source_url_"):
            v = norm(row[c])
            if v.startswith("http"):
                urls.append(v)
    return urls

def enrich_row(row: pd.Series) -> dict:
    outlet_name = norm(row.get("outlet_name_final", ""))
    urls = collect_urls(row)

    matched_urls = []
    texts = []
    soups = []

    for url in urls:
        html = fetch_html(url)
        if not html:
            continue
        txt = extract_text(html)
        soup = soupify(html)

        if page_matches_outlet(outlet_name, url, txt):
            matched_urls.append(url)
            texts.append(txt)
            soups.append(soup)

    if not texts:
        return {
            "outlet_name_final": outlet_name,
            "matched_source_count": 0,
            "enrich_source_urls": "",
            "address_raw": "",
            "opening_hours_raw": "",
            "treatment_examples_raw": "",
            "price_eur_raw": "",
            "price_eur_min": None,
            "price_eur_max": None,
            "facilities_raw": "",
            "contact_raw": "",
        }

    full_text = "\n".join(texts)
    price_raw, price_min, price_max = parse_price_values(full_text)

    return {
        "outlet_name_final": outlet_name,
        "matched_source_count": len(matched_urls),
        "enrich_source_urls": unique_join(matched_urls),
        "address_raw": unique_join([extract_address(t, s) for t, s in zip(texts, soups)]),
        "opening_hours_raw": unique_join([extract_hours(t, s) for t, s in zip(texts, soups)]),
        "treatment_examples_raw": extract_services(full_text),
        "price_eur_raw": price_raw,
        "price_eur_min": price_min,
        "price_eur_max": price_max,
        "facilities_raw": extract_facilities(full_text),
        "contact_raw": unique_join([extract_contacts(t, s) for t, s in zip(texts, soups)]),
    }

def main():
    df = pd.read_csv(INPUT).copy()
    recs = []

    for i, (_, row) in enumerate(df.iterrows(), start=1):
        rec = enrich_row(row)
        recs.append(rec)
        print(f"[{i}/{len(df)}] {rec['outlet_name_final']} | matched={rec['matched_source_count']}")

    out = pd.DataFrame(recs).sort_values("outlet_name_final").reset_index(drop=True)
    out.to_csv(OUTPUT, index=False)

    print("saved:", OUTPUT)
    print("shape:", out.shape)
    print(out.to_string(index=False))

if __name__ == "__main__":
    main()
