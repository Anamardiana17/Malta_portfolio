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
INPUT = BASE / "data_processed/spa_research/malta_spa_outlet_master_clean_v2.csv"
OUTPUT = BASE / "data_processed/spa_research/malta_spa_outlet_master_enriched_v3.csv"
CACHE_DIR = BASE / "data_processed/spa_research/page_cache_v3"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
}

PRICE_RE = re.compile(r"(€|eur\s?)(\s?\d{1,4}(?:[.,]\d{1,2})?)", re.I)
ROOM_COUNT_RE = re.compile(r"(\d{1,4})\s+(treatment rooms?|massage rooms?|guest rooms?|bedrooms?|suites?)", re.I)
PHONE_RE = re.compile(r"(\+\d[\d\s()./-]{6,}\d)")
EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)
HOURS_LINE_RE = re.compile(
    r"(monday|tuesday|wednesday|thursday|friday|saturday|sunday|mon[- ]?sun|daily|opening hours?|business hours?|operating hours?)",
    re.I
)

FACILITY_HINTS = [
    "sauna", "steam room", "steam", "indoor pool", "outdoor pool", "jacuzzi",
    "hammam", "gym", "fitness", "relaxation area", "relaxation room",
    "heated pool", "hydrotherapy", "thermal", "wellness area", "massage room",
    "massage rooms", "treatment room", "treatment rooms", "beauty salon",
    "salt room", "cold plunge",
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

BRAND_STOPWORDS = {
    "spa", "resort", "hotel", "wellness", "malta", "the", "and", "by"
}

def norm_text(x: str) -> str:
    x = "" if pd.isna(x) else str(x)
    x = x.replace("’", "'").replace("–", "-").replace("—", "-")
    x = re.sub(r"\s+", " ", x).strip(" \n\r\t,;")
    return x

def slug(x: str) -> str:
    x = norm_text(x).lower()
    x = re.sub(r"[^a-z0-9\s]", " ", x)
    x = re.sub(r"\s+", " ", x).strip()
    return x

def brand_tokens(name: str) -> list[str]:
    toks = [t for t in slug(name).split() if t not in BRAND_STOPWORDS and len(t) >= 3]
    return toks

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

def extract_text(html: str) -> str:
    if not html:
        return ""
    txt = trafilatura.extract(html, include_comments=False, include_links=False, no_fallback=False)
    if txt:
        return norm_text(txt)
    soup = BeautifulSoup(html, "lxml")
    return norm_text(soup.get_text(" ", strip=True))

def soupify(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "lxml") if html else BeautifulSoup("", "lxml")

def unique_join(values: list[str], sep: str = " | ") -> str:
    out, seen = [], set()
    for v in values:
        v = norm_text(v)
        if not v:
            continue
        k = v.lower()
        if k not in seen:
            out.append(v)
            seen.add(k)
    return sep.join(out)

def page_matches_outlet(outlet_name: str, url: str, text: str) -> bool:
    outlet_slug = slug(outlet_name)
    toks = brand_tokens(outlet_name)
    domain = urlparse(url).netloc.lower()
    combined = f"{domain} {slug(url)} {slug(text[:6000])}"

    if outlet_slug and outlet_slug in combined:
        return True

    hit_count = sum(1 for t in toks if t in combined)
    if len(toks) >= 2 and hit_count >= 2:
        return True
    if len(toks) == 1 and hit_count >= 1:
        return True

    return False

def collect_source_urls(row: pd.Series) -> list[str]:
    urls = []
    for col in row.index:
        if "url" in col.lower():
            val = norm_text(row[col])
            if val.startswith("http"):
                urls.append(val)
    out, seen = [], set()
    for u in urls:
        if u not in seen:
            out.append(u)
            seen.add(u)
    return out

def extract_prices(text: str):
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
    sample = ", ".join([f"€{int(v) if float(v).is_integer() else v}" for v in vals[:12]])
    return sample, min(vals), max(vals)

def extract_hours(text: str, soup: BeautifulSoup) -> str:
    hits = []
    for line in re.split(r"[\n\r]|(?<=[.])\s+", text):
        line = norm_text(line)
        if HOURS_LINE_RE.search(line) and len(line) <= 220:
            hits.append(line)
    for el in soup.find_all(["li", "p", "div", "span", "td", "th"]):
        t = norm_text(el.get_text(" ", strip=True))
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
    return unique_join(hits[:15])

def looks_addressish(line: str) -> bool:
    t = norm_text(line).lower()
    digit = bool(re.search(r"\d", t))
    hint = any(w in t for w in ADDRESS_HINT_WORDS)
    postcode = bool(re.search(r"\b[a-z]{2,4}\s?\d{3,5}\b", t))
    commas = t.count(",")
    return postcode or (hint and (digit or commas >= 1))

def extract_address(text: str, soup: BeautifulSoup) -> str:
    hits = []
    for el in soup.find_all(["address", "p", "div", "span", "li"]):
        t = norm_text(el.get_text(" ", strip=True))
        if 8 <= len(t) <= 220 and looks_addressish(t):
            hits.append(t)
    for line in re.split(r"[\n\r]|(?<=[.])\s+", text):
        t = norm_text(line)
        if 8 <= len(t) <= 220 and looks_addressish(t):
            hits.append(t)
    return unique_join(hits[:10])

def extract_room_counts(text: str):
    treatment_room_count = None
    hotel_room_count = None
    notes = []
    for m in ROOM_COUNT_RE.finditer(text):
        n = int(m.group(1))
        label = m.group(2).lower()
        if "treatment" in label or "massage" in label:
            if 1 <= n <= 100:
                treatment_room_count = n if treatment_room_count is None else max(treatment_room_count, n)
                notes.append(f"{n} {label}")
        elif "guest room" in label or "bedroom" in label or "suite" in label:
            if 5 <= n <= 5000:
                hotel_room_count = n if hotel_room_count is None else max(hotel_room_count, n)
                notes.append(f"{n} {label}")
    return treatment_room_count, hotel_room_count, unique_join(notes)

def extract_facilities(text: str) -> str:
    t = text.lower()
    return unique_join([f for f in FACILITY_HINTS if f in t])

def extract_services(text: str) -> str:
    t = text.lower()
    return unique_join([s for s in SERVICE_HINTS if s in t])

def enrich_row(row: pd.Series) -> dict:
    outlet_name = norm_text(row.get("outlet_name_final", ""))
    urls = collect_source_urls(row)

    matched_texts = []
    matched_soups = []
    matched_urls = []

    for url in urls[:6]:
        html = fetch_html(url)
        if not html:
            continue
        txt = extract_text(html)
        soup = soupify(html)

        if page_matches_outlet(outlet_name, url, txt):
            matched_texts.append(txt)
            matched_soups.append(soup)
            matched_urls.append(url)

    full_text = "\n".join([t for t in matched_texts if t])

    if not full_text:
        return {
            "outlet_name_final": outlet_name,
            "enrich_source_urls": "",
            "opening_hours_raw": "",
            "address_raw": "",
            "contact_raw": "",
            "treatment_examples_raw": "",
            "price_eur_raw": "",
            "price_eur_min": None,
            "price_eur_max": None,
            "treatment_room_count": None,
            "hotel_room_count": None,
            "facilities_raw": "",
            "capacity_note_raw": "",
            "matched_source_count": 0,
        }

    hours = [extract_hours(t, s) for t, s in zip(matched_texts, matched_soups)]
    addr = [extract_address(t, s) for t, s in zip(matched_texts, matched_soups)]
    contacts = [extract_contacts(t, s) for t, s in zip(matched_texts, matched_soups)]

    price_raw, price_min, price_max = extract_prices(full_text)
    treatment_room_count, hotel_room_count, room_note = extract_room_counts(full_text)
    facilities_raw = extract_facilities(full_text)
    treatment_examples_raw = extract_services(full_text)

    cap = []
    if room_note:
        cap.append(room_note)
    if treatment_room_count is not None:
        cap.append(f"treatment_room_count={treatment_room_count}")
    if hotel_room_count is not None:
        cap.append(f"hotel_room_count={hotel_room_count}")

    return {
        "outlet_name_final": outlet_name,
        "enrich_source_urls": unique_join(matched_urls),
        "opening_hours_raw": unique_join(hours),
        "address_raw": unique_join(addr),
        "contact_raw": unique_join(contacts),
        "treatment_examples_raw": treatment_examples_raw,
        "price_eur_raw": price_raw,
        "price_eur_min": price_min,
        "price_eur_max": price_max,
        "treatment_room_count": treatment_room_count,
        "hotel_room_count": hotel_room_count,
        "facilities_raw": facilities_raw,
        "capacity_note_raw": unique_join(cap),
        "matched_source_count": len(matched_urls),
    }

def main():
    if not INPUT.exists():
        raise FileNotFoundError(f"Missing input: {INPUT}")

    df = pd.read_csv(INPUT)
    records = []

    for i, (_, row) in enumerate(df.iterrows(), start=1):
        rec = enrich_row(row)
        records.append(rec)
        print(f"[{i}/{len(df)}] {rec['outlet_name_final']} | matched_sources={rec['matched_source_count']}")

    enr = pd.DataFrame(records)
    out = df.merge(enr, on="outlet_name_final", how="left")

    preferred = [
        "outlet_name_final",
        "address_raw",
        "opening_hours_raw",
        "treatment_examples_raw",
        "price_eur_min",
        "price_eur_max",
        "price_eur_raw",
        "rating",
        "review_count",
        "treatment_room_count",
        "hotel_room_count",
        "facilities_raw",
        "capacity_note_raw",
        "contact_raw",
        "matched_source_count",
        "enrich_source_urls",
    ]
    cols = [c for c in preferred if c in out.columns] + [c for c in out.columns if c not in preferred]
    out = out[cols]

    out.to_csv(OUTPUT, index=False)
    print(f"\nsaved: {OUTPUT}")
    print(f"shape: {out.shape}")

if __name__ == "__main__":
    main()
