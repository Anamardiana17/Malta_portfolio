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
OUTPUT = BASE / "data_processed/spa_research/malta_spa_outlet_master_enriched_v2.csv"
CACHE_DIR = BASE / "data_processed/spa_research/page_cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
}

PRICE_RE = re.compile(r"(€|eur\s?)(\s?\d{1,4}(?:[.,]\d{1,2})?)", re.I)
ROOM_COUNT_RE = re.compile(r"(\d{1,4})\s+(treatment rooms?|rooms?|guest rooms?|bedrooms?|suites?)", re.I)
HOURS_LINE_RE = re.compile(
    r"(monday|tuesday|wednesday|thursday|friday|saturday|sunday|mon[- ]?sun|daily|opening hours?|business hours?)",
    re.I
)
PHONE_RE = re.compile(r"(\+\d[\d\s()./-]{6,}\d)")
EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)

FACILITY_HINTS = [
    "sauna", "steam room", "steam", "indoor pool", "outdoor pool", "jacuzzi",
    "hammam", "gym", "fitness", "relaxation area", "relaxation room",
    "heated pool", "hydrotherapy", "thermal", "wellness area", "massage rooms",
    "treatment rooms", "beauty salon", "salt room", "plunge pool",
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

def norm_text(x: str) -> str:
    x = "" if pd.isna(x) else str(x)
    x = x.replace("’", "'").replace("–", "-").replace("—", "-")
    x = re.sub(r"\s+", " ", x).strip(" \n\r\t,;")
    return x

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
    out = []
    seen = set()
    for v in values:
        v = norm_text(v)
        if not v:
            continue
        key = v.lower()
        if key not in seen:
            seen.add(key)
            out.append(v)
    return sep.join(out)

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

    sample = ", ".join([f"€{int(v) if float(v).is_integer() else v}" for v in vals[:10]])
    return sample, min(vals), max(vals)

def extract_hours_candidates(text: str, soup: BeautifulSoup) -> str:
    hits = []

    for line in re.split(r"[\n\r]|(?<=[.])\s+", text):
        line2 = norm_text(line)
        if HOURS_LINE_RE.search(line2) and len(line2) <= 220:
            hits.append(line2)

    for el in soup.find_all(["li", "p", "div", "span", "td", "th"]):
        t = norm_text(el.get_text(" ", strip=True))
        if HOURS_LINE_RE.search(t) and len(t) <= 220:
            hits.append(t)

    return unique_join(hits[:20])

def extract_contacts(text: str, soup: BeautifulSoup) -> str:
    hits = []

    phones = PHONE_RE.findall(text)
    emails = EMAIL_RE.findall(text)
    hits.extend(phones)
    hits.extend(emails)

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("tel:"):
            hits.append(href.replace("tel:", "").strip())
        elif href.startswith("mailto:"):
            hits.append(href.replace("mailto:", "").strip())

    return unique_join(hits[:15])

def looks_addressish(line: str) -> bool:
    t = norm_text(line).lower()
    if not t:
        return False
    digit = bool(re.search(r"\d", t))
    hint = any(w in t for w in ADDRESS_HINT_WORDS)
    postcode = bool(re.search(r"\b[a-z]{2,4}\s?\d{3,5}\b", t))
    commas = t.count(",")
    return postcode or (hint and (digit or commas >= 1))

def extract_address(text: str, soup: BeautifulSoup) -> str:
    hits = []

    for el in soup.find_all(["address", "p", "div", "span", "li"]):
        t = norm_text(el.get_text(" ", strip=True))
        if 8 <= len(t) <= 200 and looks_addressish(t):
            hits.append(t)

    for line in re.split(r"[\n\r]|(?<=[.])\s+", text):
        t = norm_text(line)
        if 8 <= len(t) <= 200 and looks_addressish(t):
            hits.append(t)

    return unique_join(hits[:10])

def extract_room_counts(text: str):
    treatment_room_count = None
    hotel_room_count = None
    notes = []

    for m in ROOM_COUNT_RE.finditer(text):
        n = int(m.group(1))
        label = m.group(2).lower()

        if "treatment" in label or "massage room" in label:
            if 1 <= n <= 100:
                treatment_room_count = n if treatment_room_count is None else max(treatment_room_count, n)
                notes.append(f"{n} {label}")
        elif "guest room" in label or "bedroom" in label or "suite" in label:
            if 5 <= n <= 5000:
                hotel_room_count = n if hotel_room_count is None else max(hotel_room_count, n)
                notes.append(f"{n} {label}")
        elif "rooms" in label:
            if n <= 100:
                notes.append(f"ambiguous:{n} {label}")
            elif n > 100:
                hotel_room_count = n if hotel_room_count is None else max(hotel_room_count, n)
                notes.append(f"{n} {label}")

    return treatment_room_count, hotel_room_count, unique_join(notes)

def extract_facilities(text: str) -> str:
    t = text.lower()
    found = [f for f in FACILITY_HINTS if f in t]
    return unique_join(found)

def extract_services(text: str) -> str:
    t = text.lower()
    found = [s for s in SERVICE_HINTS if s in t]
    return unique_join(found)

def collect_source_urls(row: pd.Series) -> list[str]:
    urls = []
    for col in row.index:
        if "url" in col.lower():
            val = norm_text(row[col])
            if val.startswith("http"):
                urls.append(val)
    out = []
    seen = set()
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out

def enrich_row(row: pd.Series) -> dict:
    urls = collect_source_urls(row)
    texts = []
    soups = []
    fetched_urls = []

    for url in urls[:6]:
        html = fetch_html(url)
        if not html:
            continue
        texts.append(extract_text(html))
        soups.append(soupify(html))
        fetched_urls.append(url)

    full_text = "\n".join([t for t in texts if t])
    if not full_text:
        return {
            "enrich_source_urls": unique_join(urls),
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
        }

    all_hours = []
    all_addr = []
    all_contacts = []

    for txt, soup in zip(texts, soups):
        all_hours.append(extract_hours_candidates(txt, soup))
        all_addr.append(extract_address(txt, soup))
        all_contacts.append(extract_contacts(txt, soup))

    price_eur_raw, price_min, price_max = extract_prices(full_text)
    treatment_room_count, hotel_room_count, room_note = extract_room_counts(full_text)
    facilities_raw = extract_facilities(full_text)
    treatment_examples_raw = extract_services(full_text)

    capacity_bits = []
    if room_note:
        capacity_bits.append(room_note)
    if treatment_room_count is not None:
        capacity_bits.append(f"treatment_room_count={treatment_room_count}")
    if hotel_room_count is not None:
        capacity_bits.append(f"hotel_room_count={hotel_room_count}")

    return {
        "enrich_source_urls": unique_join(fetched_urls or urls),
        "opening_hours_raw": unique_join(all_hours),
        "address_raw": unique_join(all_addr),
        "contact_raw": unique_join(all_contacts),
        "treatment_examples_raw": treatment_examples_raw,
        "price_eur_raw": price_eur_raw,
        "price_eur_min": price_min,
        "price_eur_max": price_max,
        "treatment_room_count": treatment_room_count,
        "hotel_room_count": hotel_room_count,
        "facilities_raw": facilities_raw,
        "capacity_note_raw": unique_join(capacity_bits),
    }

def main():
    if not INPUT.exists():
        raise FileNotFoundError(f"Missing input: {INPUT}")

    df = pd.read_csv(INPUT)
    enriched_records = []

    for i, (_, row) in enumerate(df.iterrows(), start=1):
        rec = enrich_row(row)
        rec["outlet_name_final"] = row.get("outlet_name_final", "")
        enriched_records.append(rec)
        print(f"[{i}/{len(df)}] {row.get('outlet_name_final', '')}")

    enr = pd.DataFrame(enriched_records)

    out = df.merge(
        enr.drop_duplicates(subset=["outlet_name_final"]),
        on="outlet_name_final",
        how="left"
    )

    preferred_front = [
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
        "enrich_source_urls",
    ]
    front = [c for c in preferred_front if c in out.columns]
    other = [c for c in out.columns if c not in front]
    out = out[front + other]

    out.to_csv(OUTPUT, index=False)
    print(f"\nsaved: {OUTPUT}")
    print(f"shape: {out.shape}")

if __name__ == "__main__":
    main()
