from __future__ import annotations

import json
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
OUTPUT = BASE / "data_processed/spa_research/malta_spa_outlet_master_enriched_v4.csv"
CACHE_DIR = BASE / "data_processed/spa_research/page_cache_v4"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
}

PRICE_RE = re.compile(r"(€|eur\s?)(\s?\d{1,4}(?:[.,]\d{1,2})?)", re.I)
PRICE_RANGE_RE = re.compile(
    r"(€\s?\d{1,4}(?:[.,]\d{1,2})?)\s*(?:-|to|–|—)\s*(€\s?\d{1,4}(?:[.,]\d{1,2})?)",
    re.I
)

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

TREATMENT_ROOM_PATTERNS = [
    re.compile(r"(\d{1,3})\s+treatment\s+rooms?\b", re.I),
    re.compile(r"(\d{1,3})\s+massage\s+rooms?\b", re.I),
    re.compile(r"\bwith\s+(\d{1,3})\s+treatment\s+rooms?\b", re.I),
]

HOTEL_ROOM_PATTERNS = [
    re.compile(r"(\d{1,4})\s+guest\s+rooms?\b", re.I),
    re.compile(r"(\d{1,4})\s+bedrooms?\b", re.I),
    re.compile(r"(\d{1,4})\s+rooms?\s+and\s+\d+\s+suites?\b", re.I),
    re.compile(r"\boffers?\s+(\d{1,4})\s+rooms?\b", re.I),
    re.compile(r"\bfeatures?\s+(\d{1,4})\s+rooms?\b", re.I),
]

RATING_PATTERNS = [
    re.compile(r"\b([0-5](?:\.\d)?)\s*/\s*5\b"),
    re.compile(r"\brated\s+([0-5](?:\.\d)?)\s*(?:out of)?\s*5\b", re.I),
    re.compile(r"\b([0-5](?:\.\d)?)\s*stars?\b", re.I),
]

REVIEW_PATTERNS = [
    re.compile(r"\b(\d{1,6}(?:,\d{3})*)\s+reviews?\b", re.I),
    re.compile(r"\bfrom\s+(\d{1,6}(?:,\d{3})*)\s+reviews?\b", re.I),
    re.compile(r"\b(\d{1,6}(?:,\d{3})*)\+\s+reviews\b", re.I),
]

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

def page_matches_outlet(outlet_name: str, url: str, text: str) -> bool:
    outlet_slug = slug(outlet_name)
    toks = brand_tokens(outlet_name)
    combined = f"{urlparse(url).netloc.lower()} {slug(url)} {slug(text[:8000])}"

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

def parse_eur_amount(s: str) -> float | None:
    s = s.lower().replace("eur", "").replace("€", "").strip()
    s = s.replace(",", ".")
    try:
        v = float(s)
        if 5 <= v <= 1000:
            return v
    except Exception:
        return None
    return None

def extract_prices(text: str):
    vals = []

    for m in PRICE_RANGE_RE.finditer(text):
        a = parse_eur_amount(m.group(1))
        b = parse_eur_amount(m.group(2))
        if a is not None:
            vals.append(a)
        if b is not None:
            vals.append(b)

    for m in PRICE_RE.finditer(text):
        v = parse_eur_amount(m.group(2))
        if v is not None:
            vals.append(v)

    vals = sorted(set(vals))
    if not vals:
        return "", None, None

    sample = ", ".join([f"€{int(v) if float(v).is_integer() else v}" for v in vals[:15]])
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

def extract_facilities(text: str) -> str:
    t = text.lower()
    return unique_join([f for f in FACILITY_HINTS if f in t])

def extract_services(text: str) -> str:
    t = text.lower()
    return unique_join([s for s in SERVICE_HINTS if s in t])

def first_int_from_match(patterns: list[re.Pattern], text: str, lo: int, hi: int) -> int | None:
    vals = []
    for pat in patterns:
        for m in pat.finditer(text):
            try:
                v = int(m.group(1))
                if lo <= v <= hi:
                    vals.append(v)
            except Exception:
                pass
    return max(vals) if vals else None

def extract_room_counts(text: str):
    treatment_room_count = first_int_from_match(TREATMENT_ROOM_PATTERNS, text, 1, 100)
    hotel_room_count = first_int_from_match(HOTEL_ROOM_PATTERNS, text, 5, 5000)
    notes = []
    if treatment_room_count is not None:
        notes.append(f"treatment_room_count={treatment_room_count}")
    if hotel_room_count is not None:
        notes.append(f"hotel_room_count={hotel_room_count}")
    return treatment_room_count, hotel_room_count, unique_join(notes)

def iter_jsonld_objects(soup: BeautifulSoup):
    for tag in soup.find_all("script", attrs={"type": re.compile("ld\\+json", re.I)}):
        raw = tag.string or tag.get_text(" ", strip=True)
        raw = raw.strip()
        if not raw:
            continue
        try:
            obj = json.loads(raw)
            yield obj
        except Exception:
            continue

def flatten_jsonld(obj):
    if isinstance(obj, list):
        for item in obj:
            yield from flatten_jsonld(item)
    elif isinstance(obj, dict):
        yield obj
        for k in ["@graph", "itemListElement"]:
            if k in obj:
                yield from flatten_jsonld(obj[k])

def parse_rating_review_from_jsonld(soup: BeautifulSoup):
    ratings = []
    reviews = []

    for obj in iter_jsonld_objects(soup):
        for node in flatten_jsonld(obj):
            agg = node.get("aggregateRating")
            if isinstance(agg, dict):
                rv = agg.get("ratingValue")
                rc = agg.get("reviewCount") or agg.get("ratingCount")
                try:
                    if rv is not None:
                        rvf = float(str(rv).replace(",", "."))
                        if 0 <= rvf <= 5:
                            ratings.append(rvf)
                except Exception:
                    pass
                try:
                    if rc is not None:
                        rci = int(str(rc).replace(",", "").strip())
                        if 0 <= rci <= 1000000:
                            reviews.append(rci)
                except Exception:
                    pass

            # direct keys
            for key in ["ratingValue"]:
                if key in node:
                    try:
                        rvf = float(str(node[key]).replace(",", "."))
                        if 0 <= rvf <= 5:
                            ratings.append(rvf)
                    except Exception:
                        pass
            for key in ["reviewCount", "ratingCount"]:
                if key in node:
                    try:
                        rci = int(str(node[key]).replace(",", "").strip())
                        if 0 <= rci <= 1000000:
                            reviews.append(rci)
                    except Exception:
                        pass

    return (max(ratings) if ratings else None, max(reviews) if reviews else None)

def parse_rating_review_from_html(text: str, soup: BeautifulSoup):
    ratings = []
    reviews = []

    whole = " ".join([
        text,
        " ".join([norm_text(a.get("aria-label", "")) for a in soup.find_all(attrs={"aria-label": True})]),
    ])

    for pat in RATING_PATTERNS:
        for m in pat.finditer(whole):
            try:
                v = float(m.group(1).replace(",", "."))
                if 0 <= v <= 5:
                    ratings.append(v)
            except Exception:
                pass

    for pat in REVIEW_PATTERNS:
        for m in pat.finditer(whole):
            try:
                v = int(m.group(1).replace(",", "").strip())
                if 0 <= v <= 1000000:
                    reviews.append(v)
            except Exception:
                pass

    return (max(ratings) if ratings else None, max(reviews) if reviews else None)

def best_number(nums):
    nums = [x for x in nums if x is not None]
    return max(nums) if nums else None

def enrich_row(row: pd.Series) -> dict:
    outlet_name = norm_text(row.get("outlet_name_final", ""))
    urls = collect_source_urls(row)

    matched_urls = []
    texts = []
    soups = []
    htmls = []

    for url in urls[:8]:
        html = fetch_html(url)
        if not html:
            continue
        txt = extract_text(html)
        if page_matches_outlet(outlet_name, url, txt):
            matched_urls.append(url)
            texts.append(txt)
            soups.append(soupify(html))
            htmls.append(html)

    full_text = "\n".join([t for t in texts if t])

    if not full_text:
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
            "rating": None,
            "review_count": None,
            "treatment_room_count": None,
            "hotel_room_count": None,
            "facilities_raw": "",
            "capacity_note_raw": "",
            "contact_raw": "",
        }

    hours = [extract_hours(t, s) for t, s in zip(texts, soups)]
    addr = [extract_address(t, s) for t, s in zip(texts, soups)]
    contacts = [extract_contacts(t, s) for t, s in zip(texts, soups)]

    price_raw, price_min, price_max = extract_prices(full_text)
    treatment_room_count, hotel_room_count, cap_note = extract_room_counts(full_text)
    facilities_raw = extract_facilities(full_text)
    treatment_examples_raw = extract_services(full_text)

    jsonld_ratings = []
    jsonld_reviews = []
    html_ratings = []
    html_reviews = []

    for txt, soup in zip(texts, soups):
        r1, c1 = parse_rating_review_from_jsonld(soup)
        r2, c2 = parse_rating_review_from_html(txt, soup)
        jsonld_ratings.append(r1)
        jsonld_reviews.append(c1)
        html_ratings.append(r2)
        html_reviews.append(c2)

    rating = best_number(jsonld_ratings + html_ratings)
    review_count = best_number(jsonld_reviews + html_reviews)

    return {
        "outlet_name_final": outlet_name,
        "matched_source_count": len(matched_urls),
        "enrich_source_urls": unique_join(matched_urls),
        "address_raw": unique_join(addr),
        "opening_hours_raw": unique_join(hours),
        "treatment_examples_raw": treatment_examples_raw,
        "price_eur_raw": price_raw,
        "price_eur_min": price_min,
        "price_eur_max": price_max,
        "rating": rating,
        "review_count": review_count,
        "treatment_room_count": treatment_room_count,
        "hotel_room_count": hotel_room_count,
        "facilities_raw": facilities_raw,
        "capacity_note_raw": cap_note,
        "contact_raw": unique_join(contacts),
    }

def main():
    if not INPUT.exists():
        raise FileNotFoundError(f"Missing input: {INPUT}")

    df = pd.read_csv(INPUT)
    records = []

    for i, (_, row) in enumerate(df.iterrows(), start=1):
        rec = enrich_row(row)
        records.append(rec)
        print(f"[{i}/{len(df)}] {rec['outlet_name_final']} | matched_sources={rec['matched_source_count']} | rating={rec['rating']} | reviews={rec['review_count']}")

    enr = pd.DataFrame(records)
    out = df.merge(enr, on="outlet_name_final", how="left", suffixes=("", "_new"))

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
