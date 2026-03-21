import re
import pandas as pd
import requests
from bs4 import BeautifulSoup
from pathlib import Path
from playwright.sync_api import sync_playwright

BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")
IN_FP = BASE / "data_raw/spa_research/malta_spa_seed_urls.csv"
OUT_TREAT_FP = BASE / "data_processed/spa_research/malta_spa_treatments_raw.csv"
OUT_OUTLET_FP = BASE / "data_processed/spa_research/malta_spa_outlets_raw.csv"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0 Safari/537.36"
}

PRICE_RE = re.compile(r"(?:€|EUR)\s*([0-9]+(?:\.[0-9]{1,2})?)", re.I)
DURATION_RE = re.compile(r"(\d+)\s*(?:min|mins|minutes|hr|hrs|hour|hours)", re.I)


def fetch_html(url: str) -> str:
    try:
        r = requests.get(url, headers=HEADERS, timeout=25)
        if r.ok and len(r.text) > 800:
            return r.text
    except Exception:
        pass

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url, wait_until="networkidle", timeout=45000)
        html = page.content()
        browser.close()
        return html


def infer_source_type(url: str) -> str:
    u = url.lower()
    if "fresha.com" in u:
        return "fresha"
    if "tripadvisor.com" in u:
        return "tripadvisor"
    return "official"


def parse_outlet_name(soup, url):
    for tag in ["h1", "title"]:
        node = soup.find(tag)
        if node and node.get_text(strip=True):
            txt = node.get_text(" ", strip=True)
            txt = re.sub(r"\s+\|\s+.*$", "", txt)
            txt = re.sub(r"\s+-\s+.*$", "", txt)
            return txt[:180]
    return url


def find_rating_review(text):
    rating = None
    review_count = None

    m_rating = re.search(r"\b([1-5]\.[0-9])\b", text)
    if m_rating:
        try:
            rating = float(m_rating.group(1))
        except Exception:
            pass

    m_review = re.search(r"(\d[\d,]*)\s+reviews?", text, re.I)
    if m_review:
        try:
            review_count = int(m_review.group(1).replace(",", ""))
        except Exception:
            pass

    return rating, review_count


def guess_city(text):
    areas = [
        "Valletta", "St Julian's", "St. Julian's", "Sliema", "Gzira", "Gżira",
        "Mellieha", "Mellieħa", "Qawra", "Bugibba", "St Paul's Bay",
        "Golden Bay", "Floriana", "Gozo", "Swieqi", "Mosta", "Attard"
    ]
    txt = text.lower()
    for a in areas:
        if a.lower() in txt:
            return a
    return None


def clean_duration_to_min(v):
    if pd.isna(v) or v is None:
        return None
    s = str(v).lower()
    m = re.search(r"(\d+)\s*(min|mins|minutes)", s)
    if m:
        return int(m.group(1))
    h = re.search(r"(\d+)\s*(hr|hrs|hour|hours)", s)
    if h:
        return int(h.group(1)) * 60
    return None


def parse_treatments_from_lines(lines):
    rows = []
    for i, line in enumerate(lines):
        if ("€" not in line) and ("EUR" not in line.upper()):
            continue

        prices = PRICE_RE.findall(line)
        if not prices:
            continue

        prev1 = lines[i - 1] if i - 1 >= 0 else ""
        prev2 = lines[i - 2] if i - 2 >= 0 else ""
        next1 = lines[i + 1] if i + 1 < len(lines) else ""

        treatment_name = prev1 if len(prev1) >= 4 else prev2
        if len(treatment_name) < 4:
            treatment_name = line

        duration_match = (
            DURATION_RE.search(line)
            or DURATION_RE.search(prev1)
            or DURATION_RE.search(prev2)
            or DURATION_RE.search(next1)
        )
        duration_raw = duration_match.group(0) if duration_match else None

        for p in prices:
            try:
                price = float(p)
            except Exception:
                continue

            rows.append({
                "treatment_name": treatment_name[:180].strip(),
                "duration_raw": duration_raw,
                "price_eur": price,
                "raw_line": line[:300],
            })
    return rows


def main():
    seeds = pd.read_csv(IN_FP)
    outlet_rows = []
    treatment_rows = []

    for idx, r in seeds.iterrows():
        url = str(r["url"]).strip()
        try:
            html = fetch_html(url)
            soup = BeautifulSoup(html, "lxml")
            text = soup.get_text("\n", strip=True)
            lines = [x.strip() for x in text.splitlines() if x.strip()]

            outlet_name = parse_outlet_name(soup, url)
            source_type = infer_source_type(url)
            rating, review_count = find_rating_review(text)
            city_area = guess_city(text)

            outlet_rows.append({
                "outlet_name": outlet_name,
                "city_area": city_area,
                "source_type": source_type,
                "source_url": url,
                "rating": rating,
                "review_count": review_count,
                "page_title": soup.title.get_text(" ", strip=True)[:250] if soup.title else None,
            })

            treatments = parse_treatments_from_lines(lines)
            for t in treatments:
                t["outlet_name"] = outlet_name
                t["city_area"] = city_area
                t["source_type"] = source_type
                t["source_url"] = url
                t["rating"] = rating
                t["review_count"] = review_count
                t["duration_min"] = clean_duration_to_min(t["duration_raw"])
                treatment_rows.append(t)

            print(f"[{idx+1}/{len(seeds)}] OK | {outlet_name} | treatments={len(treatments)} | {url}")

        except Exception as e:
            print(f"[{idx+1}/{len(seeds)}] FAIL | {url} | {e}")

    df_outlets = pd.DataFrame(outlet_rows).drop_duplicates(subset=["source_url"])
    df_treat = pd.DataFrame(treatment_rows)

    if not df_treat.empty:
        df_treat["price_eur"] = pd.to_numeric(df_treat["price_eur"], errors="coerce")
        df_treat["duration_min"] = pd.to_numeric(df_treat["duration_min"], errors="coerce")
        df_treat["treatment_name"] = df_treat["treatment_name"].astype(str).str.strip()
        df_treat = df_treat[df_treat["price_eur"].between(5, 1000, inclusive="both")].copy()

    df_outlets.to_csv(OUT_OUTLET_FP, index=False)
    df_treat.to_csv(OUT_TREAT_FP, index=False)

    print("\nsaved:", OUT_OUTLET_FP, "rows=", len(df_outlets))
    print("saved:", OUT_TREAT_FP, "rows=", len(df_treat))


if __name__ == "__main__":
    main()
