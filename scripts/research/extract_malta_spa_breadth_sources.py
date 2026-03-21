import re
import pandas as pd
from pathlib import Path
from playwright.sync_api import sync_playwright

BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")
OUT_FP = BASE / "data_processed/spa_research/malta_spa_breadth_outlets.csv"

SOURCES = [
    ("fresha_spa_treatments", "https://www.fresha.com/lp/en/tt/spa-treatments/in/mt-malta"),
    ("fresha_spa_packages", "https://www.fresha.com/lp/en/tt/spa-packages/in/mt-malta"),
    ("fresha_full_body_massage", "https://www.fresha.com/lp/en/tt/full-body-massages/in/mt-malta"),
    ("tripadvisor_malta", "https://www.tripadvisor.com/Attractions-g190311-Activities-c40-Malta.html"),
    ("tripadvisor_island_of_malta", "https://www.tripadvisor.com/Attractions-g190320-Activities-c40-Island_of_Malta.html"),
    ("tripadvisor_st_julians", "https://www.tripadvisor.com/Attractions-g227101-Activities-c40-Saint_Julian_s_Island_of_Malta.html"),
    ("tripadvisor_st_pauls_bay", "https://www.tripadvisor.com/Attractions-g608946-Activities-c40-St_Paul_s_Bay_Island_of_Malta.html"),
    ("carisma_locations", "https://www.carismaspa.com/carisma-spa-locations-in-malta"),
    ("myoka_locations", "https://myoka.com/locations"),
    ("myoka_contact", "https://myoka.com/contact-us"),
]

AREA_KEYS = [
    "Malta", "Gozo", "Sliema", "St Julian", "Saint Julian", "St. Julian",
    "St Paul's Bay", "Qawra", "Bugibba", "Golden Bay", "Valletta",
    "Mellieha", "Mellieħa", "Marsaskala", "Swieqi", "San Gwann", "Gzira", "Gżira"
]

def fetch_text(url: str) -> str:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1440, "height": 3200})
        page.goto(url, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(4000)
        page.mouse.wheel(0, 6000)
        page.wait_for_timeout(3000)
        text = page.locator("body").inner_text()
        browser.close()
        return text

def add_row(rows, source_name, source_url, outlet_name, location_text=None, rating=None, review_count=None, price_hint=None):
    outlet_name = str(outlet_name).strip()
    if len(outlet_name) < 3:
        return
    rows.append({
        "source_name": source_name,
        "source_url": source_url,
        "outlet_name": outlet_name,
        "location_text": location_text,
        "rating": rating,
        "review_count": review_count,
        "price_hint_eur": price_hint,
    })

def parse_fresha(text, source_name, source_url, rows):
    lines = [x.strip() for x in text.splitlines() if x.strip()]
    for i, line in enumerate(lines):
        # Example: "soGuapa Spa & Nails 4.9 59 reviews ..."
        m = re.match(r"^(.*?)\s+([1-5]\.[0-9])\s+([\d,]+)\s+reviews?.*$", line, re.I)
        if not m:
            continue
        outlet = m.group(1).strip()
        rating = float(m.group(2))
        review_count = int(m.group(3).replace(",", ""))

        location_text = None
        price_hint = None
        for j in range(i, min(i + 8, len(lines))):
            s = lines[j]
            if location_text is None and any(k.lower() in s.lower() for k in AREA_KEYS):
                location_text = s[:180]
            pm = re.search(r"€\s*([0-9]+(?:\.[0-9]{1,2})?)", s)
            if pm and price_hint is None:
                price_hint = float(pm.group(1))

        add_row(rows, source_name, source_url, outlet, location_text, rating, review_count, price_hint)

def parse_tripadvisor(text, source_name, source_url, rows):
    lines = [x.strip() for x in text.splitlines() if x.strip()]
    for line in lines:
        # Example: "1. Nataraya Day Spa & Wellness · (1,104)"
        m = re.match(r"^\d+\.\s+(.*?)\s+[·\-]?\s*\(([\d,]+)\).*$", line)
        if m:
            outlet = m.group(1).strip()
            review_count = int(m.group(2).replace(",", ""))
            add_row(rows, source_name, source_url, outlet, None, None, review_count, None)

def parse_locations_page(text, source_name, source_url, rows):
    lines = [x.strip() for x in text.splitlines() if x.strip()]
    for i, line in enumerate(lines):
        # Carisma/Myoka location headings often look like names or resort labels
        if len(line) > 80:
            continue
        if any(bad in line.lower() for bad in [
            "facilities", "location", "contact", "read more", "mon - fri",
            "sat - sun", "our spa locations", "myoka spas", "carisma spa locations"
        ]):
            continue

        loc = None
        for j in range(i, min(i + 4, len(lines))):
            s = lines[j]
            if any(k.lower() in s.lower() for k in AREA_KEYS):
                loc = s[:180]
                break

        if loc is not None:
            add_row(rows, source_name, source_url, line, loc, None, None, None)

def main():
    rows = []

    for source_name, source_url in SOURCES:
        try:
            text = fetch_text(source_url)
            if "fresha" in source_name:
                parse_fresha(text, source_name, source_url, rows)
            elif "tripadvisor" in source_name:
                parse_tripadvisor(text, source_name, source_url, rows)
            else:
                parse_locations_page(text, source_name, source_url, rows)
            print("OK:", source_name, "rows_so_far=", len(rows))
        except Exception as e:
            print("FAIL:", source_name, e)

    df = pd.DataFrame(rows)

    if df.empty:
        df = pd.DataFrame(columns=[
            "source_name","source_url","outlet_name","location_text",
            "rating","review_count","price_hint_eur"
        ])
    else:
        df["outlet_name_norm"] = df["outlet_name"].fillna("").astype(str).str.strip().str.lower()
        bad = {
            "", "malta", "gozo", "contact us", "facilities", "location", "read more",
            "best spa treatments near me in malta", "tripadvisor", "myoka spas", "carisma spa locations"
        }
        df = df[~df["outlet_name_norm"].isin(bad)].copy()
        df = df.drop_duplicates(subset=["source_name", "outlet_name_norm"])
        df = df.drop(columns=["outlet_name_norm"])

    df.to_csv(OUT_FP, index=False)
    print("saved:", OUT_FP)
    print("shape:", df.shape)
    if not df.empty:
        print(df.head(80).to_string(index=False))

if __name__ == "__main__":
    main()
