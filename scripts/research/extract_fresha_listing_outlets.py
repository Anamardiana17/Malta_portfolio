import re
import pandas as pd
from pathlib import Path
from playwright.sync_api import sync_playwright

BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")
URL = "https://www.fresha.com/lp/en/tt/spa-packages/in/mt-malta"
OUT_FP = BASE / "data_processed/spa_research/fresha_malta_listing_outlets.csv"

def fetch_text(url: str) -> str:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1440, "height": 2400})
        page.goto(url, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(5000)
        # small scroll to trigger lazy content
        page.mouse.wheel(0, 3000)
        page.wait_for_timeout(3000)
        text = page.locator("body").inner_text()
        browser.close()
        return text

def clean_lines(text: str):
    lines = [x.strip() for x in text.splitlines() if x.strip()]
    # drop obvious nav noise
    bad = {
        "Download app", "Blog", "Careers", "Customer support", "Log in", "Home",
        "For business", "Browse", "Search", "Book now", "See all services"
    }
    out = []
    for x in lines:
        if x in bad:
            continue
        if len(x) < 2:
            continue
        out.append(x)
    return out

def looks_like_location(s: str) -> bool:
    s2 = s.lower()
    keys = [
        "malta", "gozo", "sliema", "st julians", "st. julians", "st julian's",
        "gzira", "gżira", "valletta", "qawra", "bugibba", "mellieha", "mellieħa",
        "san gwann", "swieqi", "mosta", "attard", "zira", "birkirkara"
    ]
    return any(k in s2 for k in keys)

def main():
    text = fetch_text(URL)
    lines = clean_lines(text)

    rows = []
    for i, line in enumerate(lines):
        # Pattern A: "<venue> 5.0 (428)"
        m = re.match(r"^(.*?)\s+([1-5]\.[0-9])\s*\(([\d,]+)\)\s*$", line)
        if m:
            outlet_name = m.group(1).strip()
            rating = float(m.group(2))
            review_count = int(m.group(3).replace(",", ""))

            location_text = None
            service_examples = []
            prices = []
            durations = []

            for j in range(i + 1, min(i + 10, len(lines))):
                s = lines[j].strip()

                if not location_text and looks_like_location(s):
                    location_text = s

                if "€" in s:
                    service_examples.append(s[:250])
                    for p in re.findall(r"€\s*([0-9]+(?:\.[0-9]{1,2})?)", s):
                        prices.append(float(p))
                    for d in re.findall(r"(\d+\s*(?:min|mins|minutes|hr|hrs|hour|hours))", s, re.I):
                        durations.append(d)

            rows.append({
                "outlet_name": outlet_name,
                "rating": rating,
                "review_count": review_count,
                "location_text": location_text,
                "service_examples": " | ".join(service_examples) if service_examples else None,
                "min_example_price_eur": min(prices) if prices else None,
                "max_example_price_eur": max(prices) if prices else None,
                "duration_examples": " | ".join(durations) if durations else None,
                "source_url": URL,
                "source_type": "fresha_listing",
            })

    df = pd.DataFrame(rows).drop_duplicates(subset=["outlet_name"])

    # Always save valid CSV with headers
    expected_cols = [
        "outlet_name","rating","review_count","location_text","service_examples",
        "min_example_price_eur","max_example_price_eur","duration_examples",
        "source_url","source_type"
    ]
    if df.empty:
        df = pd.DataFrame(columns=expected_cols)
    else:
        df = df[expected_cols]

    df.to_csv(OUT_FP, index=False)

    print("saved:", OUT_FP)
    print("shape:", df.shape)
    if not df.empty:
        print(df.head(30).to_string(index=False))
    else:
        print("No rows parsed from Fresha listing")
        print("\nTEXT PREVIEW:")
        print("\n".join(lines[:120]))

if __name__ == "__main__":
    main()
