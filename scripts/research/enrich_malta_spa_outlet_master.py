import re
import pandas as pd
from pathlib import Path
from playwright.sync_api import sync_playwright

BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")
IN_FP = BASE / "data_processed/spa_research/malta_spa_outlet_master_clean_v2.csv"
OUT_FP = BASE / "data_processed/spa_research/malta_spa_outlet_master_enriched.csv"

# add or edit mapping if needed
OUTLET_URL_MAP = {
    "Carisma Spa Malta": "https://www.carismaspa.com/",
    "InterContinental Malta": "https://malta.intercontinental.com/spa",
    "Hilton Malta": "https://myoka.com/fivesenses",
    "Malta Marriott Resort & Spa": "https://myoka.com/lotus",
    "DoubleTree by Hilton Malta": "https://myoka.com/doubletree",
    "Phoenicia Malta Spa": "https://phoeniciamalta.com/spa-wellness/spa-treatments/",
    "Essensi Spa": "https://inialamalta.com/spa/",
}

FACILITY_KEYS = [
    "sauna", "steam room", "jacuzzi", "whirlpool", "indoor pool", "outdoor pool",
    "relaxation area", "gym", "hamam", "hammam", "hydrotherapy", "fitness", "spa day"
]

def fetch_text(url: str) -> str:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1440, "height": 3200})
        page.goto(url, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(5000)
        page.mouse.wheel(0, 4000)
        page.wait_for_timeout(2500)
        text = page.locator("body").inner_text()
        browser.close()
        return text

def extract_opening_hours(text: str):
    lines = [x.strip() for x in text.splitlines() if x.strip()]
    hits = []
    patterns = [
        r"(Mon|Monday).{0,40}(Fri|Friday).{0,40}\d",
        r"(Mon|Monday).{0,80}(Sun|Sunday).{0,80}\d",
        r"\b\d{1,2}(?::\d{2})?\s*(am|pm)\b.{0,30}\b\d{1,2}(?::\d{2})?\s*(am|pm)\b",
        r"\b\d{2}:\d{2}\s*[-–]\s*\d{2}:\d{2}\b",
    ]
    for line in lines:
        for pat in patterns:
            if re.search(pat, line, flags=re.I):
                hits.append(line[:200])
                break
    hits = list(dict.fromkeys(hits))
    return " | ".join(hits[:6]) if hits else None

def extract_capacity_fields(text: str):
    treatment_room_count = None
    facility_capacity_people = None
    hotel_room_count = None
    capacity_note_raw = []

    patterns_rooms = [
        r"(\d+)\s+treatment rooms?",
        r"spa includes\s+(\d+)\s+treatment rooms?",
        r"features\s+(\d+)\s+treatment rooms?",
    ]
    for pat in patterns_rooms:
        m = re.search(pat, text, re.I)
        if m:
            treatment_room_count = int(m.group(1))
            capacity_note_raw.append(m.group(0))
            break

    patterns_people = [
        r"accommodate\s+(\d+)\s+people",
        r"capacity\s+of\s+(\d+)",
        r"can accommodate\s+(\d+)",
    ]
    for pat in patterns_people:
        m = re.search(pat, text, re.I)
        if m:
            facility_capacity_people = int(m.group(1))
            capacity_note_raw.append(m.group(0))
            break

    patterns_hotel = [
        r"(\d+)\s*-\s*room hotel",
        r"(\d+)\s+room hotel",
        r"(\d+)\s+rooms and suites",
        r"(\d+)\s+rooms",
    ]
    for pat in patterns_hotel:
        m = re.search(pat, text, re.I)
        if m:
            hotel_room_count = int(m.group(1))
            capacity_note_raw.append(m.group(0))
            break

    return treatment_room_count, facility_capacity_people, hotel_room_count, " | ".join(capacity_note_raw) if capacity_note_raw else None

def extract_facilities(text: str):
    found = []
    low = text.lower()
    for k in FACILITY_KEYS:
        if k in low:
            found.append(k)
    found = list(dict.fromkeys(found))
    return " | ".join(found) if found else None

def main():
    df = pd.read_csv(IN_FP)

    if "outlet_name_final" not in df.columns:
        raise SystemExit("Missing outlet_name_final in clean outlet master")

    rows = []
    for _, r in df.iterrows():
        outlet = str(r["outlet_name_final"]).strip()
        url = OUTLET_URL_MAP.get(outlet, r.get("source_url"))

        opening_hours_raw = None
        treatment_room_count = None
        facility_capacity_people = None
        hotel_room_count = None
        capacity_note_raw = None
        facilities_raw = None

        try:
            if pd.notna(url) and str(url).strip():
                text = fetch_text(str(url).strip())
                opening_hours_raw = extract_opening_hours(text)
                treatment_room_count, facility_capacity_people, hotel_room_count, capacity_note_raw = extract_capacity_fields(text)
                facilities_raw = extract_facilities(text)
        except Exception as e:
            capacity_note_raw = f"fetch_failed: {e}"

        row = r.to_dict()
        row["resolved_url"] = url
        row["opening_hours_raw"] = opening_hours_raw
        row["opening_hours_detected_flag"] = 1 if opening_hours_raw else 0
        row["treatment_room_count"] = treatment_room_count
        row["facility_capacity_people"] = facility_capacity_people
        row["hotel_room_count"] = hotel_room_count
        row["capacity_note_raw"] = capacity_note_raw
        row["facilities_raw"] = facilities_raw
        rows.append(row)

        print(f"OK | {outlet} | hours={bool(opening_hours_raw)} | tr_rooms={treatment_room_count} | hotel_rooms={hotel_room_count}")

    out = pd.DataFrame(rows)
    out.to_csv(OUT_FP, index=False)

    print("saved:", OUT_FP)
    print("shape:", out.shape)
    keep = [c for c in [
        "outlet_name_final","opening_hours_raw","treatment_room_count",
        "facility_capacity_people","hotel_room_count","capacity_note_raw","facilities_raw","resolved_url"
    ] if c in out.columns]
    print(out[keep].to_string(index=False))

if __name__ == "__main__":
    main()
