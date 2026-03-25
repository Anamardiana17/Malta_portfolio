from __future__ import annotations

from pathlib import Path
import io
import json
import re
import subprocess
from typing import Optional

import pandas as pd
import pdfplumber
import requests

BASE_DIR = Path("/Users/ambakinanti/Desktop/Malta_portfolio")
RAW_DIR = BASE_DIR / "data_raw" / "entry_exits_passenger"
PROCESSED_DIR = BASE_DIR / "data_processed" / "entry_exits_passenger"

RAW_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

SOURCES = [
    {
        "source_key": "air_passenger_movements_page",
        "source_group": "air",
        "source_type": "html",
        "url": "https://nso.gov.mt/air-transport-passenger-mail-and-cargo-movements/",
        "raw_path": RAW_DIR / "entry_exits_passenger_air_passenger_movements.html",
    },
    {
        "source_key": "sea_transport_portal_page",
        "source_group": "sea",
        "source_type": "html",
        "url": "https://nso.gov.mt/sea-transport-sea-transport-between-malta-and-gozo/",
        "raw_path": RAW_DIR / "entry_exits_passenger_sea_transport_portal.html",
    },
    {
        "source_key": "sea_transport_gozo_q4_2025_pdf",
        "source_group": "sea",
        "source_type": "pdf",
        "url": "https://nso.gov.mt/wp-content/uploads/NR-013-2026_pQKs8S.pdf",
        "raw_path": RAW_DIR / "entry_exits_passenger_sea_transport_gozo_q4_2025.pdf",
    },
    {
        "source_key": "cruise_passengers_q4_2025_pdf",
        "source_group": "cruise",
        "source_type": "pdf",
        "url": "https://nso.gov.mt/wp-content/uploads/NR-011-2026-YKFPGD.pdf",
        "raw_path": RAW_DIR / "entry_exits_passenger_cruise_passengers_q4_2025.pdf",
    },
    {
        "source_key": "bus_service_page",
        "source_group": "bus",
        "source_type": "html",
        "url": "https://www.transport.gov.mt/land/public-transport/bus-service-744",
        "raw_path": RAW_DIR / "entry_exits_passenger_bus_service.html",
    },
    {
        "source_key": "bus_monitoring_page",
        "source_group": "bus",
        "source_type": "html",
        "url": "https://www.transport.gov.mt/land/public-transport/bus-service/public-transport-monitoring-746",
        "raw_path": RAW_DIR / "entry_exits_passenger_bus_monitoring.html",
    },
    {
        "source_key": "bus_routes_timetables_page",
        "source_group": "bus",
        "source_type": "html",
        "url": "https://www.publictransport.com.mt/en/routes-timetables",
        "raw_path": RAW_DIR / "entry_exits_passenger_bus_routes_timetables.html",
    },
]

REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "*/*",
    "Referer": "https://www.google.com/",
}

def fetch_with_requests(url: str, timeout: int = 120) -> bytes:
    r = requests.get(url, headers=REQUEST_HEADERS, timeout=timeout)
    r.raise_for_status()
    return r.content

def fetch_with_curl(url: str, out_fp: Path) -> None:
    cmd = [
        "curl",
        "-L",
        "--fail",
        "-A", REQUEST_HEADERS["User-Agent"],
        "-H", f"Accept-Language: {REQUEST_HEADERS['Accept-Language']}",
        "-H", f"Referer: {REQUEST_HEADERS['Referer']}",
        "-o", str(out_fp),
        url,
    ]
    subprocess.run(cmd, check=True)

def fetch_bytes(url: str, out_fp: Path) -> str:
    try:
        data = fetch_with_requests(url)
        out_fp.write_bytes(data)
        return "requests_ok"
    except Exception:
        fetch_with_curl(url, out_fp)
        return "curl_ok"

def html_to_text(html: str) -> str:
    txt = re.sub(r"<script.*?</script>", " ", html, flags=re.S | re.I)
    txt = re.sub(r"<style.*?</style>", " ", txt, flags=re.S | re.I)
    txt = re.sub(r"<[^>]+>", " ", txt)
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt

def clean_colname(x: object) -> str:
    s = str(x).strip().lower()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^a-z0-9_]+", "", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s

def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [clean_colname(c) for c in out.columns]
    return out

def build_page_features(source_key: str, source_group: str, url: str, text: str) -> pd.DataFrame:
    tl = text.lower()
    row = {
        "source_key": source_key,
        "source_group": source_group,
        "source_url": url,
        "text_length": len(text),
        "mentions_passenger": int("passenger" in tl),
        "mentions_airport": int("airport" in tl),
        "mentions_gozo": int("gozo" in tl),
        "mentions_ferry": int("ferry" in tl),
        "mentions_cruise": int("cruise" in tl),
        "mentions_bus": int("bus" in tl),
        "mentions_route": int("route" in tl),
        "mentions_timetable": int("timetable" in tl or "schedule" in tl),
        "mentions_realtime": int("real time" in tl or "real-time" in tl),
        "mentions_monitoring": int("monitor" in tl or "control room" in tl),
    }
    return pd.DataFrame([row])

def extract_pdf_text(pdf_fp: Path) -> str:
    texts = []
    with pdfplumber.open(str(pdf_fp)) as pdf:
        for page in pdf.pages:
            txt = page.extract_text() or ""
            if txt.strip():
                texts.append(txt)
    return "\n".join(texts)

def first_int(pattern: str, text: str) -> Optional[int]:
    m = re.search(pattern, text, flags=re.I | re.S)
    if not m:
        return None
    raw = m.group(1).replace(",", "").replace(" ", "")
    try:
        return int(raw)
    except Exception:
        return None

def extract_sea_pdf_metrics(text: str) -> pd.DataFrame:
    row = {
        "source_key": "sea_transport_gozo_q4_2025_pdf",
        "period_label": "Q4_2025",
        "metric_scope": "quarterly_release",
        "total_passengers_q4": first_int(r"carrying a total of\s+([\d,]+)\s+passengers", text),
        "gozo_channel_passengers_q4": first_int(r"A total of\s+([\d,]+)\s+passengers travelled between Mġarr and Ċirkewwa", text),
        "gozo_channel_vehicles_q4": first_int(r"number of vehicles increased by .*? totalling\s+([\d,]+)", text),
        "gozo_channel_october_passengers": first_int(r"October\.\s*When compared.*?October\D+([\d,]+)\D+or\s+37\.6", text),
        "fast_ferry_december_trips": first_int(r"December registered the highest number of trips, totaling\s+([\d,]+)", text),
    }
    return pd.DataFrame([row])

def extract_cruise_pdf_metrics(text: str) -> pd.DataFrame:
    row = {
        "source_key": "cruise_passengers_q4_2025_pdf",
        "period_label": "Q4_2025",
        "metric_scope": "quarterly_release",
        "total_cruise_passengers_q4": first_int(r"fourth quarter of 2025 amounted to\s+([\d,]+)", text),
        "cruise_liner_calls_q4": first_int(r"There were\s+([\d,]+)\s+cruise liner calls during the fourth quarter of 2025", text),
        "transit_passengers_q4": first_int(r"reaching\s+([\d,]+)\s+\(94\.3 per cent\)", text),
        "total_cruise_passengers_2025": first_int(r"During 2025,\s+total cruise passengers stood at\s+([\d,]+)", text),
        "cruise_liner_calls_2025": first_int(r"There were\s+([\d,]+)\s+cruise liner calls during the year", text),
    }
    return pd.DataFrame([row])

def save_html_tables(source_key: str, source_group: str, url: str, html: str, manifest_rows: list[dict]) -> None:
    try:
        tables = pd.read_html(io.StringIO(html))
    except Exception:
        tables = []

    if not tables:
        manifest_rows.append({
            "source_key": source_key,
            "source_group": source_group,
            "source_url": url,
            "parsed_path": "",
            "artifact_type": "html_page",
            "rows": None,
            "cols": None,
            "status": "html_only",
        })
        return

    for i, tbl in enumerate(tables, start=1):
        try:
            out = clean_df(tbl)
            out.insert(0, "source_key", source_key)
            out.insert(1, "source_group", source_group)
            out.insert(2, "source_url", url)
            out_fp = PROCESSED_DIR / f"entry_exits_passenger_{source_key}_table_{i}.csv"
            out.to_csv(out_fp, index=False)
            manifest_rows.append({
                "source_key": source_key,
                "source_group": source_group,
                "source_url": url,
                "parsed_path": str(out_fp),
                "artifact_type": "html_table",
                "rows": len(out),
                "cols": len(out.columns),
                "status": "ok",
            })
        except Exception as e:
            manifest_rows.append({
                "source_key": source_key,
                "source_group": source_group,
                "source_url": url,
                "parsed_path": "",
                "artifact_type": "html_table",
                "rows": None,
                "cols": None,
                "status": f"table_parse_fail: {e}",
            })

def main() -> None:
    manifest_rows: list[dict] = []
    feature_frames: list[pd.DataFrame] = []
    pdf_metric_frames: list[pd.DataFrame] = []

    for item in SOURCES:
        source_key = item["source_key"]
        source_group = item["source_group"]
        source_type = item["source_type"]
        url = item["url"]
        raw_fp = item["raw_path"]

        print(f"\n=== FETCH {source_key} ===")
        try:
            method = fetch_bytes(url, raw_fp)
            print(f"[OK] saved raw via {method}: {raw_fp}")

            if source_type == "html":
                html = raw_fp.read_text(encoding="utf-8", errors="ignore")
                text = html_to_text(html)

                txt_fp = PROCESSED_DIR / f"{raw_fp.stem}_text.txt"
                txt_fp.write_text(text, encoding="utf-8")
                feature_frames.append(build_page_features(source_key, source_group, url, text))
                save_html_tables(source_key, source_group, url, html, manifest_rows)

            elif source_type == "pdf":
                text = extract_pdf_text(raw_fp)
                txt_fp = PROCESSED_DIR / f"{raw_fp.stem}_text.txt"
                txt_fp.write_text(text, encoding="utf-8")

                feature_frames.append(build_page_features(source_key, source_group, url, text))

                if source_key == "sea_transport_gozo_q4_2025_pdf":
                    dfm = extract_sea_pdf_metrics(text)
                    out_fp = PROCESSED_DIR / "entry_exits_passenger_sea_transport_q4_2025_metrics.csv"
                    dfm.to_csv(out_fp, index=False)
                    pdf_metric_frames.append(dfm)
                    manifest_rows.append({
                        "source_key": source_key,
                        "source_group": source_group,
                        "source_url": url,
                        "parsed_path": str(out_fp),
                        "artifact_type": "pdf_metrics",
                        "rows": len(dfm),
                        "cols": len(dfm.columns),
                        "status": "ok",
                    })

                elif source_key == "cruise_passengers_q4_2025_pdf":
                    dfm = extract_cruise_pdf_metrics(text)
                    out_fp = PROCESSED_DIR / "entry_exits_passenger_cruise_q4_2025_metrics.csv"
                    dfm.to_csv(out_fp, index=False)
                    pdf_metric_frames.append(dfm)
                    manifest_rows.append({
                        "source_key": source_key,
                        "source_group": source_group,
                        "source_url": url,
                        "parsed_path": str(out_fp),
                        "artifact_type": "pdf_metrics",
                        "rows": len(dfm),
                        "cols": len(dfm.columns),
                        "status": "ok",
                    })

        except Exception as e:
            print(f"[FAIL] {source_key}: {e}")
            manifest_rows.append({
                "source_key": source_key,
                "source_group": source_group,
                "source_url": url,
                "parsed_path": "",
                "artifact_type": source_type,
                "rows": None,
                "cols": None,
                "status": f"fetch_fail: {e}",
            })

    manifest = pd.DataFrame(manifest_rows)
    manifest_fp = PROCESSED_DIR / "entry_exits_passenger_manifest.csv"
    manifest.to_csv(manifest_fp, index=False)

    if feature_frames:
        features = pd.concat(feature_frames, ignore_index=True)
        features_fp = PROCESSED_DIR / "entry_exits_passenger_source_features.csv"
        features.to_csv(features_fp, index=False)

    registry = pd.DataFrame([{
        "dataset_name": "entry_exits_passenger",
        "project_scope": "Malta",
        "intended_use": "contextual passenger and service-access layer for daypart decision-support",
        "notes": "airport + sea/ferry + cruise + bus service context; not a direct hourly spa demand measure"
    }])
    registry_fp = PROCESSED_DIR / "entry_exits_passenger_registry.csv"
    registry.to_csv(registry_fp, index=False)

    print("\n=== DONE ===")
    print(manifest.to_string(index=False))
    print(f"\n[OK] manifest: {manifest_fp}")
    print(f"[OK] registry: {registry_fp}")

if __name__ == "__main__":
    main()
