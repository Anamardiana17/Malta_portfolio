from __future__ import annotations

import re
from pathlib import Path
import pandas as pd

BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")
INPUT = BASE / "data_processed/spa_research/malta_spa_outlet_portfolio_ready_v3.csv"
OUTPUT = BASE / "data_processed/spa_research/malta_spa_outlet_portfolio_ready_v3_clean.csv"

def norm(x: str) -> str:
    x = "" if pd.isna(x) else str(x)
    x = x.replace("’", "'").replace("–", "-").replace("—", "-")
    x = x.replace("%20", " ")
    x = re.sub(r"\?subject=.*$", "", x)
    x = re.sub(r"\s+", " ", x).strip(" |,;")
    return x

def unique_split_join(text: str, sep: str = " | ") -> str:
    parts = [norm(p) for p in str(text).split("|")]
    out, seen = [], set()
    for p in parts:
        if not p:
            continue
        k = p.lower()
        if k not in seen:
            out.append(p)
            seen.add(k)
    return sep.join(out)

def clean_address(x: str) -> str:
    x = unique_split_join(x)
    parts = [norm(p) for p in x.split(" | ") if norm(p)]

    cleaned = []
    for p in parts:
        lp = p.lower()

        if any(b in lp for b in [
            "copyright", "all rights reserved", "built by", "facebook", "instagram",
            "award-winning chain", "years of excellence", "contact us online form",
            "we will select the best mix", "opening hours", "daily ", "mon -", "monday",
            "toll free", "ihg brands", "over 4000+ reviews"
        ]):
            continue

        is_addressish = (
            bool(re.search(r"\b[a-z]{2,4}\s?\d{3,5}\b", lp)) or
            ("malta" in lp and "," in p) or
            any(k in lp for k in [
                "street", "st.", "st ", "road", "triq", "avenue", "bay",
                "bastion", "valletta", "sliema", "st. julian", "st julian"
            ])
        )

        if is_addressish:
            p = re.split(r"(?i)\b(opening hours|daily \d|kindly note|ihg brands|copyright)\b", p)[0].strip(" ,;|")
            if p:
                cleaned.append(p)

    return " | ".join(dict.fromkeys(cleaned))

def clean_hours(x: str) -> str:
    x = unique_split_join(x)
    parts = [norm(p) for p in x.split(" | ") if norm(p)]

    cleaned = []
    for p in parts:
        lp = p.lower()
        if lp in {"opening hours", "operating hours"}:
            continue
        if re.search(r"(monday|tuesday|wednesday|thursday|friday|saturday|sunday|daily|\d{1,2}:\d{2})", lp):
            p = re.split(r"(?i)\b(kindly note|access to the hotel pool|including public holidays)\b", p)[0].strip(" ,;|")
            cleaned.append(p)

    return " | ".join(dict.fromkeys([c for c in cleaned if c]))

def clean_contact(x: str) -> str:
    x = unique_split_join(x)
    parts = [norm(p) for p in x.split(" | ") if norm(p)]

    cleaned = []
    for p in parts:
        p = p.replace("(", "").replace(")", "").strip()

        if "@" in p:
            p = re.sub(r"\?subject=.*$", "", p).strip()
            if re.match(r"^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$", p, re.I):
                cleaned.append(p)
        else:
            digits_only = re.sub(r"\D", "", p)
            if len(digits_only) >= 7:
                p = re.sub(r"\s+", " ", p)
                cleaned.append(p)

    return " | ".join(dict.fromkeys(cleaned))

def clean_treatments(x: str) -> str:
    x = unique_split_join(x)
    allowed = {
        "massage", "facial", "body treatment", "scrub", "wrap", "deep tissue",
        "swedish", "hot stone", "reflexology", "aromatherapy", "couples massage",
        "manicure", "pedicure"
    }
    parts = [norm(p).lower() for p in x.split(" | ") if norm(p)]
    parts = [p for p in parts if p in allowed]
    return " | ".join(dict.fromkeys(parts))

def clean_facilities(x: str) -> str:
    x = unique_split_join(x)
    allowed = {
        "sauna", "steam room", "steam", "indoor pool", "outdoor pool", "jacuzzi",
        "hammam", "gym", "fitness", "relaxation area", "relaxation room",
        "heated pool", "hydrotherapy", "thermal", "wellness area",
        "massage room", "massage rooms", "treatment room", "treatment rooms",
        "beauty salon", "salt room", "cold plunge",
    }
    parts = [norm(p).lower() for p in x.split(" | ") if norm(p)]
    parts = [p for p in parts if p in allowed]
    return " | ".join(dict.fromkeys(parts))

def main():
    df = pd.read_csv(INPUT).copy()

    if "address_raw" in df.columns:
        df["address_clean"] = df["address_raw"].apply(clean_address)
    if "opening_hours_raw" in df.columns:
        df["opening_hours_clean"] = df["opening_hours_raw"].apply(clean_hours)
    if "contact_raw" in df.columns:
        df["contact_clean"] = df["contact_raw"].apply(clean_contact)
    if "treatment_examples_raw" in df.columns:
        df["treatment_examples_clean"] = df["treatment_examples_raw"].apply(clean_treatments)
    if "facilities_raw" in df.columns:
        df["facilities_clean"] = df["facilities_raw"].apply(clean_facilities)

    preferred = [
        "outlet_name_final",
        "source_status",
        "entity_scope",
        "used_url_validated",
        "address_clean",
        "opening_hours_clean",
        "treatment_examples_clean",
        "price_eur_min",
        "price_eur_max",
        "facilities_clean",
        "contact_clean",
        "notes",
    ]
    preferred = [c for c in preferred if c in df.columns]
    out = df[preferred].copy()

    out.to_csv(OUTPUT, index=False)

    print("saved:", OUTPUT)
    print("shape:", out.shape)
    print(out.to_string(index=False))

if __name__ == "__main__":
    main()
