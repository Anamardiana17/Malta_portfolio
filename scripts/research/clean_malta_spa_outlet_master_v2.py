from __future__ import annotations

import re
from pathlib import Path
import pandas as pd

BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")
INPUT = BASE / "data_processed/spa_research/malta_spa_outlet_master_clean.csv"
OUTPUT = BASE / "data_processed/spa_research/malta_spa_outlet_master_clean_v2.csv"

GENERIC_BAD_EXACT = {
    "malta",
    "st julian's", "st julians", "saint julian's", "saint julians",
    "st. julian's", "st. julians",
    "st paul's bay", "st pauls bay", "st. paul's bay", "st. pauls bay",
    "sliema", "gozo", "valletta", "floriana", "mellieha", "qawra",
    "bugibba", "swieqi", "san giljan", "san gwann", "saint julian's malta",
    "portomaso", "saint julian's malta", "st pauls bay", "st. pauls bay",
}

GENERIC_BAD_CONTAINS = [
    "years of excellence",
    "contact us", "book now", "view menu", "learn more",
    "our location", "our locations", "opening hours", "business hours",
    "follow us", "call us", "email us", "get in touch",
]

ADDRESS_HINTS = [
    "road", "street", "st ", "st.", "triq", "avenue", "ave", "bay",
    "coast", "square", "sq", "level", "floor", "block", "tower",
    "centre", "center", "promenade", "waterfront", "gardens",
    "frn", "spb", "slm", "sli", "mlh", "vlt", "pwl", "qrw",
]

VALID_NAME_HINTS = [
    "spa", "wellness", "resort", "hilton", "hyatt", "marriott",
    "radisson", "novotel", "intercontinental", "doubletree",
    "excelsior", "ramla", "salini", "riviera", "odycy", "carisma",
    "essensi", "hotel", "hugo", "iniala",
]

AREA_ONLY_PATTERNS = [
    r"^[a-z'. -]+,\s*malta$",
    r"^[a-z'. -]+,\s*st\.?\s*pauls?\s*bay$",
    r"^[a-z'. -]+,\s*saint\s*julians?$",
    r"^[a-z'. -]+,\s*st\.?\s*julians?$",
    r"^portomaso,\s*saint\s*julians?,\s*malta$",
    r"^qawra,\s*st\.?\s*pauls?\s*bay$",
    r"^st\.?\s*pauls?\s*bay,\s*malta$",
]

def norm_text(x: str) -> str:
    x = "" if pd.isna(x) else str(x)
    x = x.replace("’", "'").replace("–", "-").replace("—", "-")
    x = re.sub(r"\s+", " ", x).strip(" ,;-")
    return x

def simple_norm(x: str) -> str:
    x = norm_text(x).lower()
    x = re.sub(r"[^a-z0-9\s&'/.,-]", " ", x)
    x = re.sub(r"\s+", " ", x).strip()
    return x

def looks_like_address(text: str) -> bool:
    t = simple_norm(text)
    if not t:
        return True

    has_digit = bool(re.search(r"\d", t))
    address_hint_hit = any(h in t for h in ADDRESS_HINTS)
    comma_count = t.count(",")

    if has_digit and address_hint_hit:
        return True
    if comma_count >= 2 and address_hint_hit:
        return True
    if re.search(r"\b[a-z]{2,4}\s?\d{3,5}\b", t):
        return True
    if re.search(r"^\d+\s+[a-z]", t):
        return True

    return False

def looks_area_only(text: str) -> bool:
    t = simple_norm(text)
    if t in GENERIC_BAD_EXACT:
        return True
    for p in AREA_ONLY_PATTERNS:
        if re.fullmatch(p, t):
            return True
    return False

def looks_generic_bad(text: str) -> bool:
    t = simple_norm(text)
    if not t:
        return True
    if t in GENERIC_BAD_EXACT:
        return True
    if any(bad in t for bad in GENERIC_BAD_CONTAINS):
        return True
    return False

def looks_like_valid_outlet_name(text: str) -> bool:
    t = simple_norm(text)
    if not t:
        return False

    if looks_generic_bad(t) or looks_area_only(t):
        return False

    if looks_like_address(t) and not any(h in t for h in VALID_NAME_HINTS):
        return False

    token_count = len(t.split())
    valid_hint = any(h in t for h in VALID_NAME_HINTS)

    if token_count <= 3 and not valid_hint:
        return False

    # must contain at least one letter
    if not re.search(r"[a-z]", t):
        return False

    return True

def choose_best_name(row: pd.Series) -> str:
    candidates = []
    for col in row.index:
        if any(k in col.lower() for k in ["outlet_name_final", "outlet_name", "name", "title"]):
            val = norm_text(row[col])
            if val:
                candidates.append((col, val))

    priority = []
    for col, val in candidates:
        lc = col.lower()
        rank = 99
        if lc == "outlet_name_final":
            rank = 1
        elif lc == "outlet_name":
            rank = 2
        elif "name" in lc:
            rank = 3
        elif "title" in lc:
            rank = 4
        priority.append((rank, col, val))

    priority.sort(key=lambda x: x[0])

    for _, _, val in priority:
        if looks_like_valid_outlet_name(val):
            return val

    return priority[0][2] if priority else ""

def dedupe_key(text: str) -> str:
    t = simple_norm(text)
    t = re.sub(r"\b(spa|wellness|resort|hotel|malta|by appointment only)\b", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t

def main():
    if not INPUT.exists():
        raise FileNotFoundError(f"Missing input: {INPUT}")

    df = pd.read_csv(INPUT).copy()
    original_rows = len(df)

    if "outlet_name_final" not in df.columns:
        df["outlet_name_final"] = ""

    df["outlet_name_final_v2"] = df.apply(choose_best_name, axis=1)
    df["name_valid_flag"] = df["outlet_name_final_v2"].apply(looks_like_valid_outlet_name)
    df["name_address_flag"] = df["outlet_name_final_v2"].apply(looks_like_address)
    df["name_generic_flag"] = df["outlet_name_final_v2"].apply(looks_generic_bad)
    df["name_area_only_flag"] = df["outlet_name_final_v2"].apply(looks_area_only)

    cleaned = df[
        (df["name_valid_flag"])
        & (~df["name_area_only_flag"])
        & (~df["name_generic_flag"])
    ].copy()

    cleaned["outlet_name_final"] = cleaned["outlet_name_final_v2"].map(norm_text)
    cleaned["dedupe_key"] = cleaned["outlet_name_final"].map(dedupe_key)

    cleaned = cleaned.sort_values(["outlet_name_final"]).drop_duplicates(
        subset=["dedupe_key"], keep="first"
    )

    cleaned = cleaned.sort_values("outlet_name_final").reset_index(drop=True)

    front = [
        c for c in [
            "outlet_name_final",
            "location_text",
            "rating",
            "review_count",
            "source_name",
            "source_url",
            "outlet_name_final_v2",
            "name_valid_flag",
            "name_address_flag",
            "name_generic_flag",
            "name_area_only_flag",
            "dedupe_key",
        ] if c in cleaned.columns
    ]
    other = [c for c in cleaned.columns if c not in front]
    cleaned = cleaned[front + other]

    cleaned.to_csv(OUTPUT, index=False)

    print(f"input rows  : {original_rows}")
    print(f"output rows : {len(cleaned)}")
    print(f"saved       : {OUTPUT}")
    print(cleaned[["outlet_name_final"]].to_string(index=False))

if __name__ == "__main__":
    main()
