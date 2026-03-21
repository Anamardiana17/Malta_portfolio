from __future__ import annotations

from pathlib import Path
import pandas as pd
import re

BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")
ENRICHED = BASE / "data_processed/spa_research/malta_spa_outlet_master_manual_enriched_v2.csv"
VALIDATED_MAP = BASE / "data_processed/spa_research/malta_spa_outlet_source_map_validated_v3.csv"

OUT_ALL = BASE / "data_processed/spa_research/malta_spa_outlet_validated_dataset_v3.csv"
OUT_CONFIRMED = BASE / "data_processed/spa_research/malta_spa_outlet_validated_confirmed_only_v3.csv"
OUT_UNRESOLVED = BASE / "data_processed/spa_research/malta_spa_outlet_unresolved_tracker_v3.csv"

def norm(x: str) -> str:
    x = "" if pd.isna(x) else str(x)
    x = x.replace("’", "'").replace("–", "-").replace("—", "-")
    x = re.sub(r"\s+", " ", x).strip()
    return x

def key(x: str) -> str:
    return norm(x).lower()

def main():
    df = pd.read_csv(ENRICHED).copy()
    vm = pd.read_csv(VALIDATED_MAP).copy()

    df["k"] = df["outlet_name_final"].map(key)
    vm["k"] = vm["outlet_name_final"].map(key)

    out = df.merge(
        vm[["k", "official_url", "source_status", "entity_scope", "notes"]],
        on="k",
        how="left"
    )

    # source of truth URL = validated map official_url, not used_url from false positive passes
    out["used_url_validated"] = out["official_url"]

    # wipe extracted fields for unresolved rows
    fields = [
        "used_url",
        "address_raw",
        "opening_hours_raw",
        "treatment_examples_raw",
        "price_eur_raw",
        "price_eur_min",
        "price_eur_max",
        "facilities_raw",
        "contact_raw",
        "matched_source_count",
    ]

    unresolved_mask = out["source_status"].fillna("unresolved").eq("unresolved")
    for c in fields:
        if c in out.columns:
            if c == "matched_source_count":
                out.loc[unresolved_mask, c] = 0
            else:
                out.loc[unresolved_mask, c] = pd.NA

    # also wipe false-positive row that was previously extracted from wrong page
    false_positive_mask = out["outlet_name_final"].eq("Carisma Spa & Wellness")
    for c in fields:
        if c in out.columns:
            if c == "matched_source_count":
                out.loc[false_positive_mask, c] = 0
            else:
                out.loc[false_positive_mask, c] = pd.NA

    keep = [
        "outlet_name_final",
        "source_status",
        "entity_scope",
        "used_url_validated",
        "address_raw",
        "opening_hours_raw",
        "treatment_examples_raw",
        "price_eur_min",
        "price_eur_max",
        "price_eur_raw",
        "facilities_raw",
        "contact_raw",
        "matched_source_count",
        "notes",
    ]
    keep = [c for c in keep if c in out.columns]
    out = out[keep].sort_values(["source_status", "outlet_name_final"]).reset_index(drop=True)

    out.to_csv(OUT_ALL, index=False)

    confirmed = out[out["source_status"].eq("confirmed") | out["source_status"].eq("provisional")].copy()
    confirmed.to_csv(OUT_CONFIRMED, index=False)

    unresolved = out[out["source_status"].eq("unresolved")].copy()
    unresolved.to_csv(OUT_UNRESOLVED, index=False)

    print("saved:", OUT_ALL)
    print("saved:", OUT_CONFIRMED)
    print("saved:", OUT_UNRESOLVED)
    print("\nALL:")
    print(out.to_string(index=False))
    print("\nCONFIRMED_OR_PROVISIONAL:")
    print(confirmed.to_string(index=False))
    print("\nUNRESOLVED:")
    print(unresolved[["outlet_name_final", "source_status", "entity_scope", "notes"]].to_string(index=False))

if __name__ == "__main__":
    main()
