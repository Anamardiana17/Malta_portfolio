from __future__ import annotations

from pathlib import Path
import pandas as pd


BASE = Path(__file__).resolve().parents[2]
CSV_PATH = BASE / "data_processed" / "management" / "outlet_executive_ranking_layer.csv"


def fail(msg: str) -> None:
    raise SystemExit(f"[FAIL] {msg}")


def main() -> None:
    if not CSV_PATH.exists():
        fail(f"Missing file: {CSV_PATH}")

    df = pd.read_csv(CSV_PATH)
    print(f"[OK] Loaded: {CSV_PATH}")
    print(f"[INFO] shape={df.shape}")

    required = [
        "executive_ranking_id",
        "month_id",
        "outlet_id",
        "outlet_name",
        "people_readiness_score_0_100",
        "commercial_execution_score_0_100",
        "training_pressure_score_0_100",
        "reward_readiness_score_0_100",
        "executive_priority_score_0_100",
        "executive_priority_band",
        "executive_rank_within_month",
        "executive_management_recommendation",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        fail(f"Missing columns: {missing}")
    print("[OK] Required columns present")

    if df.empty:
        fail("Dataset is empty")
    print("[OK] Dataset is non-empty")

    for col in [
        "people_readiness_score_0_100",
        "commercial_execution_score_0_100",
        "training_pressure_score_0_100",
        "reward_readiness_score_0_100",
        "executive_priority_score_0_100",
    ]:
        s = pd.to_numeric(df[col], errors="coerce")
        if s.isna().any():
            fail(f"{col} contains null/non-numeric values")
        if ((s < 0) | (s > 100)).any():
            fail(f"{col} must stay within 0-100")
    print("[OK] Score columns valid")

    valid_bands = {"critical", "high", "watchlist", "stable"}
    if not set(df["executive_priority_band"].astype(str)).issubset(valid_bands):
        fail("executive_priority_band contains unexpected values")
    print("[OK] executive_priority_band valid")

    if df["executive_management_recommendation"].astype(str).str.strip().eq("").any():
        fail("executive_management_recommendation contains blanks")
    print("[OK] Recommendation text populated")

    print("\n=== EXECUTIVE PRIORITY BAND COUNTS ===")
    print(df["executive_priority_band"].value_counts().to_string())

    print("\n=== TOP 10 HIGHEST PRIORITY OUTLET-MONTHS ===")
    print(
        df.sort_values("executive_priority_score_0_100", ascending=False)[[
            "month_id",
            "outlet_name",
            "people_readiness_score_0_100",
            "commercial_execution_score_0_100",
            "training_pressure_score_0_100",
            "reward_readiness_score_0_100",
            "executive_priority_score_0_100",
            "executive_priority_band",
        ]].head(10).to_string(index=False)
    )

    print("\n[OK] outlet_executive_ranking_layer validation passed")


if __name__ == "__main__":
    main()
