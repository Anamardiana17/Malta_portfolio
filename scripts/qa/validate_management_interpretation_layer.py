from pathlib import Path
import pandas as pd

BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")
FP = BASE / "data_processed/management_interpretation/management_interpretation_layer.csv"


def main():
    if not FP.exists():
        raise FileNotFoundError(f"Missing file: {FP}")

    df = pd.read_csv(FP)

    required = [
        "outlet_id",
        "month_id",
        "story_label",
        "growth_ready_flag",
        "leakage_risk_flag",
        "team_strain_risk_flag",
        "management_priority_rank",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    print("=== BASIC CHECK ===")
    print("rows:", len(df))
    print("cols:", len(df.columns))

    print("\n=== NULL CHECK ===")
    print(df[required].isna().sum().to_string())

    print("\n=== STORY DISTRIBUTION ===")
    print(df["story_label"].value_counts(dropna=False).to_string())

    print("\n=== FLAG CONFLICTS ===")
    both_growth_and_strain = df[
        (df["growth_ready_flag"] == 1) &
        (df["team_strain_risk_flag"] == 1)
    ]
    print("growth_ready + team_strain:", len(both_growth_and_strain))

    both_growth_and_leakage = df[
        (df["growth_ready_flag"] == 1) &
        (df["leakage_risk_flag"] == 1)
    ]
    print("growth_ready + leakage:", len(both_growth_and_leakage))

    print("\n=== PRIORITY CHECK ===")
    print(df["management_priority_rank"].value_counts(dropna=False).sort_index().to_string())

    if df["story_label"].isna().any():
        raise ValueError("story_label contains nulls")

    print("\n[OK] interpretation QA passed")


if __name__ == "__main__":
    main()
