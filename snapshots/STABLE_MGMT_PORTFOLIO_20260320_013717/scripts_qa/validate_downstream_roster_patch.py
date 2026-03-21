from __future__ import annotations
import pandas as pd
from pathlib import Path

BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")
INTERNAL = BASE / "data_processed" / "internal_proxy"

def must_exist(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    print(f"[OK] exists: {path.name}")

def need_cols(df: pd.DataFrame, cols: list[str], name: str):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise AssertionError(f"{name} missing columns: {missing}")
    print(f"[OK] {name} has required roster patch columns")

def main():
    therapist = INTERNAL / "therapist_consistency_score.csv"
    conflict = INTERNAL / "conflict_resolution_layer.csv"
    signal = INTERNAL / "management_kpi_signal_layer.csv"
    treatment = INTERNAL / "treatment_health_score.csv"

    for p in [therapist, conflict, signal, treatment]:
        must_exist(p)

    therapist_df = pd.read_csv(therapist)
    conflict_df = pd.read_csv(conflict)
    signal_df = pd.read_csv(signal)
    treatment_df = pd.read_csv(treatment)

    need_cols(therapist_df, [
        "therapist_consistency_score_0_100_original",
        "therapist_consistency_score_0_100",
        "roster_integrity_score_0_100",
        "burnout_risk_score_0_100",
        "sustainability_penalty_points",
        "sustainability_adjustment_flag",
        "therapist_consistency_note"
    ], "therapist_consistency_score")

    need_cols(conflict_df, [
        "roster_conflict_type",
        "roster_conflict_severity",
        "roster_conflict_note"
    ], "conflict_resolution_layer")

    need_cols(signal_df, [
        "operating_sustainability_signal",
        "sustainability_reality_flag",
        "managerial_interpretation_roster"
    ], "management_kpi_signal_layer")

    need_cols(treatment_df, [
        "treatment_health_score_0_100_original",
        "treatment_health_score_0_100",
        "capacity_strain_penalty_points",
        "treatment_health_capacity_note"
    ], "treatment_health_score")

    print("\n[PASS] downstream roster patch QA passed")

if __name__ == "__main__":
    main()
