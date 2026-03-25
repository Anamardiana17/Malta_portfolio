from pathlib import Path
import pandas as pd

BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")

files = {
    "outlet_management_summary": BASE / "data_processed/management/outlet_management_summary.csv",
    "treatment_opportunity_summary": BASE / "data_processed/management/treatment_opportunity_summary.csv",
    "therapist_coaching_summary": BASE / "data_processed/management/therapist_coaching_summary.csv",
    "manager_action_queue": BASE / "data_processed/management/manager_action_queue.csv",
}

required = {
    "outlet_management_summary": ["outlet_id","period_start","overall_management_signal_score_0_100"],
    "treatment_opportunity_summary": ["outlet_id","period_start","treatment_category","treatment_health_score_0_100"],
    "therapist_coaching_summary": ["therapist_id","outlet_id","period_start","therapist_consistency_score_0_100"],
    "manager_action_queue": ["manager_action_queue_id","action_scope","action_priority","outlet_id"],
}

for name, fp in files.items():
    if not fp.exists():
        raise SystemExit(f"[FAIL] missing file: {fp}")
    df = pd.read_csv(fp)
    miss = [c for c in required[name] if c not in df.columns]
    if miss:
        raise SystemExit(f"[FAIL] {name} missing columns: {miss}")
    print(f"[OK] {name} validation passed | rows={len(df)}")
