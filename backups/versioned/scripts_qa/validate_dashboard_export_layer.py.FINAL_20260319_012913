from pathlib import Path
import pandas as pd

BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")

files = {
    "dashboard_exec_overview": BASE / "data_processed/dashboard_export/dashboard_exec_overview.csv",
    "dashboard_market_context": BASE / "data_processed/dashboard_export/dashboard_market_context.csv",
    "dashboard_outlet_control": BASE / "data_processed/dashboard_export/dashboard_outlet_control.csv",
    "dashboard_treatment_opportunity": BASE / "data_processed/dashboard_export/dashboard_treatment_opportunity.csv",
    "dashboard_therapist_coaching": BASE / "data_processed/dashboard_export/dashboard_therapist_coaching.csv",
    "dashboard_manager_action_queue": BASE / "data_processed/dashboard_export/dashboard_manager_action_queue.csv",
}

required = {
    "dashboard_exec_overview": ["period_start", "overall_management_signal_score_0_100"],
    "dashboard_market_context": ["period_start", "market_regime", "external_demand_proxy_index"],
    "dashboard_outlet_control": ["outlet_id", "period_start", "overall_management_signal_score_0_100"],
    "dashboard_treatment_opportunity": ["outlet_id", "period_start", "treatment_category", "treatment_health_score_0_100"],
    "dashboard_therapist_coaching": ["therapist_id", "outlet_id", "period_start", "therapist_consistency_score_0_100"],
    "dashboard_manager_action_queue": ["manager_action_queue_id", "action_priority", "action_scope"],
}

for name, fp in files.items():
    if not fp.exists():
        raise SystemExit(f"[FAIL] missing file: {fp}")
    df = pd.read_csv(fp)
    miss = [c for c in required[name] if c not in df.columns]
    if miss:
        raise SystemExit(f"[FAIL] {name} missing columns: {miss}")
    print(f"[OK] {name} validation passed | rows={len(df)}")
