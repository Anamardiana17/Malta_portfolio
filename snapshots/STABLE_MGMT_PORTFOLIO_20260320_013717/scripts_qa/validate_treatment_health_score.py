from pathlib import Path
import pandas as pd

fp = Path("/Users/ambakinanti/Desktop/Malta_portfolio/data_processed/internal_proxy/treatment_health_score.csv")
if not fp.exists():
    raise SystemExit("[FAIL] file not found")

df = pd.read_csv(fp)
required = [
    "treatment_health_id","outlet_id","period_start","period_end","treatment_category",
    "session_duration_min","treatment_health_score_0_100","treatment_health_band","status"
]
missing = [c for c in required if c not in df.columns]
if missing:
    raise SystemExit(f"[FAIL] missing columns: {missing}")

if not df.empty:
    bad = df["treatment_health_score_0_100"].dropna()
    if ((bad < 0) | (bad > 100)).any():
        raise SystemExit("[FAIL] treatment health score out of range")

print("[OK] treatment health score validation passed")
print(f"  - rows checked: {len(df)}")
