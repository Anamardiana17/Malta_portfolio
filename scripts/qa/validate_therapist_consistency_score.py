from pathlib import Path
import pandas as pd

fp = Path("/Users/ambakinanti/Desktop/Malta_portfolio/data_processed/internal_proxy/therapist_consistency_score.csv")
if not fp.exists():
    raise SystemExit("[FAIL] file not found")

df = pd.read_csv(fp)
required = [
    "therapist_consistency_id","therapist_id","outlet_id","period_start","period_end",
    "therapist_consistency_score_0_100","therapist_consistency_band","status"
]
missing = [c for c in required if c not in df.columns]
if missing:
    raise SystemExit(f"[FAIL] missing columns: {missing}")

if not df.empty:
    bad = df["therapist_consistency_score_0_100"].dropna()
    if ((bad < 0) | (bad > 100)).any():
        raise SystemExit("[FAIL] therapist consistency score out of range")

print("[OK] therapist consistency score validation passed")
print(f"  - rows checked: {len(df)}")
