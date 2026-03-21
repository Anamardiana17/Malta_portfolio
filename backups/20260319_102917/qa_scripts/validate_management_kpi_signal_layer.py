from pathlib import Path
import pandas as pd

fp = Path("/Users/ambakinanti/Desktop/Malta_portfolio/data_processed/internal_proxy/management_kpi_signal_layer.csv")
if not fp.exists():
    raise SystemExit("[FAIL] file not found")

df = pd.read_csv(fp)
required = [
    "management_signal_id","outlet_id","period_start","period_end",
    "overall_management_signal_score_0_100","overall_management_signal_band","status"
]
missing = [c for c in required if c not in df.columns]
if missing:
    raise SystemExit(f"[FAIL] missing columns: {missing}")

if not df.empty:
    bad = df["overall_management_signal_score_0_100"].dropna()
    if ((bad < 0) | (bad > 100)).any():
        raise SystemExit("[FAIL] overall management signal score out of range")

print("[OK] management KPI signal layer validation passed")
print(f"  - rows checked: {len(df)}")
