from pathlib import Path
import pandas as pd

fp = Path("/Users/ambakinanti/Desktop/Malta_portfolio/data_processed/internal_proxy/conflict_resolution_layer.csv")
if not fp.exists():
    raise SystemExit("[FAIL] file not found")

df = pd.read_csv(fp)
required = [
    "conflict_resolution_id","outlet_id","period_start","period_end",
    "conflict_case_flag","conflict_pattern_code","status"
]
missing = [c for c in required if c not in df.columns]
if missing:
    raise SystemExit(f"[FAIL] missing columns: {missing}")

allowed_flag = {"yes","no"}
if not df.empty and not set(df["conflict_case_flag"].dropna().astype(str)).issubset(allowed_flag):
    raise SystemExit("[FAIL] invalid conflict_case_flag values")

print("[OK] conflict resolution layer validation passed")
print(f"  - rows checked: {len(df)}")
