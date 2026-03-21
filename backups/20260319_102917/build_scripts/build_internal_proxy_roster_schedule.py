from __future__ import annotations
import hashlib
import numpy as np
import pandas as pd
from pathlib import Path

BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")
INTERNAL = BASE / "data_processed" / "internal_proxy"
REFERENCE = BASE / "data_processed" / "reference"

OUTFILE = INTERNAL / "internal_proxy_roster_schedule.csv"

def safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required file: {path}")
    return pd.read_csv(path)

def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.strip() for c in df.columns]
    return df

def get_col(df: pd.DataFrame, candidates: list[str], required: bool = True):
    for c in candidates:
        if c in df.columns:
            return c
    if required:
        raise KeyError(f"Missing required column. Tried: {candidates}")
    return None

def stable_noise(*parts: str, low: float = -1.0, high: float = 1.0) -> float:
    seed = "|".join(str(p) for p in parts)
    digest = hashlib.md5(seed.encode("utf-8")).hexdigest()
    val = int(digest[:8], 16) / 0xFFFFFFFF
    return low + (high - low) * val

def load_policy() -> dict:
    pol = safe_read_csv(REFERENCE / "roster_policy_reference.csv")
    pol = normalize_cols(pol)
    out = {}
    for _, r in pol.iterrows():
        out[str(r["policy_title"]).strip()] = float(r["parameter_value"])
    return out

def month_seasonality(month_num: int) -> float:
    mapping = {
        1: 0.92, 2: 0.93, 3: 0.98, 4: 1.00,
        5: 1.04, 6: 1.08, 7: 1.12, 8: 1.13,
        9: 1.07, 10: 1.01, 11: 0.96, 12: 0.97,
    }
    return mapping.get(int(month_num), 1.0)

def main():
    policy = load_policy()

    therapist = normalize_cols(safe_read_csv(INTERNAL / "internal_proxy_therapist_kpi.csv"))
    demand = normalize_cols(safe_read_csv(INTERNAL / "external_demand_proxy_index.csv"))

    th_date = get_col(therapist, ["period_start", "period_date", "date"])
    th_outlet = get_col(therapist, ["outlet_id"])
    th_therapist = get_col(therapist, ["therapist_id"])
    th_booked = get_col(therapist, ["booked_hours", "therapist_hours_sold", "hours_sold"], required=False)
    th_util = get_col(therapist, ["utilization_ratio", "utilization", "utilization_rate"], required=False)

    d_date = get_col(demand, ["period_start", "period_date", "date"])
    d_outlet = get_col(demand, ["outlet_id"], required=False)
    d_index = get_col(demand, ["external_demand_proxy_index", "demand_proxy_index"], required=False)

    therapist[th_date] = pd.to_datetime(therapist[th_date])
    demand[d_date] = pd.to_datetime(demand[d_date])

    df = therapist.copy()
    df = df.rename(columns={th_date: "period_date", th_outlet: "outlet_id", th_therapist: "therapist_id"})
    df["period_date"] = pd.to_datetime(df["period_date"])

    if th_booked and th_booked in df.columns:
        df["booked_hours_base"] = pd.to_numeric(df[th_booked], errors="coerce").fillna(0)
    elif th_util:
        util = pd.to_numeric(df[th_util], errors="coerce").fillna(0).clip(lower=0, upper=1)
        df["booked_hours_base"] = util * 110
    else:
        df["booked_hours_base"] = 80.0

    demand_small = demand[[d_date] + ([d_outlet] if d_outlet else []) + ([d_index] if d_index else [])].copy()
    demand_small = demand_small.rename(columns={d_date: "period_date"})
    if d_outlet:
        demand_small = demand_small.rename(columns={d_outlet: "outlet_id"})
    if d_index:
        demand_small = demand_small.rename(columns={d_index: "external_demand_proxy_index"})
    else:
        demand_small["external_demand_proxy_index"] = 100.0

    if "outlet_id" in demand_small.columns:
        df = df.merge(demand_small, on=["period_date", "outlet_id"], how="left")
    else:
        df = df.merge(demand_small[["period_date", "external_demand_proxy_index"]], on="period_date", how="left")

    df["external_demand_proxy_index"] = pd.to_numeric(df["external_demand_proxy_index"], errors="coerce").fillna(100.0)

    productive_factor = policy["productive_capacity_factor"]
    safe_daily = policy["max_safe_daily_scheduled_hours"]
    max_consec_safe = policy["max_safe_consecutive_workdays"]
    min_gap = policy["minimum_recovery_gap_hours"]

    rows = []
    for _, r in df.iterrows():
        period_date = pd.Timestamp(r["period_date"])
        outlet_id = str(r["outlet_id"])
        therapist_id = str(r["therapist_id"])

        booked = float(r["booked_hours_base"])
        demand_idx_raw = float(r["external_demand_proxy_index"])
        demand_idx = demand_idx_raw if demand_idx_raw > 3 else demand_idx_raw * 100.0
        demand_factor = np.clip(demand_idx / 100.0, 0.72, 1.30)

        month_factor = month_seasonality(period_date.month)
        outlet_bias = stable_noise(outlet_id, low=-0.06, high=0.06)
        therapist_eff = stable_noise(therapist_id, low=-0.10, high=0.10)
        therapist_reliability = stable_noise("reliability", therapist_id, low=-0.10, high=0.10)
        month_noise = stable_noise(str(period_date.date()), outlet_id, therapist_id, low=-0.07, high=0.07)

        peak_flag = 1 if (demand_factor >= 1.05 or month_factor >= 1.07) else 0
        strain_flag = 1 if stable_noise("strain", outlet_id, therapist_id, str(period_date.date()), low=0, high=1) < (0.18 + peak_flag * 0.16) else 0

        # Lower target utilization headroom vs prior version so some rows get tight
        target_util = (
            0.72
            + (demand_factor - 0.95) * 0.75
            + (month_factor - 1.00) * 0.30
            + outlet_bias
            + therapist_eff * 0.20
            + month_noise
            + strain_flag * 0.08
        )
        target_util = float(np.clip(target_util, 0.60, 1.02))

        productive_capacity = booked / max(target_util, 0.01)
        scheduled_hours = productive_capacity / productive_factor

        # Strain rows get tighter staffing
        if strain_flag == 1:
            scheduled_hours *= 0.93

        scheduled_hours = float(np.clip(scheduled_hours, 58, 205))

        shift_len = 7.3 + stable_noise("shiftlen", therapist_id, low=-0.8, high=0.7)
        worked_days = int(np.clip(round(scheduled_hours / shift_len), 9, 27))

        # Tight rows compress day-off pattern
        if strain_flag == 1:
            worked_days = min(27, worked_days + 1)

        day_off_days = int(np.clip(30 - worked_days, 3, 12))

        productive_capacity = scheduled_hours * productive_factor
        idle_hours = max(productive_capacity - booked, 0)

        avg_daily_scheduled_hours = scheduled_hours / worked_days

        # Overtime can happen in tight / dense cases
        overtime_hours = 0.0
        if avg_daily_scheduled_hours > safe_daily:
            overtime_hours += (avg_daily_scheduled_hours - safe_daily) * worked_days
        if strain_flag == 1 and demand_factor >= 1.02:
            overtime_hours += stable_noise("ot", therapist_id, str(period_date.date()), low=1.5, high=8.5)
        overtime_hours = float(np.clip(overtime_hours, 0, 28))

        density_ratio = booked / max(productive_capacity, 0.01)
        density_ratio = float(np.clip(density_ratio, 0.50, 1.06))

        split_shift_prob = (
            0.10
            + max(0, 0.64 - density_ratio) * 0.40
            + max(0, density_ratio - 0.92) * 0.38
            + max(0, -therapist_reliability) * 0.18
            + strain_flag * 0.08
        )
        split_shift_flag = 1 if stable_noise("split", str(period_date.date()), therapist_id, low=0, high=1) < split_shift_prob else 0

        coverage_gap_flag = 1 if (
            density_ratio >= 0.95
            or (strain_flag == 1 and density_ratio >= 0.90)
            or overtime_hours >= 8
        ) else 0

        recovery_gap_hours = (
            min_gap
            + stable_noise("recovery", therapist_id, str(period_date.date()), low=-0.22, high=0.20)
            - max(0, density_ratio - 0.84) * 0.55
            - (overtime_hours / 20.0) * 0.12
            - strain_flag * 0.05
        )
        recovery_gap_hours = float(np.clip(recovery_gap_hours, 0.30, 1.05))

        break_hours = worked_days * (
            0.78
            + stable_noise("break", therapist_id, outlet_id, low=-0.12, high=0.12)
            - max(0, density_ratio - 0.90) * 0.15
            - strain_flag * 0.05
        )
        break_hours = float(np.clip(break_hours, worked_days * 0.50, worked_days * 0.98))

        consecutive_workdays = int(np.clip(
            round(
                4.0
                + (demand_factor - 0.92) * 3.4
                + (month_factor - 1.0) * 2.0
                + max(0, density_ratio - 0.84) * 8
                + strain_flag * 1.2
                + stable_noise("consec", therapist_id, str(period_date.date()), low=-1.0, high=1.2)
            ),
            3, 8
        ))

        schedule_stability = (
            89
            + therapist_reliability * 14
            + stable_noise("stability", therapist_id, outlet_id, str(period_date.date()), low=-8, high=6)
            - split_shift_flag * 10
            - overtime_hours * 1.25
            - max(0, density_ratio - 0.88) * 42
            - max(0, consecutive_workdays - max_consec_safe) * 6
            - max(0, 6 - day_off_days) * 3.0
            - strain_flag * 6
        )
        schedule_stability = float(np.clip(schedule_stability, 32, 97))

        burnout_exposure_flag = 1 if (
            overtime_hours >= 6
            or density_ratio >= 0.94
            or consecutive_workdays >= 7
            or recovery_gap_hours <= 0.42
            or (strain_flag == 1 and schedule_stability <= 70)
        ) else 0

        rows.append({
            "period_date": period_date,
            "outlet_id": outlet_id,
            "therapist_id": therapist_id,
            "scheduled_hours": round(scheduled_hours, 4),
            "booked_hours": round(booked, 4),
            "productive_capacity_hours": round(productive_capacity, 4),
            "idle_hours": round(idle_hours, 4),
            "break_hours": round(break_hours, 4),
            "recovery_gap_hours": round(recovery_gap_hours, 4),
            "overtime_hours": round(overtime_hours, 4),
            "split_shift_flag": int(split_shift_flag),
            "coverage_gap_flag": int(coverage_gap_flag),
            "day_off_flag": 0,
            "worked_flag": 1 if worked_days > 0 else 0,
            "day_off_days": int(day_off_days),
            "worked_days": int(worked_days),
            "schedule_stability_score_0_100": round(schedule_stability, 4),
            "workload_density_ratio": round(density_ratio, 6),
            "consecutive_workdays": int(consecutive_workdays),
            "burnout_exposure_flag": int(burnout_exposure_flag),
            "external_demand_proxy_index": round(demand_idx, 3),
        })

    out = pd.DataFrame(rows).sort_values(["period_date", "outlet_id", "therapist_id"]).reset_index(drop=True)
    out.to_csv(OUTFILE, index=False)

    print(f"[OK] saved: {OUTFILE}")
    print(f"[OK] rows: {len(out)}")
    print(out.head(10).to_string(index=False))

if __name__ == "__main__":
    main()
