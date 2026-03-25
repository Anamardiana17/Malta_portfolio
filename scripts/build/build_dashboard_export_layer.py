from pathlib import Path
import pandas as pd
import numpy as np

BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")

IN_OUTLET = BASE / "data_processed/management/outlet_management_summary.csv"
IN_TREAT = BASE / "data_processed/management/treatment_opportunity_summary.csv"
IN_COACH = BASE / "data_processed/management/therapist_coaching_summary.csv"
IN_QUEUE = BASE / "data_processed/management/manager_action_queue.csv"
IN_EXT = BASE / "data_processed/internal_proxy/external_demand_proxy_index.csv"

OUT_EXEC = BASE / "data_processed/dashboard_export/dashboard_exec_overview.csv"
OUT_MARKET = BASE / "data_processed/dashboard_export/dashboard_market_context.csv"
OUT_OUTLET = BASE / "data_processed/dashboard_export/dashboard_outlet_control.csv"
OUT_TREAT = BASE / "data_processed/dashboard_export/dashboard_treatment_opportunity.csv"
OUT_THER = BASE / "data_processed/dashboard_export/dashboard_therapist_coaching.csv"
OUT_QUEUE = BASE / "data_processed/dashboard_export/dashboard_manager_action_queue.csv"

def safe_read(fp: Path) -> pd.DataFrame:
    if not fp.exists() or fp.stat().st_size == 0:
        return pd.DataFrame()
    return pd.read_csv(fp)

def main():
    outlet = safe_read(IN_OUTLET)
    treat = safe_read(IN_TREAT)
    coach = safe_read(IN_COACH)
    queue = safe_read(IN_QUEUE)
    ext = safe_read(IN_EXT)

    if not outlet.empty:
        group_cols = [c for c in [
            "period_start","period_end","year","month","month_id",
            "market_regime","regime_label","event_flag"
        ] if c in outlet.columns]

        agg_map = {
            "total_revenue_eur": "sum",
            "utilization_percent": "mean",
            "yield_eur_per_sold_hour": "mean",
            "revpath_eur_per_available_hour": "mean",
            "overall_management_signal_score_0_100": "mean",
            "high_priority_conflict_count": "sum",
            "burnout_risk_case_count": "sum",
            "leakage_risk_case_count": "sum",
        }
        use_agg = {k: v for k, v in agg_map.items() if k in outlet.columns}

        exec_df = outlet.groupby(group_cols, dropna=False, as_index=False).agg(use_agg)

        if "overall_management_signal_band" in outlet.columns:
            watch_ct = (
                outlet.groupby("period_start")["overall_management_signal_band"]
                .apply(lambda s: s.astype(str).isin(["watchlist", "critical"]).sum())
                .reset_index(name="watchlist_or_worse_outlet_count")
            )
            exec_df = exec_df.merge(watch_ct, on="period_start", how="left")
        else:
            exec_df["watchlist_or_worse_outlet_count"] = np.nan
    else:
        exec_df = pd.DataFrame(columns=[
            "period_start","period_end","overall_management_signal_score_0_100"
        ])

    market_df = ext.copy()
    outlet_df = outlet.copy()
    treat_df = treat.copy()
    coach_df = coach.copy()
    queue_df = queue.copy()

    OUT_EXEC.parent.mkdir(parents=True, exist_ok=True)
    exec_df.to_csv(OUT_EXEC, index=False)
    market_df.to_csv(OUT_MARKET, index=False)
    outlet_df.to_csv(OUT_OUTLET, index=False)
    treat_df.to_csv(OUT_TREAT, index=False)
    coach_df.to_csv(OUT_THER, index=False)
    queue_df.to_csv(OUT_QUEUE, index=False)

    print(f"[OK] saved: {OUT_EXEC} | rows={len(exec_df)}")
    print(f"[OK] saved: {OUT_MARKET} | rows={len(market_df)}")
    print(f"[OK] saved: {OUT_OUTLET} | rows={len(outlet_df)}")
    print(f"[OK] saved: {OUT_TREAT} | rows={len(treat_df)}")
    print(f"[OK] saved: {OUT_THER} | rows={len(coach_df)}")
    print(f"[OK] saved: {OUT_QUEUE} | rows={len(queue_df)}")

if __name__ == "__main__":
    main()
