from pathlib import Path
import pandas as pd
import numpy as np

BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")

PRICING_MASTER_FP = BASE / "data_processed/pricing_research/final_treatment_pricing_master.csv"
TREATMENT_KPI_FP = BASE / "data_processed/internal_proxy/internal_proxy_treatment_kpi.csv"
THERAPIST_KPI_FP = BASE / "data_processed/internal_proxy/internal_proxy_therapist_kpi.csv"
EXTERNAL_PROXY_FP = BASE / "data_processed/internal_proxy/external_demand_proxy_index.csv"
CAPACITY_PROXY_FP = BASE / "data_processed/internal_proxy/outlet_capacity_proxy.csv"
MARKET_REGIME_REF_FP = BASE / "data_processed/reference/market_regime_reference.csv"

OUTLETS = [
    ("MLT_OUTLET_001", "Valletta", 1.00, "premium_core"),
    ("MLT_OUTLET_002", "Sliema",   1.08, "premium_plus"),
    ("MLT_OUTLET_003", "St Julians", 0.94, "balanced_commercial"),
]

THERAPISTS = [
    ("TH_001", "Therapist A", "MLT_OUTLET_001", 1.00),
    ("TH_002", "Therapist B", "MLT_OUTLET_001", 0.95),
    ("TH_003", "Therapist C", "MLT_OUTLET_002", 1.08),
    ("TH_004", "Therapist D", "MLT_OUTLET_002", 0.90),
    ("TH_005", "Therapist E", "MLT_OUTLET_003", 1.03),
    ("TH_006", "Therapist F", "MLT_OUTLET_003", 0.92),
]

TREATMENT_SENSITIVITY = {
    "aromatherapy":   {"demand_sensitivity": 0.95, "yield_strength": 0.98, "recovery_speed": 1.00},
    "body_treatment": {"demand_sensitivity": 1.00, "yield_strength": 1.00, "recovery_speed": 1.00},
    "deep_tissue":    {"demand_sensitivity": 0.92, "yield_strength": 1.02, "recovery_speed": 1.03},
    "facial":         {"demand_sensitivity": 1.05, "yield_strength": 1.08, "recovery_speed": 0.98},
    "hot_stone":      {"demand_sensitivity": 1.10, "yield_strength": 1.03, "recovery_speed": 0.92},
    "wrap":           {"demand_sensitivity": 1.18, "yield_strength": 1.00, "recovery_speed": 0.90},
}

def normalize_treatment(df: pd.DataFrame) -> pd.DataFrame:
    for c in ["treatment_category", "treatment_variant"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip().str.lower()
    if "session_duration_min" in df.columns:
        df["session_duration_min"] = pd.to_numeric(df["session_duration_min"], errors="coerce")
    return df

def get_seasonality(month: int) -> float:
    return {
        1: 0.92, 2: 0.94, 3: 0.98, 4: 1.00,
        5: 1.03, 6: 1.07, 7: 1.12, 8: 1.15,
        9: 1.06, 10: 1.01, 11: 0.96, 12: 1.10
    }[month]

def get_market_regime(year: int, month: int):
    if year in [2017, 2018, 2019]:
        return "pre_covid_growth", 0.96 + ((year - 2017) * 0.03)
    if year == 2020 and month in [1, 2]:
        return "pre_shock_softening", 0.90
    if year == 2020 and month in [3, 4, 5, 6]:
        return "covid_lockdown", 0.28
    if year == 2020 and month in [7, 8, 9, 10, 11, 12]:
        return "covid_partial_reopen", 0.48
    if year == 2021:
        return "recovery_constrained", 0.68
    if year == 2022:
        return "rebound_growth", 0.90
    if year == 2023:
        return "macro_stress_normalization", 0.96
    if year == 2024:
        return "stable_recovery", 1.03
    if year == 2025:
        return "stable_mature_market", 1.06
    return "normal", 1.00

def build_periods():
    periods = pd.date_range("2017-01-01", "2025-12-01", freq="MS")
    rows = []
    for dt in periods:
        year = dt.year
        month = dt.month

        regime, regime_base = get_market_regime(year, month)
        seasonality = get_seasonality(month)

        geopolitical_modifier = 1.00
        if year in [2022, 2023]:
            geopolitical_modifier = 0.97

        inflation_cost_pressure = 1.00
        if year in [2022, 2023]:
            inflation_cost_pressure = 0.98

        external_demand_proxy_index = round(regime_base * seasonality * geopolitical_modifier, 3)

        external_stress_flag = "yes" if (
            regime in ["covid_lockdown", "covid_partial_reopen", "recovery_constrained"]
            or external_demand_proxy_index < 0.90
        ) else "no"

        period_start = dt.strftime("%Y-%m-%d")
        period_end = (dt + pd.offsets.MonthEnd(1)).strftime("%Y-%m-%d")

        rows.append({
            "market_context": "Malta",
            "period_type": "monthly",
            "period_start": period_start,
            "period_end": period_end,
            "year": year,
            "month": month,
            "month_id": dt.strftime("%Y-%m"),
            "rolling_window_weeks": 12,
            "market_regime": regime,
            "seasonality_factor": round(seasonality, 3),
            "regime_base_factor": round(regime_base, 3),
            "geopolitical_modifier": round(geopolitical_modifier, 3),
            "inflation_cost_pressure_factor": round(inflation_cost_pressure, 3),
            "external_demand_proxy_index": external_demand_proxy_index,
            "external_stress_flag": external_stress_flag,
        })
    return pd.DataFrame(rows)


def load_market_regime_reference():
    ref = pd.read_csv(MARKET_REGIME_REF_FP)
    return ref

def main():
    np.random.seed(42)

    pricing = pd.read_csv(PRICING_MASTER_FP)
    pricing = normalize_treatment(pricing)

    keep = [c for c in [
        "treatment_category",
        "treatment_variant",
        "session_duration_min",
        "pricing_position",
        "recommended_sell_price_eur",
        "market_price_median_eur",
        "commercial_market_price_median_eur",
        "launch_recommendation_flag",
    ] if c in pricing.columns]

    treatment_ref = pricing[keep].copy()
    treatment_ref = treatment_ref.dropna(subset=["treatment_category", "session_duration_min"]).drop_duplicates()

    periods = build_periods()
    regime_ref = load_market_regime_reference()
    periods = periods.merge(regime_ref, on="market_regime", how="left")

    # ======================================================
    # TREATMENT KPI
    # ======================================================
    tr_rows = []

    for outlet_id, outlet_name, outlet_factor, outlet_position in OUTLETS:
        for _, p in periods.iterrows():
            demand_env = float(p["external_demand_proxy_index"])
            regime = p["market_regime"]

            for _, t in treatment_ref.iterrows():
                cat = str(t["treatment_category"])
                dur = float(t["session_duration_min"])

                sens = TREATMENT_SENSITIVITY.get(
                    cat,
                    {"demand_sensitivity": 1.00, "yield_strength": 1.00, "recovery_speed": 1.00}
                )

                duration_load = 1.00 if dur <= 60 else 0.82
                demand_factor = demand_env / sens["demand_sensitivity"] * sens["recovery_speed"]

                base_monthly_bookings = 48 if dur <= 60 else 28
                bookings = max(3, int(round(base_monthly_bookings * outlet_factor * demand_factor * duration_load)))

                sold_hours = round(bookings * (dur / 60.0), 2)
                hours_available = round(max(sold_hours * 1.35, sold_hours + 28), 2)
                utilization = round((sold_hours / hours_available) * 100, 2) if hours_available > 0 else 0

                rec_price = float(t.get("recommended_sell_price_eur", 60) or 60)

                price_realization_factor = 0.96
                if regime == "covid_lockdown":
                    price_realization_factor = 0.90
                elif regime in ["covid_partial_reopen", "recovery_constrained"]:
                    price_realization_factor = 0.93
                elif regime in ["stable_recovery", "stable_mature_market"]:
                    price_realization_factor = 0.98

                avg_ticket = round(rec_price * sens["yield_strength"] * price_realization_factor * np.random.uniform(0.96, 1.03), 2)
                revenue = round(bookings * avg_ticket, 2)
                revpath = round(revenue / hours_available, 2) if hours_available > 0 else 0
                yield_ = round(revenue / sold_hours, 2) if sold_hours > 0 else 0

                if regime == "covid_lockdown":
                    cancellation = np.random.uniform(18, 28)
                    no_show = np.random.uniform(6, 11)
                    rebooking = np.random.uniform(20, 35)
                    addon_attach = np.random.uniform(8, 16)
                    retail_attach = np.random.uniform(4, 10)
                elif regime in ["covid_partial_reopen", "recovery_constrained"]:
                    cancellation = np.random.uniform(12, 20)
                    no_show = np.random.uniform(4, 8)
                    rebooking = np.random.uniform(28, 42)
                    addon_attach = np.random.uniform(10, 20)
                    retail_attach = np.random.uniform(5, 12)
                elif regime in ["macro_stress_normalization"]:
                    cancellation = np.random.uniform(8, 14)
                    no_show = np.random.uniform(3, 6)
                    rebooking = np.random.uniform(35, 50)
                    addon_attach = np.random.uniform(14, 24)
                    retail_attach = np.random.uniform(8, 15)
                else:
                    cancellation = np.random.uniform(6, 11)
                    no_show = np.random.uniform(2, 5)
                    rebooking = np.random.uniform(40, 58)
                    addon_attach = np.random.uniform(16, 28)
                    retail_attach = np.random.uniform(9, 18)

                complaint = np.random.uniform(1.0, 3.8)
                recovery = min(100, complaint * np.random.uniform(7.5, 9.5))

                tr_rows.append({
                    "outlet_id": outlet_id,
                    "outlet_name": outlet_name,
                    "market_context": "Malta",
                    "period_type": p["period_type"],
                    "period_start": p["period_start"],
                    "period_end": p["period_end"],
                    "year": p["year"],
                    "month": p["month"],
                    "month_id": p["month_id"],
                    "rolling_window_weeks": p["rolling_window_weeks"],
                    "market_regime": regime,
                    "outlet_positioning": outlet_position,
                    "treatment_category": cat,
                    "treatment_variant": t.get("treatment_variant", "standard"),
                    "session_duration_min": int(dur),
                    "bookings_count": bookings,
                    "guest_count": bookings,
                    "sold_hours": sold_hours,
                    "hours_available": hours_available,
                    "utilization_percent": round(utilization, 2),
                    "revenue_eur": revenue,
                    "revpath_eur_per_available_hour": revpath,
                    "yield_eur_per_sold_hour": yield_,
                    "avg_ticket_eur": avg_ticket,
                    "rebooking_rate_percent": round(rebooking, 2),
                    "addon_attach_rate_percent": round(addon_attach, 2),
                    "retail_attach_rate_percent": round(retail_attach, 2),
                    "cancellation_rate_percent": round(cancellation, 2),
                    "no_show_rate_percent": round(no_show, 2),
                    "complaint_rate_percent": round(complaint, 2),
                    "service_recovery_rate_percent": round(recovery, 2),
                })

    treatment_kpi = pd.DataFrame(tr_rows)
    treatment_kpi.to_csv(TREATMENT_KPI_FP, index=False)

    # ======================================================
    # THERAPIST KPI
    # ======================================================
    th_rows = []

    for therapist_id, therapist_name, outlet_id, therapist_factor in THERAPISTS:
        outlet_name = [x[1] for x in OUTLETS if x[0] == outlet_id][0]

        for _, p in periods.iterrows():
            regime = p["market_regime"]
            demand_env = float(p["external_demand_proxy_index"])

            hours_available = round(148 + np.random.uniform(-10, 14), 2)

            if regime == "covid_lockdown":
                sold_ratio = np.random.uniform(0.18, 0.35) * therapist_factor
            elif regime in ["covid_partial_reopen", "recovery_constrained"]:
                sold_ratio = np.random.uniform(0.38, 0.56) * therapist_factor
            elif regime == "rebound_growth":
                sold_ratio = np.random.uniform(0.55, 0.72) * therapist_factor
            else:
                sold_ratio = np.random.uniform(0.52, 0.70) * therapist_factor

            sold_ratio = min(0.92, max(0.12, sold_ratio))
            hours_sold = round(hours_available * sold_ratio, 2)
            utilization = round((hours_sold / hours_available) * 100, 2) if hours_available > 0 else 0

            base_yield = 62 * therapist_factor
            if regime == "covid_lockdown":
                base_yield *= 0.92
            elif regime in ["stable_recovery", "stable_mature_market"]:
                base_yield *= 1.03

            yield_ = round(base_yield * np.random.uniform(0.94, 1.06), 2)
            revenue = round(hours_sold * yield_, 2)
            revpath = round(revenue / hours_available, 2) if hours_available > 0 else 0
            avg_ticket = round(yield_ * 1.05, 2)

            if regime == "covid_lockdown":
                rebooking = np.random.uniform(20, 34)
                addon = np.random.uniform(8, 15)
                retail = np.random.uniform(4, 9)
                cancel_impact = np.random.uniform(7, 12)
                adherence = np.random.uniform(72, 88)
                attendance = np.random.uniform(74, 90)
            elif regime in ["covid_partial_reopen", "recovery_constrained"]:
                rebooking = np.random.uniform(28, 42)
                addon = np.random.uniform(10, 18)
                retail = np.random.uniform(5, 11)
                cancel_impact = np.random.uniform(4, 8)
                adherence = np.random.uniform(76, 91)
                attendance = np.random.uniform(80, 94)
            else:
                rebooking = np.random.uniform(38, 58)
                addon = np.random.uniform(15, 28)
                retail = np.random.uniform(8, 18)
                cancel_impact = np.random.uniform(2, 6)
                adherence = np.random.uniform(80, 96)
                attendance = np.random.uniform(84, 97)

            complaint = np.random.uniform(1.0, 4.0) + max(0, (1 - therapist_factor) * 3)
            recovery = min(100, complaint * np.random.uniform(7.5, 9.5))

            th_rows.append({
                "therapist_id": therapist_id,
                "therapist_name": therapist_name,
                "outlet_id": outlet_id,
                "outlet_name": outlet_name,
                "market_context": "Malta",
                "period_type": p["period_type"],
                "period_start": p["period_start"],
                "period_end": p["period_end"],
                "year": p["year"],
                "month": p["month"],
                "month_id": p["month_id"],
                "rolling_window_weeks": p["rolling_window_weeks"],
                "market_regime": regime,
                "therapist_role": "therapist",
                "employment_type": "full_time",
                "active_flag": "yes",
                "hours_available": round(hours_available, 2),
                "hours_sold": round(hours_sold, 2),
                "utilization_percent": round(utilization, 2),
                "revenue_eur": round(revenue, 2),
                "yield_eur_per_sold_hour": round(yield_, 2),
                "revpath_proxy_eur_per_available_hour": round(revpath, 2),
                "avg_ticket_eur": round(avg_ticket, 2),
                "rebooking_rate_percent": round(rebooking, 2),
                "addon_attach_rate_percent": round(addon, 2),
                "retail_attach_rate_percent": round(retail, 2),
                "cancellation_impact_rate_percent": round(cancel_impact, 2),
                "complaint_rate_percent": round(complaint, 2),
                "service_recovery_rate_percent": round(recovery, 2),
                "schedule_adherence_percent": round(adherence, 2),
                "attendance_reliability_percent": round(attendance, 2),
            })

    therapist_kpi = pd.DataFrame(th_rows)
    therapist_kpi.to_csv(THERAPIST_KPI_FP, index=False)

    # ======================================================
    # EXTERNAL PROXY
    # ======================================================
    ext = periods.copy()
    ext.to_csv(EXTERNAL_PROXY_FP, index=False)

    # ======================================================
    # CAPACITY PROXY
    # ======================================================
    cap_rows = []
    for outlet_id, outlet_name, outlet_factor, outlet_position in OUTLETS:
        for _, p in periods.iterrows():
            regime = p["market_regime"]

            if regime == "covid_lockdown":
                payroll_eff = np.random.uniform(55, 68)
                capacity_pressure = np.random.uniform(18, 35)
            elif regime in ["covid_partial_reopen", "recovery_constrained"]:
                payroll_eff = np.random.uniform(60, 76)
                capacity_pressure = np.random.uniform(28, 50)
            elif regime == "rebound_growth":
                payroll_eff = np.random.uniform(72, 86)
                capacity_pressure = np.random.uniform(52, 76)
            else:
                payroll_eff = np.random.uniform(74, 90)
                capacity_pressure = np.random.uniform(45, 72)

            cap_rows.append({
                "outlet_id": outlet_id,
                "outlet_name": outlet_name,
                "period_type": p["period_type"],
                "period_start": p["period_start"],
                "period_end": p["period_end"],
                "year": p["year"],
                "month": p["month"],
                "month_id": p["month_id"],
                "market_regime": regime,
                "payroll_efficiency_percent": round(payroll_eff, 2),
                "capacity_pressure_percent": round(capacity_pressure, 2),
                "external_stress_flag": p["external_stress_flag"],
            })

    cap = pd.DataFrame(cap_rows)
    cap.to_csv(CAPACITY_PROXY_FP, index=False)

    print(f"[OK] saved: {TREATMENT_KPI_FP} | rows={len(treatment_kpi)}")
    print(f"[OK] saved: {THERAPIST_KPI_FP} | rows={len(therapist_kpi)}")
    print(f"[OK] saved: {EXTERNAL_PROXY_FP} | rows={len(ext)}")
    print(f"[OK] saved: {CAPACITY_PROXY_FP} | rows={len(cap)}")

if __name__ == "__main__":
    main()
