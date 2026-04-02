from __future__ import annotations

from pathlib import Path
import numpy as np
import pandas as pd


BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")

INPUT_CANDIDATES = [
    BASE / "data_processed/final_bundle/malta_external_proxy_monthly_2017_2025_clean.csv",
    BASE / "data_processed/monthly_proxy_blocks/proxy_airport_monthly_2017_2025.csv",
]

OUTPUT_FP = BASE / "data_processed/final_bundle/malta_external_proxy_monthly_2017_2025_with_footfall_score.csv"

FEATURE_SPECS = [
    {
        "name": "airport_passengers_international",
        "weight": 0.40,
        "higher_is_better": True,
        "group": "air_arrivals",
    },
    {
        "name": "tourism_demand_air_passengers_international",
        "weight": 0.30,
        "higher_is_better": True,
        "group": "tourism_air_demand",
    },
    {
        "name": "accommodation_nights_spent",
        "weight": 0.30,
        "higher_is_better": True,
        "group": "stay_intensity",
    },
]

WINSOR_LOWER = 0.05
WINSOR_UPPER = 0.95
MIN_REQUIRED_FEATURES = 1
ROLLING_WINDOW = 3


def resolve_input_file() -> Path:
    for fp in INPUT_CANDIDATES:
        if fp.exists():
            return fp
    raise FileNotFoundError(
        "No valid input file found. Checked:\n" + "\n".join(str(x) for x in INPUT_CANDIDATES)
    )


def winsorize_series(s: pd.Series, lower_q: float, upper_q: float) -> pd.Series:
    s_num = pd.to_numeric(s, errors="coerce")
    non_null = s_num.dropna()
    if non_null.empty:
        return s_num
    lo = non_null.quantile(lower_q)
    hi = non_null.quantile(upper_q)
    return s_num.clip(lower=lo, upper=hi)


def percentile_score_0_100(s: pd.Series, ascending: bool = True) -> pd.Series:
    s_num = pd.to_numeric(s, errors="coerce")
    mask = s_num.notna()

    out = pd.Series(np.nan, index=s.index, dtype="float64")
    if mask.sum() == 0:
        return out

    ranked = s_num[mask].rank(method="average", pct=True, ascending=ascending)
    out.loc[mask] = (ranked * 100).round(2)
    return out


def normalize_active_weights(active_features: list[dict]) -> dict[str, float]:
    total = sum(f["weight"] for f in active_features)
    if total <= 0:
        raise ValueError("Feature weights must sum to > 0.")
    return {f["name"]: f["weight"] / total for f in active_features}


def weighted_mean_ignore_na(df: pd.DataFrame, value_cols: list[str], weight_map: dict[str, float]) -> pd.Series:
    values = df[value_cols].copy()

    weights = pd.DataFrame(
        {
            col: np.where(values[col].notna(), weight_map[col], 0.0)
            for col in value_cols
        },
        index=df.index,
    )

    weight_sum = weights.sum(axis=1)
    weighted_sum = sum(values[col].fillna(0) * weights[col] for col in value_cols)

    out = pd.Series(np.nan, index=df.index, dtype="float64")
    valid = weight_sum > 0
    out.loc[valid] = weighted_sum.loc[valid] / weight_sum.loc[valid]
    return out


def add_period_fields(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    if "period_date" in out.columns:
        out["period_date"] = pd.to_datetime(out["period_date"], errors="coerce")
    elif "period_start" in out.columns:
        out["period_date"] = pd.to_datetime(out["period_start"], errors="coerce")
    elif "month_id" in out.columns:
        out["period_date"] = pd.to_datetime(out["month_id"].astype(str), format="%Y-%m", errors="coerce")
    elif "month" in out.columns:
        out["period_date"] = pd.to_datetime(out["month"], errors="coerce")

    if "period_date" in out.columns:
        out["year"] = out["period_date"].dt.year
        out["month_num"] = out["period_date"].dt.month

    return out


def classify_signal_quality(n_features: float) -> str:
    if pd.isna(n_features):
        return "missing"
    if n_features >= 3:
        return "strong"
    if n_features >= 2:
        return "moderate"
    if n_features >= 1:
        return "limited"
    return "missing"


def classify_weight_regime(row: pd.Series, feature_names: list[str]) -> str:
    active = [f for f in feature_names if pd.notna(row.get(f"{f}__score_0_100"))]

    if len(active) == 3:
        return "balanced_full_blend"
    if set(active) == {"accommodation_nights_spent"}:
        return "accommodation_only"
    if set(active) == {"airport_passengers_international"}:
        return "airport_only"
    if set(active) == {"tourism_demand_air_passengers_international"}:
        return "tourism_air_only"
    if "airport_passengers_international" in active and "tourism_demand_air_passengers_international" in active and "accommodation_nights_spent" not in active:
        return "air_travel_only"
    if "accommodation_nights_spent" in active and (
        "airport_passengers_international" in active or "tourism_demand_air_passengers_international" in active
    ):
        return "mixed_partial_blend"
    if len(active) == 2:
        return "two_factor_blend"
    if len(active) == 1:
        return "single_factor_blend"
    return "no_active_inputs"


def main() -> None:
    input_fp = resolve_input_file()

    df = pd.read_csv(
        input_fp,
        na_values=[":", "", "NA", "N/A", "null", "None"],
        keep_default_na=True,
    )
    df = add_period_fields(df)

    available_specs = [f for f in FEATURE_SPECS if f["name"] in df.columns]
    if not available_specs:
        raise ValueError(
            "None of the configured feature columns exist in the input file.\n"
            f"Configured: {[f['name'] for f in FEATURE_SPECS]}\n"
            f"Available: {list(df.columns)}"
        )

    weights = normalize_active_weights(available_specs)

    feature_score_cols = []
    feature_used_cols = []
    feature_names = [f["name"] for f in available_specs]

    for spec in available_specs:
        col = spec["name"]
        clipped_col = f"{col}__winsorized"
        score_col = f"{col}__score_0_100"
        used_col = f"{col}__used_flag"

        df[col] = pd.to_numeric(df[col], errors="coerce")
        df[clipped_col] = winsorize_series(df[col], WINSOR_LOWER, WINSOR_UPPER)
        df[score_col] = percentile_score_0_100(
            df[clipped_col],
            ascending=spec["higher_is_better"],
        )
        df[used_col] = df[score_col].notna().astype(int)

        feature_score_cols.append(score_col)
        feature_used_cols.append(used_col)

    score_weight_map = {
        f"{spec['name']}__score_0_100": weights[spec["name"]]
        for spec in available_specs
    }

    df["footfall_proxy_features_available"] = df[feature_used_cols].sum(axis=1)

    df["footfall_proxy_air_component_present_flag"] = (
        df.get("airport_passengers_international__used_flag", 0).fillna(0).astype(int)
        .clip(lower=0, upper=1)
    )

    tourism_air_flag = (
        df.get("tourism_demand_air_passengers_international__used_flag", 0).fillna(0).astype(int)
        .clip(lower=0, upper=1)
    )

    accommodation_flag = (
        df.get("accommodation_nights_spent__used_flag", 0).fillna(0).astype(int)
        .clip(lower=0, upper=1)
    )

    df["footfall_proxy_core_components_present"] = (
        df["footfall_proxy_air_component_present_flag"]
        + tourism_air_flag
        + accommodation_flag
    )

    raw_score = weighted_mean_ignore_na(df, feature_score_cols, score_weight_map)
    df["external_context_flow_score_0_100_raw"] = raw_score.round(2)

    if "period_date" in df.columns and df["period_date"].notna().any():
        df = df.sort_values("period_date").reset_index(drop=True)
        df["external_context_flow_score_0_100"] = (
            df["external_context_flow_score_0_100_raw"]
            .rolling(window=ROLLING_WINDOW, min_periods=1)
            .mean()
            .round(2)
        )
    else:
        df["external_context_flow_score_0_100"] = df["external_context_flow_score_0_100_raw"]

    df.loc[
        df["footfall_proxy_features_available"] < MIN_REQUIRED_FEATURES,
        "external_context_flow_score_0_100",
    ] = np.nan

    bins = [-np.inf, 20, 40, 60, 80, np.inf]
    labels = [
        "very low contextual support",
        "low contextual support",
        "moderate contextual support",
        "high contextual support",
        "very high contextual support",
    ]
    df["external_context_flow_band"] = pd.cut(
        df["external_context_flow_score_0_100"],
        bins=bins,
        labels=labels,
    )

    df["footfall_proxy_signal_quality"] = df["footfall_proxy_features_available"].apply(classify_signal_quality)
    df["footfall_proxy_weight_regime"] = df.apply(
        lambda row: classify_weight_regime(row, feature_names),
        axis=1,
    )

    df["external_context_flow_method"] = "winsorized_percentile_weighted_blend"
    df["external_context_flow_inputs"] = ", ".join(feature_names)
    df["external_context_flow_note"] = (
        "Monthly external context-support proxy only; not a direct hourly spa demand, outlet traffic, or observed daypart footfall measure. Must be interpreted alongside internal operating proxies for actionable daypart decisions."
    )

    OUTPUT_FP.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_FP, index=False)

    print(f"[OK] input : {input_fp}")
    print(f"[OK] output: {OUTPUT_FP}")

    print("\n=== ACTIVE FEATURES ===")
    for spec in available_specs:
        print(f"- {spec['name']} | weight={weights[spec['name']]:.4f}")

    show_cols = [
        c for c in [
            "period_date",
            "month_id",
            "airport_passengers_international",
            "tourism_demand_air_passengers_international",
            "accommodation_nights_spent",
            "footfall_proxy_features_available",
            "footfall_proxy_signal_quality",
            "footfall_proxy_air_component_present_flag",
            "footfall_proxy_weight_regime",
            "external_context_flow_score_0_100_raw",
            "external_context_flow_score_0_100",
            "external_context_flow_band",
        ] if c in df.columns
    ]

    print("\n=== SAMPLE OUTPUT ===")
    print(df[show_cols].head(12).to_string(index=False))

    print("\n=== QUALITY DISTRIBUTION ===")
    print(df["footfall_proxy_signal_quality"].value_counts(dropna=False).to_string())

    print("\n=== WEIGHT REGIME DISTRIBUTION ===")
    print(df["footfall_proxy_weight_regime"].value_counts(dropna=False).to_string())

    print("\n=== SCORE SUMMARY ===")
    print(df["external_context_flow_score_0_100"].describe().round(2).to_string())


if __name__ == "__main__":
    main()
