from pathlib import Path
import pandas as pd
import math

BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")
OPERATING_DIR = BASE / "data_processed" / "operating_model"
OPERATING_DIR.mkdir(parents=True, exist_ok=True)

ROSTER_FP = OPERATING_DIR / "outlet_roster_quality_design_output.csv"
SERVICE_FP = OPERATING_DIR / "outlet_treatment_service_model.csv"

roster = pd.read_csv(ROSTER_FP)
service = pd.read_csv(SERVICE_FP)

# --------------------------------------------------
# 1) BUILD THERAPIST OPERATING PROFILE MASTER
# --------------------------------------------------
# We use revised safe roster headcount as the baseline therapist pool size.
# This is a designed management layer, not a claim of actual employee records.

outlet_archetype_plan = {
    "Central Malta Spa": [
        ("balanced_core", 0.45),
        ("premium_capable", 0.20),
        ("throughput_capable", 0.20),
        ("flex_generalist", 0.15),
    ],
    "Gozo Spa": [
        ("leisure_specialist", 0.40),
        ("premium_capable", 0.25),
        ("balanced_core", 0.20),
        ("flex_generalist", 0.15),
    ],
    "Sliema / Balluta Spa": [
        ("premium_capable", 0.45),
        ("balanced_core", 0.25),
        ("leisure_specialist", 0.15),
        ("flex_generalist", 0.15),
    ],
    "Valletta Spa": [
        ("premium_capable", 0.50),
        ("curated_specialist", 0.25),
        ("balanced_core", 0.15),
        ("flex_generalist", 0.10),
    ],
    "Mellieha Spa": [
        ("leisure_specialist", 0.35),
        ("balanced_core", 0.25),
        ("premium_capable", 0.20),
        ("flex_generalist", 0.20),
    ],
    "Qawra / St Paul’s Bay Spa": [
        ("throughput_capable", 0.35),
        ("balanced_core", 0.30),
        ("flex_generalist", 0.20),
        ("premium_capable", 0.15),
    ],
    "St Julian’s / Paceville Spa": [
        ("throughput_capable", 0.35),
        ("compression_specialist", 0.25),
        ("balanced_core", 0.20),
        ("premium_capable", 0.10),
        ("flex_generalist", 0.10),
    ],
}

archetype_scores = {
    "balanced_core": {
        "treatment_precision_score_0_100": 78,
        "service_consistency_score_0_100": 80,
        "long_treatment_suitability_score_0_100": 74,
        "premium_delivery_score_0_100": 76,
        "throughput_delivery_score_0_100": 76,
        "compression_resilience_score_0_100": 74,
        "recovery_discipline_score_0_100": 80,
        "cross_sell_maturity_score_0_100": 72,
        "coaching_need_score_0_100": 36,
    },
    "premium_capable": {
        "treatment_precision_score_0_100": 88,
        "service_consistency_score_0_100": 90,
        "long_treatment_suitability_score_0_100": 86,
        "premium_delivery_score_0_100": 91,
        "throughput_delivery_score_0_100": 68,
        "compression_resilience_score_0_100": 66,
        "recovery_discipline_score_0_100": 87,
        "cross_sell_maturity_score_0_100": 80,
        "coaching_need_score_0_100": 28,
    },
    "throughput_capable": {
        "treatment_precision_score_0_100": 74,
        "service_consistency_score_0_100": 75,
        "long_treatment_suitability_score_0_100": 62,
        "premium_delivery_score_0_100": 66,
        "throughput_delivery_score_0_100": 90,
        "compression_resilience_score_0_100": 88,
        "recovery_discipline_score_0_100": 72,
        "cross_sell_maturity_score_0_100": 70,
        "coaching_need_score_0_100": 40,
    },
    "flex_generalist": {
        "treatment_precision_score_0_100": 72,
        "service_consistency_score_0_100": 73,
        "long_treatment_suitability_score_0_100": 70,
        "premium_delivery_score_0_100": 68,
        "throughput_delivery_score_0_100": 72,
        "compression_resilience_score_0_100": 70,
        "recovery_discipline_score_0_100": 74,
        "cross_sell_maturity_score_0_100": 68,
        "coaching_need_score_0_100": 48,
    },
    "leisure_specialist": {
        "treatment_precision_score_0_100": 84,
        "service_consistency_score_0_100": 85,
        "long_treatment_suitability_score_0_100": 88,
        "premium_delivery_score_0_100": 84,
        "throughput_delivery_score_0_100": 62,
        "compression_resilience_score_0_100": 64,
        "recovery_discipline_score_0_100": 84,
        "cross_sell_maturity_score_0_100": 74,
        "coaching_need_score_0_100": 30,
    },
    "curated_specialist": {
        "treatment_precision_score_0_100": 92,
        "service_consistency_score_0_100": 93,
        "long_treatment_suitability_score_0_100": 90,
        "premium_delivery_score_0_100": 94,
        "throughput_delivery_score_0_100": 58,
        "compression_resilience_score_0_100": 60,
        "recovery_discipline_score_0_100": 90,
        "cross_sell_maturity_score_0_100": 82,
        "coaching_need_score_0_100": 24,
    },
    "compression_specialist": {
        "treatment_precision_score_0_100": 76,
        "service_consistency_score_0_100": 78,
        "long_treatment_suitability_score_0_100": 64,
        "premium_delivery_score_0_100": 68,
        "throughput_delivery_score_0_100": 92,
        "compression_resilience_score_0_100": 92,
        "recovery_discipline_score_0_100": 76,
        "cross_sell_maturity_score_0_100": 72,
        "coaching_need_score_0_100": 38,
    },
}

outlet_code_map = {
    "Central Malta Spa": "CEN",
    "Gozo Spa": "GOZ",
    "Sliema / Balluta Spa": "SLI",
    "Valletta Spa": "VAL",
    "Mellieha Spa": "MEL",
    "Qawra / St Paul’s Bay Spa": "QAW",
    "St Julian’s / Paceville Spa": "STJ",
}

def allocate_counts(total, plan):
    raw = [total * p for _, p in plan]
    base = [int(x) for x in raw]
    remainder = total - sum(base)
    fractions = [(i, raw[i] - base[i]) for i in range(len(plan))]
    fractions.sort(key=lambda x: x[1], reverse=True)
    for i in range(remainder):
        base[fractions[i][0]] += 1
    return {plan[i][0]: base[i] for i in range(len(plan))}

therapist_rows = []

for _, r in roster.iterrows():
    outlet = r["outlet_name"]
    total_pool = int(r["revised_safe_roster_headcount"])
    plan = outlet_archetype_plan[outlet]
    counts = allocate_counts(total_pool, plan)

    seq = 1
    for archetype, cnt in counts.items():
        base_scores = archetype_scores[archetype]
        for _ in range(cnt):
            therapist_id = f"{outlet_code_map[outlet]}_TH_{seq:03d}"
            seq += 1

            therapist_rows.append({
                "therapist_id": therapist_id,
                "home_outlet_name": outlet,
                "therapist_archetype": archetype,
                **base_scores
            })

therapist_master = pd.DataFrame(therapist_rows)

# --------------------------------------------------
# 2) MERGE OUTLET SERVICE MODEL
# --------------------------------------------------
df = therapist_master.merge(
    service,
    left_on="home_outlet_name",
    right_on="outlet_name",
    how="left"
)

# --------------------------------------------------
# 3) BUILD SCORECARD
# --------------------------------------------------
def weighted_score(values, weights):
    return round(sum(values[k] * weights[k] for k in weights) / sum(weights.values()), 1)

score_rows = []

for _, r in df.iterrows():
    treatment_mix_fit_score = weighted_score(
        {
            "treatment_precision": r["treatment_precision_score_0_100"],
            "long_treatment_suitability": r["long_treatment_suitability_score_0_100"],
            "service_consistency": r["service_consistency_score_0_100"],
        },
        {
            "treatment_precision": 0.40,
            "long_treatment_suitability": 0.35,
            "service_consistency": 0.25,
        }
    )

    service_consistency_fit_score = weighted_score(
        {
            "service_consistency": r["service_consistency_score_0_100"],
            "recovery_discipline": r["recovery_discipline_score_0_100"],
            "precision": r["treatment_precision_score_0_100"],
        },
        {
            "service_consistency": 0.45,
            "recovery_discipline": 0.30,
            "precision": 0.25,
        }
    )

    premium_delivery_readiness_score = weighted_score(
        {
            "premium_delivery": r["premium_delivery_score_0_100"],
            "precision": r["treatment_precision_score_0_100"],
            "consistency": r["service_consistency_score_0_100"],
        },
        {
            "premium_delivery": 0.45,
            "precision": 0.25,
            "consistency": 0.30,
        }
    )

    throughput_delivery_readiness_score = weighted_score(
        {
            "throughput_delivery": r["throughput_delivery_score_0_100"],
            "compression_resilience": r["compression_resilience_score_0_100"],
            "recovery_discipline": r["recovery_discipline_score_0_100"],
        },
        {
            "throughput_delivery": 0.45,
            "compression_resilience": 0.35,
            "recovery_discipline": 0.20,
        }
    )

    fatigue_exposure_risk_score = round(
        0.35 * (100 - r["compression_resilience_score_0_100"]) +
        0.25 * (100 - r["recovery_discipline_score_0_100"]) +
        0.20 * r["coaching_need_score_0_100"] +
        0.20 * (100 - r["service_consistency_score_0_100"]),
        1
    )

    # Outlet-model fit logic
    pacing_model = r["treatment_pacing_model"]
    if pacing_model in ["curated_premium_protected", "premium_consistency_protected"]:
        outlet_fit_score = round(
            0.50 * premium_delivery_readiness_score +
            0.30 * service_consistency_fit_score +
            0.20 * treatment_mix_fit_score,
            1
        )
    elif pacing_model in ["throughput_standardized_control", "high_compression_protected"]:
        outlet_fit_score = round(
            0.45 * throughput_delivery_readiness_score +
            0.30 * service_consistency_fit_score +
            0.25 * treatment_mix_fit_score,
            1
        )
    else:
        outlet_fit_score = round(
            0.34 * premium_delivery_readiness_score +
            0.33 * throughput_delivery_readiness_score +
            0.33 * service_consistency_fit_score,
            1
        )

    # Coaching priority
    if fatigue_exposure_risk_score >= 45 or r["coaching_need_score_0_100"] >= 45:
        coaching_priority = "high"
    elif fatigue_exposure_risk_score >= 32 or r["coaching_need_score_0_100"] >= 32:
        coaching_priority = "medium"
    else:
        coaching_priority = "low"

    # Deployment recommendation
    if premium_delivery_readiness_score >= 88 and service_consistency_fit_score >= 85:
        deployment_recommendation = "deploy_to_premium_curated_service"
    elif throughput_delivery_readiness_score >= 86 and fatigue_exposure_risk_score <= 38:
        deployment_recommendation = "deploy_to_high_throughput_or_compression_windows"
    elif outlet_fit_score >= 78:
        deployment_recommendation = "retain_in_core_outlet_model"
    elif coaching_priority == "high":
        deployment_recommendation = "coach_before_peak_or_sensitive_deployment"
    else:
        deployment_recommendation = "use_in_balanced_or_supported_schedule"

    score_rows.append({
        "therapist_id": r["therapist_id"],
        "home_outlet_name": r["home_outlet_name"],
        "assigned_service_model": r["treatment_pacing_model"],
        "therapist_archetype": r["therapist_archetype"],
        "treatment_mix_fit_score_0_100": treatment_mix_fit_score,
        "service_consistency_fit_score_0_100": service_consistency_fit_score,
        "fatigue_exposure_risk_score_0_100": fatigue_exposure_risk_score,
        "premium_delivery_readiness_score_0_100": premium_delivery_readiness_score,
        "throughput_delivery_readiness_score_0_100": throughput_delivery_readiness_score,
        "outlet_model_fit_score_0_100": outlet_fit_score,
        "coaching_need_score_0_100": r["coaching_need_score_0_100"],
        "coaching_priority": coaching_priority,
        "deployment_recommendation": deployment_recommendation,
    })

scorecard = pd.DataFrame(score_rows)

# --------------------------------------------------
# 4) POLICY TABLE
# --------------------------------------------------
policy_rows = [
    {
        "policy_id": "THSC_001",
        "policy_group": "definition_rule",
        "policy_title": "Therapist operating profile baseline",
        "formula_expression": "Therapist archetype mapped to treatment precision, consistency, premium, throughput, resilience, and coaching need",
        "management_reasoning": "Creates a managerially usable therapist profile layer when live therapist operating data is not yet available.",
        "status": "policy_defined",
    },
    {
        "policy_id": "THSC_002",
        "policy_group": "fit_rule",
        "policy_title": "Treatment mix fit score",
        "formula_expression": "Weighted score using treatment_precision, long_treatment_suitability, service_consistency",
        "management_reasoning": "Measures how well a therapist fits the treatment rhythm and complexity expected by the outlet model.",
        "status": "policy_defined",
    },
    {
        "policy_id": "THSC_003",
        "policy_group": "fit_rule",
        "policy_title": "Service consistency fit score",
        "formula_expression": "Weighted score using service_consistency, recovery_discipline, treatment_precision",
        "management_reasoning": "Measures how reliably a therapist can deliver repeatable service quality.",
        "status": "policy_defined",
    },
    {
        "policy_id": "THSC_004",
        "policy_group": "risk_rule",
        "policy_title": "Fatigue exposure risk score",
        "formula_expression": "Inverse resilience + inverse recovery discipline + coaching need + inverse service consistency",
        "management_reasoning": "Flags therapists more likely to lose quality under pressure, fatigue, or schedule compression.",
        "status": "policy_defined",
    },
    {
        "policy_id": "THSC_005",
        "policy_group": "deployment_rule",
        "policy_title": "Deployment recommendation",
        "formula_expression": "Recommendation based on premium readiness, throughput readiness, outlet-model fit, and coaching priority",
        "management_reasoning": "Translates score logic into management action for deployment, protection, or coaching.",
        "status": "policy_defined",
    },
]

policy = pd.DataFrame(policy_rows)

# --------------------------------------------------
# 5) SAVE
# --------------------------------------------------
fp_master = OPERATING_DIR / "therapist_operating_profile_master.csv"
fp_scorecard = OPERATING_DIR / "therapist_service_consistency_scorecard.csv"
fp_policy = OPERATING_DIR / "therapist_service_consistency_policy.csv"

therapist_master.to_csv(fp_master, index=False)
scorecard.to_csv(fp_scorecard, index=False)
policy.to_csv(fp_policy, index=False)

print(f"[OK] saved: {fp_master}")
print(f"[OK] saved: {fp_scorecard}")
print(f"[OK] saved: {fp_policy}")

print("\n=== THERAPIST OPERATING PROFILE MASTER ===")
print(therapist_master.head(20).to_string(index=False))

print("\n=== THERAPIST SERVICE CONSISTENCY SCORECARD ===")
print(scorecard.head(30).to_string(index=False))

print("\n=== DEPLOYMENT SUMMARY ===")
summary = (
    scorecard.groupby(["home_outlet_name", "deployment_recommendation"])
    .size()
    .reset_index(name="therapist_count")
    .sort_values(["home_outlet_name", "therapist_count"], ascending=[True, False])
)
print(summary.to_string(index=False))
