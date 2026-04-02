from __future__ import annotations

from pathlib import Path
import csv


REPO_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_PATH = REPO_ROOT / "data_processed" / "management" / "outlet_operating_role_framework.csv"

OUTLETS = [
    "Central Malta Spa",
    "Gozo Spa",
    "Mellieha Spa",
    "Qawra / St Paul’s Bay Spa",
    "Sliema / Balluta Spa",
    "St Julian’s / Paceville Spa",
    "Valletta Spa",
]

ROLE_TEMPLATES = [
    {
        "role_name": "spa receptionist",
        "role_scope_level": "outlet_operating_role",
        "minimum_role_headcount": 3,
        "ideal_role_headcount": 3,
        "role_coverage_rationale": (
            "Minimum resilient front-desk coverage requires three receptionists per outlet "
            "to protect booking continuity, guest communication, shift rotation, leave coverage, "
            "and arrival-flow control."
        ),
        "coverage_continuity_note": (
            "Three-person receptionist coverage reduces single-point-of-failure risk across "
            "off days, sick leave, breaks, and peak guest-contact windows."
        ),
        "role_mission": (
            "Protect front-of-house conversion, booking accuracy, arrival discipline, "
            "and guest communication continuity across the spa journey."
        ),
        "guest_journey_stage": "pre-arrival, arrival, check-in, rebooking, post-visit follow-up",
        "primary_operating_scope": (
            "booking flow, guest contact handling, arrival coordination, reschedule recovery, "
            "rebooking support, front desk commercial continuity"
        ),
        "core_kpi_link": (
            "booking conversion support; cancellation recovery discipline; no-show containment support; "
            "rebooking continuity; arrival-flow stability"
        ),
        "service_risk_if_missing": (
            "Higher booking leakage risk, weaker arrival control, reduced rebooking continuity, "
            "and weaker guest communication consistency."
        ),
        "coordination_with_therapists": (
            "Coordinates arrival timing, booking accuracy, therapist assignment clarity, "
            "and guest handoff discipline."
        ),
        "coordination_with_spa_manager": (
            "Escalates booking friction, guest complaints, flow congestion, and recurring front-desk control gaps."
        ),
        "coordination_with_front_of_house": (
            "Acts as primary front-of-house operating anchor for guest communication and booking continuity."
        ),
        "decision_support_note": (
            "This role should be interpreted as a control and conversion support layer, "
            "not as a synthetic demand-forecasting or hour-level deployment model."
        ),
    },
    {
        "role_name": "spa attendant",
        "role_scope_level": "outlet_operating_role",
        "minimum_role_headcount": 3,
        "ideal_role_headcount": 3,
        "role_coverage_rationale": (
            "Minimum resilient floor-support coverage requires three spa attendants per outlet "
            "to protect room readiness, turnaround continuity, hygiene support, amenity coverage, "
            "and treatment-environment stability."
        ),
        "coverage_continuity_note": (
            "Three-person attendant coverage reduces service-delay and room-readiness risk across "
            "off days, sick leave, breaks, and turnover peaks."
        ),
        "role_mission": (
            "Protect service-environment readiness, room turnaround reliability, amenity completeness, "
            "and floor-support continuity."
        ),
        "guest_journey_stage": "pre-service, treatment setup, between-service turnaround, recovery/reset",
        "primary_operating_scope": (
            "room readiness, linen and amenity readiness, hygiene support, turnaround speed, "
            "on-floor service support, treatment-environment consistency"
        ),
        "core_kpi_link": (
            "room-readiness continuity; service-delay containment support; turnaround discipline; "
            "environment consistency protection"
        ),
        "service_risk_if_missing": (
            "Higher room-delay risk, weaker service environment consistency, slower reset between appointments, "
            "and greater strain on therapist focus."
        ),
        "coordination_with_therapists": (
            "Supports treatment-room readiness, reset discipline, and smooth handoff into therapist-led service delivery."
        ),
        "coordination_with_spa_manager": (
            "Escalates repeated room-readiness failures, amenity shortages, hygiene issues, and turnover bottlenecks."
        ),
        "coordination_with_front_of_house": (
            "Supports desk-to-floor readiness visibility so guest arrival timing matches room readiness."
        ),
        "decision_support_note": (
            "This role should be interpreted as an operating-readiness support layer, "
            "not as a pseudo-granular room scheduling engine."
        ),
    },
    {
        "role_name": "spa assistant manager",
        "role_scope_level": "outlet_operating_role",
        "minimum_role_headcount": 1,
        "ideal_role_headcount": 1,
        "role_coverage_rationale": (
            "One assistant spa manager per outlet provides a stable execution bridge between "
            "spa manager direction and live shift-level operating follow-through."
        ),
        "coverage_continuity_note": (
            "One assistant spa manager is treated as sufficient baseline support for escalation handling, "
            "floor coordination, and service recovery oversight."
        ),
        "role_mission": (
            "Protect shift-level execution, floor coordination, service recovery oversight, "
            "and cross-role discipline between manager intent and frontline delivery."
        ),
        "guest_journey_stage": "live service oversight, escalation, service recovery, shift coordination",
        "primary_operating_scope": (
            "floor coordination, escalation handling, service recovery oversight, shift control, "
            "team follow-through, cross-role alignment"
        ),
        "core_kpi_link": (
            "service recovery discipline; floor-execution stability; escalation response quality; "
            "cross-role operating consistency"
        ),
        "service_risk_if_missing": (
            "Higher execution drift, weaker live escalation handling, weaker service recovery discipline, "
            "and greater dependence on the spa manager for every operational interruption."
        ),
        "coordination_with_therapists": (
            "Supports deployment discipline, live floor escalation, service recovery, and adherence to operating standards."
        ),
        "coordination_with_spa_manager": (
            "Acts as the execution bridge between management direction and shift-level operating follow-through."
        ),
        "coordination_with_front_of_house": (
            "Aligns front desk flow with floor readiness, escalation handling, and guest recovery priorities."
        ),
        "decision_support_note": (
            "This role should be interpreted as a shift-control and execution-bridge layer, "
            "not as a proxy for unsupported hierarchy assumptions."
        ),
    },
    {
        "role_name": "spa manager",
        "role_scope_level": "outlet_operating_role",
        "minimum_role_headcount": 1,
        "ideal_role_headcount": 1,
        "role_coverage_rationale": (
            "One spa manager per outlet provides accountable leadership for commercial control, "
            "service standards, staffing oversight, escalation ownership, and outlet-level decision making."
        ),
        "coverage_continuity_note": (
            "One spa manager is treated as the accountable operating lead per outlet, "
            "with assistant manager support handling part of live shift coordination."
        ),
        "role_mission": (
            "Provide accountable outlet leadership across commercial control, service standards, "
            "people oversight, operating discipline, and management decision follow-through."
        ),
        "guest_journey_stage": "full outlet oversight, escalation ownership, management review, operating control",
        "primary_operating_scope": (
            "outlet leadership, staffing control, service-standard ownership, management escalation, "
            "commercial oversight, operating-discipline enforcement"
        ),
        "core_kpi_link": (
            "outlet control stability; service-standard protection; staffing governance; "
            "management escalation ownership; operating-discipline continuity"
        ),
        "service_risk_if_missing": (
            "Higher control drift, weaker accountability, slower escalation ownership, "
            "and weaker outlet-level operating discipline."
        ),
        "coordination_with_therapists": (
            "Owns people oversight, staffing direction, service-standard enforcement, and coaching escalation."
        ),
        "coordination_with_spa_manager": (
            "This role is the accountable spa manager ownership layer for the outlet."
        ),
        "coordination_with_front_of_house": (
            "Owns front-of-house performance expectations, service recovery standards, and operating control escalation."
        ),
        "decision_support_note": (
            "This role should be interpreted as the accountable outlet management layer, "
            "not as a synthetic multi-site control abstraction."
        ),
    },
]

MODEL_LAYER_TYPE = "operating_role_management_layer"
MODEL_BASIS_NOTE = (
    "This outlet operating role framework extends the Malta portfolio management layer with "
    "non-therapist operating-role interpretation and baseline staffing-quantity guidance under controlled assumptions. "
    "It does not infer unsupported hour-level staffing deployment or synthetic intra-day logic."
)


def build_rows() -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []

    for outlet_name in OUTLETS:
        for role in ROLE_TEMPLATES:
            rows.append(
                {
                    "outlet_name": outlet_name,
                    "role_name": role["role_name"],
                    "role_scope_level": role["role_scope_level"],
                    "minimum_role_headcount": str(role["minimum_role_headcount"]),
                    "ideal_role_headcount": str(role["ideal_role_headcount"]),
                    "role_coverage_rationale": role["role_coverage_rationale"],
                    "coverage_continuity_note": role["coverage_continuity_note"],
                    "role_mission": role["role_mission"],
                    "guest_journey_stage": role["guest_journey_stage"],
                    "primary_operating_scope": role["primary_operating_scope"],
                    "core_kpi_link": role["core_kpi_link"],
                    "service_risk_if_missing": role["service_risk_if_missing"],
                    "coordination_with_therapists": role["coordination_with_therapists"],
                    "coordination_with_spa_manager": role["coordination_with_spa_manager"],
                    "coordination_with_front_of_house": role["coordination_with_front_of_house"],
                    "decision_support_note": role["decision_support_note"],
                    "model_layer_type": MODEL_LAYER_TYPE,
                    "model_basis_note": MODEL_BASIS_NOTE,
                }
            )

    return rows


def write_csv(rows: list[dict[str, str]]) -> None:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "outlet_name",
        "role_name",
        "role_scope_level",
        "minimum_role_headcount",
        "ideal_role_headcount",
        "role_coverage_rationale",
        "coverage_continuity_note",
        "role_mission",
        "guest_journey_stage",
        "primary_operating_scope",
        "core_kpi_link",
        "service_risk_if_missing",
        "coordination_with_therapists",
        "coordination_with_spa_manager",
        "coordination_with_front_of_house",
        "decision_support_note",
        "model_layer_type",
        "model_basis_note",
    ]

    with OUTPUT_PATH.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    rows = build_rows()
    write_csv(rows)
    print(f"[OK] wrote {len(rows)} rows to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
