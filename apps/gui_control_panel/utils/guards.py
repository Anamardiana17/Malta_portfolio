from __future__ import annotations

def gui_guardrail_notes() -> list[str]:
    return [
        "GUI is control/presentation layer only",
        "No synthetic intra-day logic",
        "No unsupported staffing inference",
        "Core repo pipeline remains authoritative",
    ]
