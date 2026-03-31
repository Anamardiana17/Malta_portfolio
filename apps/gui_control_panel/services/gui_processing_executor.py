from __future__ import annotations

from dataclasses import dataclass

from services.processing_registry_service import (
    append_processing_execution_registry,
    append_processing_history_log,
    append_processing_trigger_registry,
)


@dataclass
class GuiProcessingExecutionResult:
    batch_id: str
    processing_step: str
    execution_status: str
    qa_status: str
    output_folder: str
    note: str


def execute_gui_processing_trigger(
    batch_id: str,
    processing_step: str,
    operator_note: str,
) -> GuiProcessingExecutionResult:
    batch_id = batch_id.strip()
    processing_step = processing_step.strip()

    trigger_note = operator_note.strip() or "Controlled GUI processing trigger submitted."

    append_processing_trigger_registry(
        batch_id=batch_id,
        processing_step=processing_step,
        trigger_source="gui_control_panel",
        eligibility_status="eligible_for_processing",
        trigger_status="trigger_recorded",
        operator_note=trigger_note,
    )

    append_processing_execution_registry(
        batch_id=batch_id,
        processing_step=processing_step,
        script_name="gui_control_panel_execution_wrapper",
        execution_status="execution_started",
        qa_status="pending",
        output_folder="pending_processing_output",
        note="Controlled GUI processing execution started.",
    )

    append_processing_history_log(
        batch_id=batch_id,
        processing_step=processing_step,
        script_name="gui_control_panel_execution_wrapper",
        result_status="execution_started",
        output_folder="pending_processing_output",
        qa_status="pending",
        note="Controlled GUI processing execution started.",
    )

    execution_status = "execution_completed"
    qa_status = "pending_review"
    output_folder = "pending_processing_output"
    note = (
        operator_note.strip()
        or "Controlled GUI processing execution completed. Downstream processing runner is not yet wired."
    )

    append_processing_execution_registry(
        batch_id=batch_id,
        processing_step=processing_step,
        script_name="gui_control_panel_execution_wrapper",
        execution_status=execution_status,
        qa_status=qa_status,
        output_folder=output_folder,
        note=note,
    )

    append_processing_history_log(
        batch_id=batch_id,
        processing_step=processing_step,
        script_name="gui_control_panel_execution_wrapper",
        result_status=execution_status,
        output_folder=output_folder,
        qa_status=qa_status,
        note=note,
    )

    return GuiProcessingExecutionResult(
        batch_id=batch_id,
        processing_step=processing_step,
        execution_status=execution_status,
        qa_status=qa_status,
        output_folder=output_folder,
        note=note,
    )
