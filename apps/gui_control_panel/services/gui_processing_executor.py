from __future__ import annotations

from dataclasses import dataclass

from services.processing_history_logger import append_processing_history


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

    append_processing_history(
        batch_id=batch_id,
        processing_step=processing_step,
        script_name="gui_control_panel_execution_wrapper",
        result_status="execution_started",
        output_folder="pending_processing_output",
        qa_status="pending",
        note=operator_note.strip() or "Controlled GUI processing execution started.",
    )

    execution_status = "execution_completed"
    qa_status = "pending_review"
    output_folder = "pending_processing_output"
    note = (
        operator_note.strip()
        or "Controlled GUI processing execution completed. Downstream processing runner is not yet wired."
    )

    append_processing_history(
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
