from __future__ import annotations

from pathlib import Path
import pandas as pd


FILES = {
    "data_input/registry/processing_trigger_registry.csv": [
        "trigger_event_ts",
        "batch_id",
        "processing_step",
        "trigger_source",
        "eligibility_status",
        "trigger_status",
        "operator_note",
    ],
    "data_input/registry/processing_execution_registry.csv": [
        "execution_event_ts",
        "batch_id",
        "processing_step",
        "script_name",
        "execution_status",
        "qa_status",
        "output_folder",
        "note",
    ],
    "data_input/registry/processing_history_log.csv": [
        "history_event_ts",
        "batch_id",
        "processing_step",
        "script_name",
        "result_status",
        "output_folder",
        "qa_status",
        "note",
    ],
}


def main() -> None:
    for path_str, required_cols in FILES.items():
        path = Path(path_str)
        assert path.exists(), f"Missing registry file: {path_str}"

        df = pd.read_csv(path)
        missing = [c for c in required_cols if c not in df.columns]
        assert not missing, f"{path_str} missing columns: {missing}"

        assert not df.empty, f"{path_str} is empty"
        assert df["batch_id"].notna().all(), f"{path_str} has null batch_id"
        assert (df["batch_id"].astype(str).str.strip() != "").all(), f"{path_str} has blank batch_id"

        print(f"[OK] {path_str}")
        print(f"[INFO] rows={len(df)} cols={len(df.columns)}")

    print("[OK] Governed processing registries validation passed")


if __name__ == "__main__":
    main()
