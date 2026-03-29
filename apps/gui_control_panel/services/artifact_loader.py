from __future__ import annotations

from typing import Optional

import pandas as pd

from services.artifact_resolver import resolve_artifacts


def load_artifact_df(artifact_name: str) -> Optional[pd.DataFrame]:
    artifacts = resolve_artifacts()
    spec = artifacts.get(artifact_name)

    if not spec or not spec.get("exists") or not spec.get("resolved_path"):
        return None

    return pd.read_csv(spec["resolved_path"])
