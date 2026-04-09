from __future__ import annotations
from typing import Any, Dict, Optional
import pandas as pd


class PipelineContext:
    """Runtime context passed between tasks in a pipeline.

    Holds the working DataFrame plus arbitrary metadata that tasks
    can read and write as the pipeline progresses.
    """

    def __init__(self, pipeline_name: str) -> None:
        self.pipeline_name = pipeline_name
        self.df: Optional[pd.DataFrame] = None
        self._meta: Dict[str, Any] = {}

    # ------------------------------------------------------------------
    # Metadata helpers
    # ------------------------------------------------------------------

    def set(self, key: str, value: Any) -> None:
        """Store an arbitrary value in the context."""
        self._meta[key] = value

    def get(self, key: str, default: Any = None) -> Any:
        """Retrieve a value from the context."""
        return self._meta.get(key, default)

    def __repr__(self) -> str:  # pragma: no cover
        rows = len(self.df) if self.df is not None else 0
        cols = list(self.df.columns) if self.df is not None else []
        return (
            f"PipelineContext(pipeline={self.pipeline_name!r}, "
            f"rows={rows}, columns={cols})"
        )
