from __future__ import annotations
from typing import Dict, Any

import pandas as pd


class CastTransform:
    """Cast one or more columns to specified dtypes.

    Args:
        schema: Dict of ``{column_name: dtype}``.
                Dtype values can be Python types, numpy dtypes, or
                pandas-compatible strings (e.g. ``'int64'``, ``'float32'``,
                ``'datetime64[ns]'``, ``'category'``).

    Example::

        CastTransform({"age": int, "salary": float, "joined": "datetime64[ns]"})
    """

    def __init__(self, schema: Dict[str, Any]) -> None:
        self.schema = schema

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        for col, dtype in self.schema.items():
            if col not in df.columns:
                raise KeyError(f"CastTransform: column '{col}' not found in DataFrame.")
            if str(dtype).startswith("datetime"):
                df[col] = pd.to_datetime(df[col])
            else:
                df[col] = df[col].astype(dtype)
        return df

    def __repr__(self) -> str:  # pragma: no cover
        return f"CastTransform(schema={self.schema!r})"
