from __future__ import annotations
from typing import Callable, Dict, Optional, Union

import pandas as pd


class MapTransform:
    """Apply a function to one or more columns, or to the entire DataFrame.

    Args:
        fn: Transformation function.
        column: If given, apply ``fn`` to that single column only.
                If None, ``fn`` receives and returns the whole DataFrame.

    Examples::

        MapTransform(str.upper, column="name")
        MapTransform(lambda df: df.assign(full_name=df["first"] + " " + df["last"]))
    """

    def __init__(
        self,
        fn: Callable,
        column: Optional[str] = None,
    ) -> None:
        self.fn = fn
        self.column = column

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.column:
            df = df.copy()
            df[self.column] = df[self.column].map(self.fn)
            return df
        return self.fn(df)

    def __repr__(self) -> str:  # pragma: no cover
        return f"MapTransform(column={self.column!r})"
