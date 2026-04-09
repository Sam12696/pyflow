from __future__ import annotations
from typing import Callable, Union

import pandas as pd


class FilterTransform:
    """Keep only rows that satisfy a condition.

    Args:
        condition: Either a callable ``(df) -> bool_series`` or a
                   query string passed to :meth:`DataFrame.query`.

    Examples::

        FilterTransform(lambda df: df["age"] >= 18)
        FilterTransform("age >= 18 and status == 'active'")
    """

    def __init__(self, condition: Union[Callable[[pd.DataFrame], pd.Series], str]) -> None:
        self.condition = condition

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        if callable(self.condition):
            mask = self.condition(df)
            return df[mask].reset_index(drop=True)
        return df.query(self.condition).reset_index(drop=True)

    def __repr__(self) -> str:  # pragma: no cover
        return f"FilterTransform(condition={self.condition!r})"
