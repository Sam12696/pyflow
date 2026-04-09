from __future__ import annotations
from typing import Dict

import pandas as pd


class RenameTransform:
    """Rename one or more columns.

    Args:
        mapping: Dict of ``{old_name: new_name}``.

    Example::

        RenameTransform({"cust_id": "customer_id", "amt": "amount"})
    """

    def __init__(self, mapping: Dict[str, str]) -> None:
        self.mapping = mapping

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns=self.mapping)

    def __repr__(self) -> str:  # pragma: no cover
        return f"RenameTransform(mapping={self.mapping!r})"
