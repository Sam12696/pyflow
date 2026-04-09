from __future__ import annotations
from typing import Optional

import pandas as pd


class StdoutSink:
    """Print a DataFrame to stdout (useful for debugging and demos).

    Args:
        max_rows: Maximum rows to display (default: 20, None = all).
        max_cols: Maximum columns to display (None = all).
        title: Optional header printed before the table.
    """

    def __init__(
        self,
        max_rows: Optional[int] = 20,
        max_cols: Optional[int] = None,
        title: Optional[str] = None,
    ) -> None:
        self.max_rows = max_rows
        self.max_cols = max_cols
        self.title = title

    def write(self, df: pd.DataFrame) -> None:
        if self.title:
            print(f"\n{'=' * 60}")
            print(f"  {self.title}")
            print(f"{'=' * 60}")

        with pd.option_context(
            "display.max_rows", self.max_rows,
            "display.max_columns", self.max_cols,
        ):
            print(df.to_string(index=False))

        print(f"\n[{len(df)} rows x {len(df.columns)} columns]")

    def __repr__(self) -> str:  # pragma: no cover
        return "StdoutSink()"
