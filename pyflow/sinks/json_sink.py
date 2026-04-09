from __future__ import annotations
from pathlib import Path
from typing import Union

import pandas as pd


class JSONSink:
    """Write a DataFrame to a JSON file.

    Args:
        path: Destination file path.
        orient: JSON format (default: 'records' — list of dicts).
        indent: Indentation for pretty-printing (default: 2).
        kwargs: Additional keyword arguments passed to :meth:`DataFrame.to_json`.
    """

    def __init__(
        self,
        path: Union[str, Path],
        orient: str = "records",
        indent: int = 2,
        **kwargs,
    ) -> None:
        self.path = Path(path)
        self.orient = orient
        self.indent = indent
        self._kwargs = kwargs

    def write(self, df: pd.DataFrame) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        df.to_json(
            self.path,
            orient=self.orient,
            indent=self.indent,
            **self._kwargs,
        )

    def __repr__(self) -> str:  # pragma: no cover
        return f"JSONSink(path={str(self.path)!r})"
