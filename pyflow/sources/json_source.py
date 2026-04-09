from __future__ import annotations
from pathlib import Path
from typing import Union

import pandas as pd


class JSONSource:
    """Read a JSON file into a DataFrame.

    Args:
        path: Path to the JSON file.
        orient: Expected JSON format (passed to :func:`pandas.read_json`).
        kwargs: Additional keyword arguments passed to :func:`pandas.read_json`.
    """

    def __init__(
        self,
        path: Union[str, Path],
        orient: str = "records",
        **kwargs,
    ) -> None:
        self.path = Path(path)
        self.orient = orient
        self._kwargs = kwargs

    def read(self) -> pd.DataFrame:
        if not self.path.exists():
            raise FileNotFoundError(f"JSON source not found: {self.path}")
        return pd.read_json(self.path, orient=self.orient, **self._kwargs)

    def __repr__(self) -> str:  # pragma: no cover
        return f"JSONSource(path={str(self.path)!r})"
