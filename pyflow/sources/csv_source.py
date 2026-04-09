from __future__ import annotations
from pathlib import Path
from typing import Optional, Union

import pandas as pd


class CSVSource:
    """Read a CSV file into a DataFrame.

    Args:
        path: Path to the CSV file.
        delimiter: Column delimiter (default: ',').
        encoding: File encoding (default: 'utf-8').
        kwargs: Additional keyword arguments passed to :func:`pandas.read_csv`.
    """

    def __init__(
        self,
        path: Union[str, Path],
        delimiter: str = ",",
        encoding: str = "utf-8",
        **kwargs,
    ) -> None:
        self.path = Path(path)
        self.delimiter = delimiter
        self.encoding = encoding
        self._kwargs = kwargs

    def read(self) -> pd.DataFrame:
        if not self.path.exists():
            raise FileNotFoundError(f"CSV source not found: {self.path}")
        return pd.read_csv(
            self.path,
            delimiter=self.delimiter,
            encoding=self.encoding,
            **self._kwargs,
        )

    def __repr__(self) -> str:  # pragma: no cover
        return f"CSVSource(path={str(self.path)!r})"
