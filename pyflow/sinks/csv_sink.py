from __future__ import annotations
from pathlib import Path
from typing import Union

import pandas as pd


class CSVSink:
    """Write a DataFrame to a CSV file.

    Args:
        path: Destination file path.
        index: Whether to write the DataFrame index (default: False).
        delimiter: Column delimiter (default: ',').
        encoding: File encoding (default: 'utf-8').
        kwargs: Additional keyword arguments passed to :meth:`DataFrame.to_csv`.
    """

    def __init__(
        self,
        path: Union[str, Path],
        index: bool = False,
        delimiter: str = ",",
        encoding: str = "utf-8",
        **kwargs,
    ) -> None:
        self.path = Path(path)
        self.index = index
        self.delimiter = delimiter
        self.encoding = encoding
        self._kwargs = kwargs

    def write(self, df: pd.DataFrame) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(
            self.path,
            index=self.index,
            sep=self.delimiter,
            encoding=self.encoding,
            **self._kwargs,
        )

    def __repr__(self) -> str:  # pragma: no cover
        return f"CSVSink(path={str(self.path)!r})"
