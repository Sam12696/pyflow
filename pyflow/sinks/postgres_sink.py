from __future__ import annotations
from typing import Literal, Optional

import pandas as pd


class PostgresSink:
    """Write a DataFrame to a PostgreSQL table.

    Requires ``sqlalchemy`` and ``psycopg2-binary``.

    Args:
        connection_string: SQLAlchemy DSN.
        table: Destination table name.
        if_exists: Action if table exists — 'fail', 'replace', or 'append'.
        index: Whether to write the DataFrame index as a column.
        chunksize: Rows per INSERT batch (None = all at once).
        schema: Target schema (default: None → public).
    """

    def __init__(
        self,
        connection_string: str,
        table: str,
        if_exists: Literal["fail", "replace", "append"] = "append",
        index: bool = False,
        chunksize: Optional[int] = 1000,
        schema: Optional[str] = None,
    ) -> None:
        self.connection_string = connection_string
        self.table = table
        self.if_exists = if_exists
        self.index = index
        self.chunksize = chunksize
        self.schema = schema

    def write(self, df: pd.DataFrame) -> None:
        try:
            from sqlalchemy import create_engine
        except ImportError as exc:
            raise ImportError(
                "PostgresSink requires sqlalchemy. Install it with: "
                "pip install sqlalchemy psycopg2-binary"
            ) from exc

        engine = create_engine(self.connection_string)
        df.to_sql(
            self.table,
            engine,
            if_exists=self.if_exists,
            index=self.index,
            chunksize=self.chunksize,
            schema=self.schema,
        )

    def __repr__(self) -> str:  # pragma: no cover
        return f"PostgresSink(table={self.table!r})"
