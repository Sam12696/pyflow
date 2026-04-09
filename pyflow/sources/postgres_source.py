from __future__ import annotations
from typing import Any, Dict, Optional, Union

import pandas as pd


class PostgresSource:
    """Read data from a PostgreSQL table or query into a DataFrame.

    Requires ``sqlalchemy`` and ``psycopg2-binary``.

    Args:
        connection_string: SQLAlchemy DSN, e.g.
            ``'postgresql://user:pass@host:5432/dbname'``.
        query: SQL query or table name to read from.
        is_table: If True, ``query`` is treated as a table name
                  and wrapped in ``SELECT * FROM {query}``.
        params: Optional dict of bind parameters for the query.
        chunksize: If set, reads in chunks (returns the first chunk only
                   for simplicity; override :meth:`read` for streaming).
    """

    def __init__(
        self,
        connection_string: str,
        query: str,
        is_table: bool = False,
        params: Optional[Dict[str, Any]] = None,
        chunksize: Optional[int] = None,
    ) -> None:
        self.connection_string = connection_string
        self.query = f"SELECT * FROM {query}" if is_table else query
        self.params = params
        self.chunksize = chunksize

    def read(self) -> pd.DataFrame:
        try:
            from sqlalchemy import create_engine, text
        except ImportError as exc:
            raise ImportError(
                "PostgresSource requires sqlalchemy. Install it with: "
                "pip install sqlalchemy psycopg2-binary"
            ) from exc

        engine = create_engine(self.connection_string)
        with engine.connect() as conn:
            return pd.read_sql(
                text(self.query),
                conn,
                params=self.params,
            )

    def __repr__(self) -> str:  # pragma: no cover
        return f"PostgresSource(query={self.query!r})"
