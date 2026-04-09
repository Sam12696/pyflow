from __future__ import annotations
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import requests


class RESTSource:
    """Fetch data from a REST API endpoint and return it as a DataFrame.

    Handles pagination via a ``next_key`` pointer in the JSON response.

    Args:
        url: The API endpoint URL.
        method: HTTP method ('GET' or 'POST').
        headers: Optional request headers (e.g. auth tokens).
        params: Query parameters appended to the URL.
        data_key: JSON key whose value is the list of records.
                  If None, the root of the response is used.
        next_key: JSON key for the next-page URL (enables auto-pagination).
        max_pages: Safety cap on pagination (default 10).
        timeout: Request timeout in seconds.
    """

    def __init__(
        self,
        url: str,
        method: str = "GET",
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        data_key: Optional[str] = None,
        next_key: Optional[str] = None,
        max_pages: int = 10,
        timeout: int = 30,
    ) -> None:
        self.url = url
        self.method = method.upper()
        self.headers = headers or {}
        self.params = params or {}
        self.data_key = data_key
        self.next_key = next_key
        self.max_pages = max_pages
        self.timeout = timeout

    def read(self) -> pd.DataFrame:
        records: List[Dict] = []
        url: Optional[str] = self.url
        page = 0

        while url and page < self.max_pages:
            response = requests.request(
                self.method,
                url,
                headers=self.headers,
                params=self.params if page == 0 else None,
                timeout=self.timeout,
            )
            response.raise_for_status()
            payload = response.json()

            # Extract records
            if self.data_key:
                page_records = payload.get(self.data_key, [])
            else:
                page_records = payload if isinstance(payload, list) else [payload]
            records.extend(page_records)

            # Pagination
            url = payload.get(self.next_key) if self.next_key else None
            page += 1

        return pd.DataFrame(records)

    def __repr__(self) -> str:  # pragma: no cover
        return f"RESTSource(url={self.url!r})"
