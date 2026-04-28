"""API ingestion source — pulls data from HTTP/REST endpoints into DataFrames.

We use requests with a retry/backoff wrapper because public APIs can be flaky.
JSONPlaceholder and Open-Meteo are both completely free, no API key needed.
"""

import logging
import time
from typing import Optional, Union
from urllib.parse import urlencode

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

# Reasonable defaults — don't hammer free APIs
DEFAULT_TIMEOUT = 30  # seconds
DEFAULT_RETRIES = 3
DEFAULT_BACKOFF = 1.0  # seconds between retries


class APISource:
    """Fetches JSON from an HTTP API and returns a pandas DataFrame.

    Supports GET requests with optional query params, auth headers,
    pagination (offset/page style), and JSON path extraction (e.g. response["data"]).
    """

    def __init__(self, config: dict):
        self.url = config["url"]
        self.method = config.get("method", "GET").upper()
        self.params = config.get("params", {})
        self.headers = config.get("headers", {})
        self.json_path = config.get("json_path", None)  # e.g. "results" or "data.items"
        self.timeout = config.get("timeout", DEFAULT_TIMEOUT)
        self.retries = config.get("retries", DEFAULT_RETRIES)
        self.pagination = config.get("pagination", None)  # dict with page config
        # If the API needs a key, put it in .env — we never hardcode keys
        api_key = config.get("api_key", None)
        if api_key:
            self.headers["Authorization"] = f"Bearer {api_key}"

        self.session = self._build_session()

    def _build_session(self) -> requests.Session:
        """Session with retry/backoff built in — free APIs appreciate this."""
        session = requests.Session()
        retry_strategy = Retry(
            total=self.retries,
            backoff_factor=DEFAULT_BACKOFF,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        session.headers.update(self.headers)
        return session

    def ingest(self) -> pd.DataFrame:
        """Fetch data from the API and return as DataFrame."""
        logger.info("Ingesting from API: %s", self.url)

        if self.pagination:
            raw_data = self._fetch_paginated()
        else:
            raw_data = self._fetch_single()

        if not raw_data:
            logger.warning("API returned empty dataset")
            return pd.DataFrame()

        # If it's a list of records, normalise directly; if it's nested, flatten
        raw_df = pd.json_normalize(raw_data)
        logger.info("API ingestion complete: %d rows × %d cols", len(raw_df), len(raw_df.columns))
        return raw_df

    def _fetch_single(self) -> list:
        """Single-page fetch."""
        resp = self.session.request(
            method=self.method,
            url=self.url,
            params=self.params,
            timeout=self.timeout,
        )
        resp.raise_for_status()
        return self._extract(resp.json())

    def _fetch_paginated(self) -> list:
        """Offset or page-number pagination. Stops when response is empty."""
        page_config = self.pagination
        param_name = page_config.get("param", "page")
        start = page_config.get("start", 1)
        limit = page_config.get("limit", None)
        max_pages = page_config.get("max_pages", 10)  # safety cap so we don't run forever

        all_records = []
        page = start

        for _ in range(max_pages):
            params = {**self.params, param_name: page}
            if limit:
                params["limit"] = limit

            resp = self.session.request(
                method=self.method,
                url=self.url,
                params=params,
                timeout=self.timeout,
            )
            resp.raise_for_status()
            records = self._extract(resp.json())

            if not records:
                logger.info("Pagination done — empty page at page=%d", page)
                break

            all_records.extend(records)
            logger.debug("Fetched page %d — cumulative %d records", page, len(all_records))
            page += 1
            time.sleep(0.1)  # tiny courtesy delay for free APIs

        return all_records

    def _extract(self, data: Union[dict, list]) -> list:
        """Walk json_path to the list of records we care about."""
        if isinstance(data, list):
            return data

        if self.json_path:
            for key in self.json_path.split("."):
                data = data[key]
            if isinstance(data, list):
                return data

        # Wrap single dict in a list
        return [data] if isinstance(data, dict) else []

    def get_source_info(self) -> dict:
        return {
            "type": "api",
            "url": self.url,
            "method": self.method,
        }
