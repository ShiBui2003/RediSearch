"""HTTP client for crawler requests with retry and backoff."""

from __future__ import annotations

import time
from typing import Optional

import requests

from redisearch.config.settings import CrawlerSettings, get_settings


class CrawlerHttpClient:
    """HTTP client used by the crawler to fetch HTML pages."""

    _RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

    def __init__(self, settings: Optional[CrawlerSettings] = None) -> None:
        self._settings = settings or get_settings().crawler
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": self._settings.user_agent})

    def get(self, url: str) -> str:
        """Fetch a URL and return response HTML text."""
        last_error: Optional[Exception] = None

        for attempt in range(self._settings.max_retries + 1):
            try:
                response = self._session.get(url, timeout=self._settings.request_timeout)
            except requests.RequestException as exc:
                last_error = exc
                if attempt >= self._settings.max_retries:
                    break
                self._sleep_with_backoff(attempt)
                continue

            if response.status_code in self._RETRYABLE_STATUS_CODES:
                last_error = RuntimeError(
                    f"Retryable HTTP status {response.status_code} for URL: {url}"
                )
                if attempt >= self._settings.max_retries:
                    break
                self._sleep_with_backoff(attempt)
                continue

            if response.status_code >= 400:
                raise RuntimeError(
                    f"HTTP request failed with status {response.status_code} for URL: {url}"
                )

            return response.text

        raise RuntimeError(
            f"Failed to fetch URL after {self._settings.max_retries + 1} attempts: {url}"
        ) from last_error

    def _sleep_with_backoff(self, attempt: int) -> None:
        backoff = min(
            self._settings.backoff_base * (2 ** attempt),
            self._settings.max_backoff,
        )
        time.sleep(backoff)
