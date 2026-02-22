"""robots.txt policy helpers for crawler safety checks."""

from __future__ import annotations

import logging
from typing import Optional
from urllib.parse import urljoin
from urllib.robotparser import RobotFileParser

import requests

from redisearch.config.settings import CrawlerSettings, get_settings

logger = logging.getLogger(__name__)


class RobotsPolicy:
    """robots.txt policy for deciding whether a URL can be fetched."""

    def __init__(self, settings: Optional[CrawlerSettings] = None) -> None:
        self._settings = settings or get_settings().crawler
        self._robots_url = urljoin(self._settings.base_url, "/robots.txt")
        self._parser = RobotFileParser()
        self._loaded = False
        self._fail_open = True
        self._load()

    def _load(self) -> None:
        headers = {"User-Agent": self._settings.user_agent}
        try:
            response = requests.get(
                self._robots_url,
                timeout=self._settings.request_timeout,
                headers=headers,
            )
            response.raise_for_status()
            self._parser.parse(response.text.splitlines())
            self._loaded = True
            self._fail_open = False
        except requests.RequestException as exc:
            logger.warning("Could not load robots.txt (%s). Continuing fail-open.", exc)
            self._loaded = False
            self._fail_open = True

    def can_fetch(self, url: str) -> bool:
        """Return True if robots policy allows fetching a URL."""
        if self._fail_open:
            return True
        if not self._loaded:
            return True
        return self._parser.can_fetch(self._settings.user_agent, url)
