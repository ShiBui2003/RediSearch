"""Tests for robots policy behavior."""

import requests

from redisearch.config.settings import CrawlerSettings
from redisearch.crawler.robots import RobotsPolicy


def test_robots_fetch_failure_is_fail_open(monkeypatch):
    def _raise(*args, **kwargs):
        raise requests.RequestException("network down")

    monkeypatch.setattr("redisearch.crawler.robots.requests.get", _raise)

    settings = CrawlerSettings(base_url="https://old.reddit.com")
    policy = RobotsPolicy(settings)

    assert policy.can_fetch("https://old.reddit.com/r/python/new/") is True
