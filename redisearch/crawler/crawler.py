"""Subreddit crawler orchestration logic."""

from __future__ import annotations

import logging
import random
import time
from typing import Callable, Optional
from urllib.parse import urljoin

from redisearch.config.settings import CrawlerSettings, get_settings
from redisearch.crawler.http_client import CrawlerHttpClient
from redisearch.crawler.parser import ListingPageParser
from redisearch.crawler.robots import RobotsPolicy
from redisearch.storage.raw_store import RawPostStore

logger = logging.getLogger(__name__)


class SubredditCrawler:
    """Crawler for collecting raw posts from a subreddit listing."""

    def __init__(
        self,
        raw_store: Optional[RawPostStore] = None,
        http_client: Optional[CrawlerHttpClient] = None,
        robots_policy: Optional[RobotsPolicy] = None,
        parser: Optional[ListingPageParser] = None,
        settings: Optional[CrawlerSettings] = None,
        sleep_func: Callable[[float], None] = time.sleep,
        random_func: Callable[[float, float], float] = random.uniform,
    ) -> None:
        self._settings = settings or get_settings().crawler
        self._raw_store = raw_store or RawPostStore()
        self._http_client = http_client or CrawlerHttpClient(self._settings)
        self._robots_policy = robots_policy or RobotsPolicy(self._settings)
        self._parser = parser or ListingPageParser()
        self._sleep = sleep_func
        self._random = random_func

    def crawl_subreddit(self, subreddit: str, max_pages: Optional[int] = None) -> dict:
        """Crawl /new listing pages for a subreddit and persist unseen posts."""
        subreddit_name = subreddit.strip().lower()
        max_pages_to_crawl = max_pages or self._settings.max_pages_per_subreddit
        current_url = urljoin(self._settings.base_url, f"/r/{subreddit_name}/new/")

        pages_crawled = 0
        posts_seen = 0
        posts_inserted = 0
        duplicates = 0

        while current_url and pages_crawled < max_pages_to_crawl:
            if not self._robots_policy.can_fetch(current_url):
                logger.info("Skipping URL blocked by robots policy: %s", current_url)
                break

            html = self._http_client.get(current_url)
            parsed_page = self._parser.parse(html, subreddit_name, page_url=current_url)

            pages_crawled += 1
            posts_seen += len(parsed_page.posts)

            new_posts = []
            for post in parsed_page.posts:
                if self._raw_store.exists(post.id):
                    duplicates += 1
                    continue
                new_posts.append(post)

            if new_posts:
                inserted_count = self._raw_store.insert_many(new_posts)
                posts_inserted += inserted_count
                duplicates += max(0, len(new_posts) - inserted_count)

            if not parsed_page.next_url:
                break

            current_url = parsed_page.next_url

            delay = self._settings.min_delay + self._random(0.0, self._settings.max_jitter)
            self._sleep(delay)

        return {
            "subreddit": subreddit_name,
            "pages_crawled": pages_crawled,
            "posts_seen": posts_seen,
            "posts_inserted": posts_inserted,
            "duplicates": duplicates,
        }
