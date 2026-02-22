"""Reddit JSON API crawler — fetches posts via reddit.com JSON endpoints.

Uses the public JSON API (appending ``.json`` to listing URLs) which does not
require authentication and is not blocked by robots.txt.  This replaces HTML
scraping for the pipeline so that any subreddit typed by the user actually
returns real posts.
"""

from __future__ import annotations

import logging
import random
import time
from typing import Callable, Optional

import requests

from redisearch.config.settings import CrawlerSettings, get_settings
from redisearch.storage.models import RawPost
from redisearch.storage.raw_store import RawPostStore

logger = logging.getLogger(__name__)


class RedditJsonCrawler:
    """Crawl a subreddit using Reddit's public JSON API."""

    _BASE = "https://www.reddit.com"

    def __init__(
        self,
        raw_store: Optional[RawPostStore] = None,
        settings: Optional[CrawlerSettings] = None,
        sleep_func: Callable[[float], None] = time.sleep,
        random_func: Callable[[float, float], float] = random.uniform,
    ) -> None:
        self._settings = settings or get_settings().crawler
        self._raw_store = raw_store or RawPostStore()
        self._sleep = sleep_func
        self._random = random_func
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": self._settings.user_agent,
            "Accept": "application/json",
        })

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def crawl_subreddit(
        self,
        subreddit: str,
        max_pages: Optional[int] = None,
    ) -> dict:
        """Fetch /new listing pages for *subreddit* and persist unseen posts.

        Returns a summary dict compatible with the HTML-based crawler so the
        pipeline pipeline progress UI keeps working unchanged.
        """
        subreddit_name = subreddit.strip().lower()
        max_pages_to_crawl = max_pages or self._settings.max_pages_per_subreddit
        after_token: Optional[str] = None

        pages_crawled = 0
        posts_seen = 0
        posts_inserted = 0
        duplicates = 0

        while pages_crawled < max_pages_to_crawl:
            url = f"{self._BASE}/r/{subreddit_name}/new.json?limit=25&raw_json=1"
            if after_token:
                url += f"&after={after_token}"

            logger.info("Fetching JSON page %d for r/%s: %s", pages_crawled + 1, subreddit_name, url)

            try:
                resp = self._session.get(url, timeout=self._settings.request_timeout)
                resp.raise_for_status()
                data = resp.json()
            except (requests.RequestException, ValueError) as exc:
                logger.error("Failed to fetch JSON for r/%s: %s", subreddit_name, exc)
                break

            listing = data.get("data", {})
            children = listing.get("children", [])

            if not children:
                logger.info("No more posts for r/%s", subreddit_name)
                break

            pages_crawled += 1
            posts_seen += len(children)

            new_posts: list[RawPost] = []
            for child in children:
                post_data = child.get("data", {})
                post = self._json_to_raw_post(post_data, subreddit_name)
                if post is None:
                    continue
                if self._raw_store.exists(post.id):
                    duplicates += 1
                    continue
                new_posts.append(post)

            if new_posts:
                inserted_count = self._raw_store.insert_many(new_posts)
                posts_inserted += inserted_count
                duplicates += max(0, len(new_posts) - inserted_count)

            after_token = listing.get("after")
            if not after_token:
                break

            # Polite delay between pages
            delay = self._settings.min_delay + self._random(0.0, self._settings.max_jitter)
            self._sleep(delay)

        summary = {
            "subreddit": subreddit_name,
            "pages_crawled": pages_crawled,
            "posts_seen": posts_seen,
            "posts_inserted": posts_inserted,
            "duplicates": duplicates,
        }
        logger.info("Crawl complete for r/%s: %s", subreddit_name, summary)
        return summary

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _json_to_raw_post(d: dict, fallback_sub: str) -> Optional[RawPost]:
        """Convert a single JSON post (child.data) to a RawPost."""
        post_id = d.get("name", "")  # e.g. "t3_xyz"
        if not post_id.startswith("t3_"):
            return None

        title = d.get("title", "")
        if not title:
            return None

        body = d.get("selftext") or None
        if body and body == "[removed]":
            body = None

        permalink = d.get("permalink", "")
        author = d.get("author")
        if author == "[deleted]":
            author = None

        score = d.get("score", 0)
        comment_count = d.get("num_comments", 0)
        created_utc = int(d.get("created_utc", 0))
        is_self = d.get("is_self", True)
        post_type = "self" if is_self else "link"
        subreddit = (d.get("subreddit") or fallback_sub).lower()

        return RawPost(
            id=post_id,
            subreddit=subreddit,
            permalink=permalink,
            title=title,
            body=body,
            author=author,
            score=score,
            comment_count=comment_count,
            created_utc=created_utc,
            post_type=post_type,
            raw_html=None,
        )
