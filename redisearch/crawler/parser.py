"""HTML parsing utilities for old.reddit listing pages."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Optional
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from redisearch.storage.models import RawPost


@dataclass
class ParsedListingPage:
    """Parsed listing page result containing posts and next-page URL."""

    posts: list[RawPost]
    next_url: Optional[str]


class ListingPageParser:
    """Parser for old.reddit listing pages."""

    def parse(
        self,
        html: str,
        subreddit: str,
        page_url: Optional[str] = None,
    ) -> ParsedListingPage:
        """Parse subreddit listing HTML into raw-post skeleton entries."""
        soup = BeautifulSoup(html, "lxml")
        posts: list[RawPost] = []

        for thing in soup.select("div.thing"):
            post_id = thing.get("data-fullname") or ""
            if not post_id.startswith("t3_"):
                continue

            permalink = thing.get("data-permalink") or ""
            if not permalink:
                comments_link = thing.select_one("a.comments")
                permalink = comments_link.get("href", "") if comments_link else ""
                parsed_link = urlparse(permalink)
                if parsed_link.scheme or parsed_link.netloc:
                    permalink = parsed_link.path

            title_node = thing.select_one("a.title")
            title = title_node.get_text(strip=True) if title_node else ""

            author_node = thing.select_one("a.author")
            author = author_node.get_text(strip=True) if author_node else None

            score_node = thing.select_one("div.score")
            score = self._parse_score(
                thing.get("data-score")
                or (score_node.get("title") if score_node else None)
                or (score_node.get_text(strip=True) if score_node else None)
            )

            comments_node = thing.select_one("a.comments")
            comment_count = self._parse_comment_count(
                comments_node.get_text(strip=True) if comments_node else None
            )

            created_utc = self._parse_created_utc(thing.select_one("time"))

            classes = set(thing.get("class", []))
            post_type = "self" if "self" in classes else "link"

            body_node = thing.select_one("div.expando div.usertext-body")
            body = body_node.get_text(" ", strip=True) if body_node else None

            posts.append(
                RawPost(
                    id=post_id,
                    subreddit=(thing.get("data-subreddit") or subreddit or "").lower(),
                    permalink=permalink,
                    title=title,
                    body=body,
                    author=author,
                    score=score,
                    comment_count=comment_count,
                    created_utc=created_utc,
                    post_type=post_type,
                    raw_html=str(thing).encode("utf-8"),
                )
            )

        next_url = None
        next_link = soup.select_one("span.next-button a")
        if next_link:
            next_href = next_link.get("href")
            if next_href:
                next_url = urljoin(page_url or "", next_href)

        return ParsedListingPage(posts=posts, next_url=next_url)

    @staticmethod
    def _parse_score(value: Optional[str]) -> int:
        if not value:
            return 0
        match = re.search(r"(\d+)", value.replace(",", ""))
        return int(match.group(1)) if match else 0

    @staticmethod
    def _parse_comment_count(value: Optional[str]) -> int:
        if not value:
            return 0
        if "comment" not in value.lower():
            return 0
        match = re.search(r"(\d+)", value.replace(",", ""))
        return int(match.group(1)) if match else 0

    @staticmethod
    def _parse_created_utc(time_node) -> int:
        if not time_node:
            return 0

        datetime_value = time_node.get("datetime")
        if datetime_value:
            try:
                dt = datetime.fromisoformat(datetime_value.replace("Z", "+00:00"))
                return int(dt.timestamp())
            except ValueError:
                pass

        title_value = time_node.get("title")
        if title_value:
            try:
                dt = parsedate_to_datetime(title_value)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return int(dt.timestamp())
            except (TypeError, ValueError):
                pass

        return 0
