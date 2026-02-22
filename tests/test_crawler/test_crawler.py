"""Tests for subreddit crawler orchestration behavior."""

from redisearch.config.settings import CrawlerSettings
from redisearch.crawler.crawler import SubredditCrawler
from redisearch.crawler.parser import ParsedListingPage
from redisearch.storage.models import RawPost


class _FakeRawStore:
    def __init__(self) -> None:
        self._existing = {"t3_dup"}
        self.inserted: list[RawPost] = []

    def exists(self, post_id: str) -> bool:
        return post_id in self._existing

    def insert_many(self, posts: list[RawPost]) -> int:
        self.inserted.extend(posts)
        for post in posts:
            self._existing.add(post.id)
        return len(posts)


class _FakeHttpClient:
    def get(self, url: str) -> str:
        return f"<html>{url}</html>"


class _FakeRobots:
    def can_fetch(self, url: str) -> bool:
        return True


class _FakeParser:
    def __init__(self) -> None:
        self._calls = 0

    def parse(self, html: str, subreddit: str, page_url: str):
        self._calls += 1
        if self._calls == 1:
            return ParsedListingPage(
                posts=[
                    RawPost(
                        id="t3_dup",
                        subreddit=subreddit,
                        permalink="/r/python/comments/dup/post/",
                        title="Duplicate",
                    ),
                    RawPost(
                        id="t3_new1",
                        subreddit=subreddit,
                        permalink="/r/python/comments/new1/post/",
                        title="New 1",
                    ),
                ],
                next_url="https://old.reddit.com/r/python/new/?count=25&after=t3_new1",
            )

        return ParsedListingPage(
            posts=[
                RawPost(
                    id="t3_new2",
                    subreddit=subreddit,
                    permalink="/r/python/comments/new2/post/",
                    title="New 2",
                )
            ],
            next_url=None,
        )


def test_crawl_subreddit_dedup_and_insert_flow():
    fake_store = _FakeRawStore()
    crawler = SubredditCrawler(
        raw_store=fake_store,
        http_client=_FakeHttpClient(),
        robots_policy=_FakeRobots(),
        parser=_FakeParser(),
        settings=CrawlerSettings(base_url="https://old.reddit.com", min_delay=0.0, max_jitter=0.0),
        sleep_func=lambda seconds: None,
        random_func=lambda a, b: 0.0,
    )

    summary = crawler.crawl_subreddit("python", max_pages=2)

    assert summary == {
        "subreddit": "python",
        "pages_crawled": 2,
        "posts_seen": 3,
        "posts_inserted": 2,
        "duplicates": 1,
    }
    assert [post.id for post in fake_store.inserted] == ["t3_new1", "t3_new2"]
