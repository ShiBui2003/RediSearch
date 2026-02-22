"""Integration tests for the API endpoints using FastAPI TestClient."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from redisearch.api.app import create_app
from redisearch.config.settings import Settings
from redisearch.search.bm25_searcher import BM25SearchHit
from redisearch.storage.models import RawPost
from redisearch.storage.schema import initialize_database


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    """Settings pointing at a temp directory."""
    s = Settings(project_root=tmp_path)
    s.ensure_dirs()
    return s


@pytest.fixture
def app(settings: Settings):
    """Create the app with real stores (empty DB) but a mocked searcher."""
    application = create_app(settings)
    # Replace the searcher with a mock for predictable results
    application.state.searcher = MagicMock()
    application.state.searcher.search.return_value = []
    return application


@pytest.fixture
def client(app) -> TestClient:
    return TestClient(app)


# ---------------------------------------------------------------------------
# Health / Stats
# ---------------------------------------------------------------------------


class TestHealth:
    def test_returns_ok(self, client: TestClient):
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"


class TestStats:
    def test_empty_db(self, client: TestClient):
        resp = client.get("/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert data["raw_post_count"] == 0
        assert data["processed_post_count"] == 0
        assert data["subreddits"] == []
        assert data["active_indexes"] == 0


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------


def _make_raw_post(post_id: str = "t3_abc") -> RawPost:
    return RawPost(
        id=post_id,
        subreddit="python",
        permalink="/r/python/comments/abc/test/",
        title="Test Title",
        body="Test body",
        author="tester",
        score=10,
        comment_count=5,
        created_utc=1700000000,
        crawled_at="2025-01-01T00:00:00+00:00",
        raw_html=b"<html>test</html>",
        post_type="self",
    )


class TestSearch:
    def test_empty_results(self, client: TestClient):
        resp = client.get("/search", params={"q": "python"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["query"] == "python"
        assert data["hits"] == []
        assert data["total_hits"] == 0

    def test_returns_hits(self, app, client: TestClient):
        # Seed raw_store with a post so the enrichment works
        raw = _make_raw_post("t3_hit1")
        app.state.raw_store.insert(raw)

        # Mock searcher to return a hit
        app.state.searcher.search.return_value = [
            BM25SearchHit(id="t3_hit1", score=2.5, shard_id="shard_python"),
        ]

        resp = client.get("/search", params={"q": "python"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_hits"] == 1
        assert data["hits"][0]["id"] == "t3_hit1"
        assert data["hits"][0]["title"] == "Test Title"
        assert data["hits"][0]["score"] == 2.5

    def test_query_too_short(self, client: TestClient):
        resp = client.get("/search", params={"q": ""})
        assert resp.status_code == 422  # validation error

    def test_pagination_cursor(self, app, client: TestClient):
        # Seed 3 raw posts + mock 3 hits
        for i in range(3):
            app.state.raw_store.insert(_make_raw_post(f"t3_p{i}"))
        app.state.searcher.search.return_value = [
            BM25SearchHit(id=f"t3_p{i}", score=3.0 - i, shard_id="shard_python")
            for i in range(3)
        ]

        # Page 1
        resp = client.get("/search", params={"q": "test", "page_size": 2})
        data = resp.json()
        assert len(data["hits"]) == 2
        assert data["next_cursor"] is not None

        # Page 2
        resp2 = client.get(
            "/search", params={"q": "test", "page_size": 2, "cursor": data["next_cursor"]}
        )
        data2 = resp2.json()
        assert len(data2["hits"]) == 1
        assert data2["next_cursor"] is None

    def test_rate_limit_rejects_excess(self, app, client: TestClient):
        # Set search rate limiter to capacity=2, no refill
        from redisearch.api.rate_limiter import RateLimiter

        app.state.search_rate_limiter = RateLimiter(capacity=2, refill_rate=0.0)

        assert client.get("/search", params={"q": "a"}).status_code == 200
        assert client.get("/search", params={"q": "b"}).status_code == 200
        assert client.get("/search", params={"q": "c"}).status_code == 429

    def test_subreddit_filter_passed(self, app, client: TestClient):
        resp = client.get("/search", params={"q": "test", "subreddit": "python"})
        assert resp.status_code == 200
        app.state.searcher.search.assert_called_once()
        call_kwargs = app.state.searcher.search.call_args
        assert call_kwargs[1].get("subreddit") == "python" or call_kwargs[0][1] == "python"
