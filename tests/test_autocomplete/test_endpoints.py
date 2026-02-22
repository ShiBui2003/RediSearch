"""Tests for the /autocomplete endpoint."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from redisearch.api.app import create_app
from redisearch.autocomplete.trie import Suggestion
from redisearch.config.settings import Settings


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    s = Settings(project_root=tmp_path)
    s.ensure_dirs()
    return s


@pytest.fixture
def app(settings: Settings):
    application = create_app(settings)
    # Replace searcher + suggester with mocks
    application.state.searcher = MagicMock()
    application.state.searcher.search.return_value = []

    mock_suggester = MagicMock()
    mock_suggester.suggest.return_value = [
        Suggestion(term="python tutorial", score=100.0),
        Suggestion(term="pytorch setup", score=80.0),
    ]
    application.state.suggester = mock_suggester
    return application


@pytest.fixture
def client(app) -> TestClient:
    return TestClient(app)


class TestAutocompleteEndpoint:
    def test_returns_suggestions(self, client: TestClient):
        resp = client.get("/autocomplete", params={"q": "py"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["prefix"] == "py"
        assert len(data["suggestions"]) == 2
        assert data["suggestions"][0]["term"] == "python tutorial"

    def test_empty_query_rejected(self, client: TestClient):
        resp = client.get("/autocomplete", params={"q": ""})
        assert resp.status_code == 422

    def test_subreddit_param_passed(self, app, client: TestClient):
        resp = client.get("/autocomplete", params={"q": "py", "subreddit": "python"})
        assert resp.status_code == 200
        app.state.suggester.suggest.assert_called_once_with(
            "py", subreddit="python", top_k=10,
        )

    def test_rate_limit_enforced(self, app, client: TestClient):
        # Drain rate limit tokens
        rl = app.state.autocomplete_rate_limiter
        for _ in range(200):
            rl.is_allowed("testclient")

        resp = client.get("/autocomplete", params={"q": "py"})
        assert resp.status_code == 429
