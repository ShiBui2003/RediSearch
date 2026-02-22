"""Tests for the AutocompleteBuilder."""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from redisearch.autocomplete.builder import AutocompleteBuilder
from redisearch.autocomplete.trie import Trie
from redisearch.config.settings import AutocompleteSettings
from redisearch.storage.raw_store import RawPostStore
from redisearch.storage.schema import initialize_database

from tests.conftest import make_raw_post


@pytest.fixture
def builder_env(tmp_path: Path):
    """Set up a raw store with sample posts and return (raw_store, project_root)."""
    db_path = tmp_path / "test.db"
    initialize_database(db_path)
    raw_store = RawPostStore(db_path)

    now = int(time.time())
    posts = [
        make_raw_post("t3_a1", subreddit="python", title="Learn Python Basics", score=100, created_utc=now - 86400),
        make_raw_post("t3_a2", subreddit="python", title="Python for Data Science", score=50, created_utc=now - 86400 * 60),
        make_raw_post("t3_a3", subreddit="python", title="Learning Django Framework", score=30, created_utc=now - 86400),
        make_raw_post("t3_b1", subreddit="rust", title="Rust vs Go Performance", score=80, created_utc=now - 86400),
    ]
    for p in posts:
        raw_store.insert(p)

    return raw_store, tmp_path


class TestAutocompleteBuilder:
    def test_build_specific_subreddit(self, builder_env):
        raw_store, project_root = builder_env
        builder = AutocompleteBuilder(
            raw_store=raw_store,
            project_root=project_root,
        )
        summary = builder.build(subreddit="python")
        assert summary["subreddit"] == "python"
        assert summary["term_count"] > 0

        # Verify the trie file was created
        trie_path = Path(summary["file_path"])
        assert trie_path.exists()

    def test_build_all_subreddits(self, builder_env):
        raw_store, project_root = builder_env
        builder = AutocompleteBuilder(
            raw_store=raw_store,
            project_root=project_root,
        )
        summary = builder.build()
        assert summary["subreddit"] == "all"
        assert summary["term_count"] > 0

    def test_recency_boost_applied(self, builder_env):
        raw_store, project_root = builder_env
        ac_settings = AutocompleteSettings(
            recency_days=30,
            recency_multiplier=2.0,
        )
        builder = AutocompleteBuilder(
            raw_store=raw_store,
            ac_settings=ac_settings,
            project_root=project_root,
        )
        summary = builder.build(subreddit="python")

        # Load the trie and check that the recent post has a boosted score
        trie = Trie.load(Path(summary["file_path"]))
        results = trie.search("learn python basics")
        assert len(results) >= 1
        # The title "Learn Python Basics" has score=100, recent → boosted to 200
        assert results[0].score >= 200.0

    def test_individual_words_inserted(self, builder_env):
        raw_store, project_root = builder_env
        builder = AutocompleteBuilder(
            raw_store=raw_store,
            project_root=project_root,
        )
        builder.build(subreddit="python")

        trie_path = project_root / "data" / "indexes" / "autocomplete" / "python.msgpack"
        trie = Trie.load(trie_path)
        # "python" should be in the trie as an individual word
        results = trie.search("python")
        assert len(results) >= 1
