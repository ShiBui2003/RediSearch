"""Tests for the PrefixSuggester."""

from __future__ import annotations

from pathlib import Path

import pytest

from redisearch.autocomplete.suggester import PrefixSuggester
from redisearch.autocomplete.trie import Trie
from redisearch.config.settings import AutocompleteSettings


@pytest.fixture
def suggests_env(tmp_path: Path):
    """Pre-build a trie and return (project_root, ac_settings)."""
    trie = Trie()
    trie.insert("python programming", 100)
    trie.insert("pytorch tutorial", 80)
    trie.insert("pylint tips", 30)
    trie.insert("rust ownership", 50)

    ac_dir = tmp_path / "data" / "indexes" / "autocomplete"
    ac_dir.mkdir(parents=True, exist_ok=True)
    trie.save(ac_dir / "all.msgpack")

    # Also save a python-specific trie
    py_trie = Trie()
    py_trie.insert("python web development", 60)
    py_trie.insert("python async await", 40)
    py_trie.save(ac_dir / "python.msgpack")

    ac_settings = AutocompleteSettings(max_suggestions=5)
    return tmp_path, ac_settings


class TestPrefixSuggester:
    def test_basic_suggest(self, suggests_env):
        project_root, ac_settings = suggests_env
        s = PrefixSuggester(ac_settings=ac_settings, project_root=project_root)
        results = s.suggest("py")
        assert len(results) >= 2
        assert results[0].term == "python programming"

    def test_subreddit_specific_trie(self, suggests_env):
        project_root, ac_settings = suggests_env
        s = PrefixSuggester(ac_settings=ac_settings, project_root=project_root)
        results = s.suggest("python", subreddit="python")
        assert len(results) == 2
        terms = {r.term for r in results}
        assert "python web development" in terms
        assert "python async await" in terms

    def test_fallback_to_global(self, suggests_env):
        project_root, ac_settings = suggests_env
        s = PrefixSuggester(ac_settings=ac_settings, project_root=project_root)
        # "rust" subreddit has no trie file — falls back to global
        results = s.suggest("rust", subreddit="rust")
        assert len(results) >= 1
        assert results[0].term == "rust ownership"

    def test_no_trie_returns_empty(self, tmp_path: Path):
        s = PrefixSuggester(
            ac_settings=AutocompleteSettings(),
            project_root=tmp_path,
        )
        assert s.suggest("anything") == []

    def test_top_k_respected(self, suggests_env):
        project_root, ac_settings = suggests_env
        s = PrefixSuggester(ac_settings=ac_settings, project_root=project_root)
        results = s.suggest("py", top_k=1)
        assert len(results) == 1

    def test_cache_is_used(self, suggests_env):
        project_root, ac_settings = suggests_env
        s = PrefixSuggester(ac_settings=ac_settings, project_root=project_root)
        s.suggest("py")
        assert "all" in s._cache
        # Second query uses cache
        results = s.suggest("py")
        assert len(results) >= 2
