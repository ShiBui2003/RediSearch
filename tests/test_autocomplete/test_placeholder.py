"""Tests for the autocomplete Trie data structure."""

from __future__ import annotations

from pathlib import Path

import pytest

from redisearch.autocomplete.trie import Suggestion, Trie


class TestTrieInsertAndSearch:
    """Basic insertion and prefix retrieval."""

    def test_single_term(self):
        t = Trie()
        t.insert("python", score=5.0)
        assert t.size == 1
        results = t.search("py")
        assert len(results) == 1
        assert results[0].term == "python"
        assert results[0].score == 5.0

    def test_multiple_terms_same_prefix(self):
        t = Trie()
        t.insert("python", 10)
        t.insert("pytorch", 8)
        t.insert("pylint", 3)
        results = t.search("py")
        assert len(results) == 3
        assert results[0].term == "python"  # highest score

    def test_case_insensitive(self):
        t = Trie()
        t.insert("Python", 5.0)
        results = t.search("PY")
        assert len(results) == 1
        assert results[0].term == "python"

    def test_no_match_returns_empty(self):
        t = Trie()
        t.insert("python", 5.0)
        assert t.search("java") == []

    def test_empty_prefix_returns_all(self):
        t = Trie()
        t.insert("a", 1)
        t.insert("b", 2)
        results = t.search("")
        assert len(results) == 2

    def test_duplicate_insert_keeps_max_score(self):
        t = Trie()
        t.insert("python", 3.0)
        t.insert("python", 7.0)
        assert t.size == 1
        results = t.search("python")
        assert results[0].score == 7.0

    def test_top_k_limits_results(self):
        t = Trie()
        for i in range(20):
            t.insert(f"prefix{i:02d}", score=float(i))
        results = t.search("prefix", top_k=5)
        assert len(results) == 5
        # Should be top 5 by score (19, 18, 17, 16, 15)
        scores = [r.score for r in results]
        assert scores == sorted(scores, reverse=True)
        assert scores[0] == 19.0


class TestTriePersistence:
    """Save / load round-trip."""

    def test_save_and_load(self, tmp_path: Path):
        t = Trie()
        t.insert("alpha", 10)
        t.insert("alphabet", 5)
        t.insert("beta", 2)

        path = tmp_path / "trie.msgpack"
        t.save(path)

        loaded = Trie.load(path)
        assert loaded.size == 3
        results = loaded.search("alph")
        assert len(results) == 2
        assert results[0].term == "alpha"
        assert results[0].score == 10.0

    def test_load_missing_file_raises(self, tmp_path: Path):
        with pytest.raises(FileNotFoundError):
            Trie.load(tmp_path / "no_such.msgpack")


class TestTrieEdgeCases:
    def test_empty_trie_search(self):
        t = Trie()
        assert t.search("anything") == []
        assert t.size == 0

    def test_single_character_terms(self):
        t = Trie()
        t.insert("a", 1)
        t.insert("ab", 2)
        t.insert("abc", 3)
        results = t.search("a")
        assert len(results) == 3

    def test_exact_match_is_returned(self):
        t = Trie()
        t.insert("test", 5.0)
        results = t.search("test")
        assert len(results) == 1
        assert results[0].term == "test"
