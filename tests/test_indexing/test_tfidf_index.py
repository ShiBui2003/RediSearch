"""Tests for TF-IDF index build, score, and persistence."""

from __future__ import annotations

from pathlib import Path

import pytest

from redisearch.indexing.tfidf_index import TFIDFIndex


@pytest.fixture
def sample_docs() -> dict[str, list[str]]:
    return {
        "d1": ["python", "decor", "advanc"],
        "d2": ["python", "basic", "tutori"],
        "d3": ["javascript", "closur", "advanc"],
    }


class TestTFIDFBuild:
    def test_doc_count(self, sample_docs):
        idx = TFIDFIndex()
        idx.build(sample_docs)
        assert idx.doc_count == 3

    def test_empty_corpus(self):
        idx = TFIDFIndex()
        idx.build({})
        assert idx.doc_count == 0

    def test_score_returns_results(self, sample_docs):
        idx = TFIDFIndex()
        idx.build(sample_docs)
        results = idx.score(["python", "decor"], top_k=3)
        assert len(results) > 0
        assert results[0][0] == "d1"  # most relevant

    def test_score_empty_query(self, sample_docs):
        idx = TFIDFIndex()
        idx.build(sample_docs)
        assert idx.score([], top_k=3) == []

    def test_unknown_term(self, sample_docs):
        idx = TFIDFIndex()
        idx.build(sample_docs)
        results = idx.score(["nonexistent_xyz"], top_k=3)
        assert len(results) == 0

    def test_top_k_limits(self, sample_docs):
        idx = TFIDFIndex()
        idx.build(sample_docs)
        results = idx.score(["python"], top_k=1)
        assert len(results) <= 1


class TestTFIDFPersistence:
    def test_save_and_load(self, sample_docs, tmp_path: Path):
        idx = TFIDFIndex()
        idx.build(sample_docs)
        path = tmp_path / "tfidf.msgpack"
        idx.save(path)

        loaded = TFIDFIndex.load(path)
        assert loaded.doc_count == 3

    def test_loaded_scores_match(self, sample_docs, tmp_path: Path):
        idx = TFIDFIndex()
        idx.build(sample_docs)
        original = idx.score(["python", "decor"], top_k=3)

        path = tmp_path / "tfidf.msgpack"
        idx.save(path)
        loaded = TFIDFIndex.load(path)
        reloaded = loaded.score(["python", "decor"], top_k=3)

        assert len(original) == len(reloaded)
        for (o_id, o_score), (r_id, r_score) in zip(original, reloaded):
            assert o_id == r_id
            assert abs(o_score - r_score) < 1e-4
