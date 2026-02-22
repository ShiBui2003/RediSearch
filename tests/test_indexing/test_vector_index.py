"""Tests for VectorIndex build, search, and persistence."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from redisearch.indexing.vector_index import VectorIndex


@pytest.fixture
def sample_embeddings():
    """3 doc embeddings in 8-D space (small for speed)."""
    np.random.seed(42)
    ids = ["d1", "d2", "d3"]
    vecs = np.random.randn(3, 8).astype(np.float32)
    return ids, vecs


class TestVectorIndexBuild:
    def test_doc_count(self, sample_embeddings):
        ids, vecs = sample_embeddings
        idx = VectorIndex(embedding_dim=8)
        idx.build(ids, vecs)
        assert idx.doc_count == 3

    def test_empty(self):
        idx = VectorIndex(embedding_dim=8)
        idx.build([], np.zeros((0, 8), dtype=np.float32))
        assert idx.doc_count == 0


class TestVectorIndexSearch:
    def test_returns_results(self, sample_embeddings):
        ids, vecs = sample_embeddings
        idx = VectorIndex(embedding_dim=8)
        idx.build(ids, vecs)
        results = idx.search(vecs[0], top_k=2)
        assert len(results) >= 1
        assert results[0][0] == "d1"  # closest to itself

    def test_top_k_limits(self, sample_embeddings):
        ids, vecs = sample_embeddings
        idx = VectorIndex(embedding_dim=8)
        idx.build(ids, vecs)
        results = idx.search(vecs[0], top_k=1)
        assert len(results) == 1

    def test_empty_index(self):
        idx = VectorIndex(embedding_dim=8)
        results = idx.search(np.zeros(8, dtype=np.float32), top_k=5)
        assert results == []


class TestVectorIndexPersistence:
    def test_save_and_load(self, sample_embeddings, tmp_path: Path):
        ids, vecs = sample_embeddings
        idx = VectorIndex(embedding_dim=8)
        idx.build(ids, vecs)
        save_dir = tmp_path / "vec_index"
        idx.save(save_dir)

        loaded = VectorIndex.load(save_dir)
        assert loaded.doc_count == 3

    def test_loaded_search_matches(self, sample_embeddings, tmp_path: Path):
        ids, vecs = sample_embeddings
        idx = VectorIndex(embedding_dim=8)
        idx.build(ids, vecs)

        original = idx.search(vecs[0], top_k=3)

        save_dir = tmp_path / "vec_index"
        idx.save(save_dir)
        loaded = VectorIndex.load(save_dir)
        reloaded = loaded.search(vecs[0], top_k=3)

        assert [r[0] for r in original] == [r[0] for r in reloaded]
