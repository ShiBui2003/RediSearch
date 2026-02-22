"""Tests for BM25InvertedIndex build, score, and save/load."""

from pathlib import Path

from redisearch.indexing.bm25_index import BM25InvertedIndex


# ---------------------------------------------------------------------------
# Sample corpus shared across tests
# ---------------------------------------------------------------------------

_DOCS = {
    "d1": ["python", "decorators", "advanced", "python"],
    "d2": ["python", "basics", "tutorial"],
    "d3": ["javascript", "closures", "tutorial"],
}


class TestBM25Build:
    """Tests for building the inverted index."""

    def test_build_sets_doc_count(self):
        idx = BM25InvertedIndex()
        idx.build(_DOCS)
        assert idx.doc_count == 3

    def test_build_sets_avg_doc_len(self):
        idx = BM25InvertedIndex()
        idx.build(_DOCS)
        expected = (4 + 3 + 3) / 3
        assert abs(idx.avg_doc_len - expected) < 1e-9

    def test_postings_contain_all_terms(self):
        idx = BM25InvertedIndex()
        idx.build(_DOCS)
        assert "python" in idx.postings
        assert "javascript" in idx.postings
        assert "tutorial" in idx.postings

    def test_term_frequency_correct(self):
        idx = BM25InvertedIndex()
        idx.build(_DOCS)
        assert idx.postings["python"]["d1"] == 2
        assert idx.postings["python"]["d2"] == 1

    def test_build_empty_corpus(self):
        idx = BM25InvertedIndex()
        idx.build({})
        assert idx.doc_count == 0
        assert idx.avg_doc_len == 0.0


class TestBM25Score:
    """Tests for BM25 scoring/ranking."""

    def test_relevant_doc_ranks_first(self):
        idx = BM25InvertedIndex()
        idx.build(_DOCS)
        results = idx.score(["python", "decorators"], top_k=3)
        assert results[0][0] == "d1"

    def test_top_k_limits_results(self):
        idx = BM25InvertedIndex()
        idx.build(_DOCS)
        results = idx.score(["python"], top_k=1)
        assert len(results) == 1

    def test_unknown_term_returns_empty(self):
        idx = BM25InvertedIndex()
        idx.build(_DOCS)
        results = idx.score(["nonexistent"])
        assert results == []

    def test_empty_query_returns_empty(self):
        idx = BM25InvertedIndex()
        idx.build(_DOCS)
        assert idx.score([]) == []

    def test_scores_are_positive(self):
        idx = BM25InvertedIndex()
        idx.build(_DOCS)
        results = idx.score(["tutorial"])
        assert all(score > 0 for _, score in results)


class TestBM25Persistence:
    """Tests for save/load round-trip."""

    def test_save_and_load_roundtrip(self, tmp_path: Path):
        idx = BM25InvertedIndex(k1=1.5, b=0.8)
        idx.build(_DOCS)

        file_path = tmp_path / "index.msgpack"
        idx.save(file_path)

        loaded = BM25InvertedIndex.load(file_path)
        assert loaded.k1 == 1.5
        assert loaded.b == 0.8
        assert loaded.doc_count == 3
        assert abs(loaded.avg_doc_len - idx.avg_doc_len) < 1e-9
        assert loaded.postings["python"]["d1"] == 2

    def test_loaded_index_produces_same_scores(self, tmp_path: Path):
        idx = BM25InvertedIndex()
        idx.build(_DOCS)
        original_results = idx.score(["python", "tutorial"], top_k=3)

        file_path = tmp_path / "index.msgpack"
        idx.save(file_path)
        loaded = BM25InvertedIndex.load(file_path)
        loaded_results = loaded.score(["python", "tutorial"], top_k=3)

        assert len(original_results) == len(loaded_results)
        for (oid, oscore), (lid, lscore) in zip(original_results, loaded_results):
            assert oid == lid
            assert abs(oscore - lscore) < 1e-9
