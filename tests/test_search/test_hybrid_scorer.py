"""Tests for hybrid score fusion functions."""

from __future__ import annotations

from redisearch.search.hybrid_scorer import (
    ScoredHit,
    linear_combination,
    reciprocal_rank_fusion,
)


class TestLinearCombination:
    def test_bm25_only(self):
        bm25 = [("d1", 5.0, "s1"), ("d2", 3.0, "s1")]
        results = linear_combination(bm25, top_k=5)
        assert results[0].id == "d1"
        assert results[0].score > results[1].score

    def test_fusion_reranks(self):
        bm25 = [("d1", 5.0, "s1"), ("d2", 3.0, "s1")]
        tfidf = [("d2", 0.9, "s1"), ("d1", 0.1, "s1")]
        results = linear_combination(
            bm25, tfidf_hits=tfidf, bm25_weight=0.5, tfidf_weight=0.5, top_k=5
        )
        # d2 has a strong TF-IDF boost; verify both appear
        ids = [r.id for r in results]
        assert "d1" in ids and "d2" in ids

    def test_empty_bm25(self):
        results = linear_combination([], top_k=5)
        assert results == []

    def test_top_k_limits(self):
        bm25 = [(f"d{i}", float(i), "s1") for i in range(10)]
        results = linear_combination(bm25, top_k=3)
        assert len(results) == 3

    def test_with_all_three_sources(self):
        bm25 = [("d1", 2.0, "s1")]
        tfidf = [("d1", 0.8, "s1")]
        vector = [("d1", 0.9, "s1")]
        results = linear_combination(
            bm25, tfidf_hits=tfidf, vector_hits=vector, top_k=5
        )
        assert len(results) == 1
        assert results[0].score > 0


class TestReciprocalRankFusion:
    def test_single_list(self):
        ranked = [("d1", 5.0, "s1"), ("d2", 3.0, "s1")]
        results = reciprocal_rank_fusion(ranked, top_k=5)
        assert results[0].id == "d1"

    def test_multiple_lists(self):
        list1 = [("d1", 5.0, "s1"), ("d2", 3.0, "s1")]
        list2 = [("d2", 0.9, "s1"), ("d1", 0.1, "s1")]
        results = reciprocal_rank_fusion(list1, list2, top_k=5)
        ids = [r.id for r in results]
        assert "d1" in ids and "d2" in ids

    def test_empty_lists(self):
        results = reciprocal_rank_fusion([], top_k=5)
        assert results == []

    def test_top_k_limits(self):
        ranked = [(f"d{i}", float(i), "s1") for i in range(10)]
        results = reciprocal_rank_fusion(ranked, top_k=2)
        assert len(results) == 2
