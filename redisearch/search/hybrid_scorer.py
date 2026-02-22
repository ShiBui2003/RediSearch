"""
Hybrid score fusion for combining BM25, TF-IDF, and vector results.

Supports two strategies:
* **Linear combination** — min-max normalise each source's scores then
  weighted-sum with configurable alpha/weights.
* **Reciprocal Rank Fusion (RRF)** — rank-based merge that is robust
  to score-scale differences.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class ScoredHit:
    """A search hit with a fused score and source breakdown."""

    id: str
    score: float
    shard_id: str = ""
    bm25_score: float = 0.0
    tfidf_score: float = 0.0
    vector_score: float = 0.0


def _min_max_normalise(scores: dict[str, float]) -> dict[str, float]:
    """Normalise scores to [0, 1] using min-max scaling."""
    if not scores:
        return {}
    lo = min(scores.values())
    hi = max(scores.values())
    span = hi - lo
    if span == 0:
        return {k: 1.0 for k in scores}
    return {k: (v - lo) / span for k, v in scores.items()}


def linear_combination(
    bm25_hits: list[tuple[str, float, str]],
    tfidf_hits: list[tuple[str, float, str]] | None = None,
    vector_hits: list[tuple[str, float, str]] | None = None,
    bm25_weight: float = 0.7,
    tfidf_weight: float = 0.15,
    vector_weight: float = 0.15,
    top_k: int = 20,
) -> list[ScoredHit]:
    """
    Fuse results via weighted linear combination of normalised scores.

    Each input is a list of ``(doc_id, raw_score, shard_id)`` tuples.
    """
    bm25_norm = _min_max_normalise({d: s for d, s, _ in bm25_hits})
    tfidf_norm = _min_max_normalise({d: s for d, s, _ in (tfidf_hits or [])})
    vector_norm = _min_max_normalise({d: s for d, s, _ in (vector_hits or [])})

    shard_map: dict[str, str] = {}
    for d, _, sid in bm25_hits:
        shard_map.setdefault(d, sid)
    for d, _, sid in (tfidf_hits or []):
        shard_map.setdefault(d, sid)
    for d, _, sid in (vector_hits or []):
        shard_map.setdefault(d, sid)

    all_ids = set(bm25_norm) | set(tfidf_norm) | set(vector_norm)
    merged: list[ScoredHit] = []
    for doc_id in all_ids:
        b = bm25_norm.get(doc_id, 0.0)
        t = tfidf_norm.get(doc_id, 0.0)
        v = vector_norm.get(doc_id, 0.0)
        fused = b * bm25_weight + t * tfidf_weight + v * vector_weight
        merged.append(ScoredHit(
            id=doc_id,
            score=round(fused, 6),
            shard_id=shard_map.get(doc_id, ""),
            bm25_score=round(b, 6),
            tfidf_score=round(t, 6),
            vector_score=round(v, 6),
        ))

    merged.sort(key=lambda h: h.score, reverse=True)
    return merged[:top_k]


def reciprocal_rank_fusion(
    *ranked_lists: list[tuple[str, float, str]],
    k: int = 60,
    top_k: int = 20,
) -> list[ScoredHit]:
    """
    Reciprocal Rank Fusion (RRF) across multiple ranked result lists.

    ``score(d) = Σ 1 / (k + rank_i(d))``
    """
    shard_map: dict[str, str] = {}
    scores: dict[str, float] = {}

    for ranked in ranked_lists:
        for rank, (doc_id, raw_score, shard_id) in enumerate(ranked, start=1):
            scores[doc_id] = scores.get(doc_id, 0.0) + 1.0 / (k + rank)
            shard_map.setdefault(doc_id, shard_id)

    merged = [
        ScoredHit(id=doc_id, score=round(s, 6), shard_id=shard_map.get(doc_id, ""))
        for doc_id, s in scores.items()
    ]
    merged.sort(key=lambda h: h.score, reverse=True)
    return merged[:top_k]
