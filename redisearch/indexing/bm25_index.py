"""Inverted index and BM25 scoring implementation."""

from __future__ import annotations

import math
from pathlib import Path
from typing import Optional

import msgpack


class BM25InvertedIndex:
    """In-memory BM25 index with msgpack persistence."""

    def __init__(self, k1: float = 1.2, b: float = 0.75) -> None:
        self.k1 = k1
        self.b = b
        self.postings: dict[str, dict[str, int]] = {}
        self.doc_lengths: dict[str, int] = {}
        self.doc_count: int = 0
        self.avg_doc_len: float = 0.0

    def build(self, documents: dict[str, list[str]]) -> None:
        """Build postings and stats from document token lists."""
        self.postings = {}
        self.doc_lengths = {}

        total_len = 0
        for doc_id, tokens in documents.items():
            token_list = list(tokens or [])
            self.doc_lengths[doc_id] = len(token_list)
            total_len += len(token_list)

            term_freq: dict[str, int] = {}
            for token in token_list:
                term_freq[token] = term_freq.get(token, 0) + 1

            for term, tf in term_freq.items():
                self.postings.setdefault(term, {})[doc_id] = tf

        self.doc_count = len(self.doc_lengths)
        self.avg_doc_len = (total_len / self.doc_count) if self.doc_count else 0.0

    def score(self, query_tokens: list[str], top_k: int = 20) -> list[tuple[str, float]]:
        """Score documents for query tokens and return top-k doc IDs with scores."""
        if not query_tokens or self.doc_count == 0:
            return []

        scores: dict[str, float] = {}

        for term in query_tokens:
            posting = self.postings.get(term)
            if not posting:
                continue

            df = len(posting)
            idf = math.log(1.0 + ((self.doc_count - df + 0.5) / (df + 0.5)))

            for doc_id, tf in posting.items():
                dl = self.doc_lengths.get(doc_id, 0)
                norm = (1.0 - self.b) + self.b * (dl / self.avg_doc_len) if self.avg_doc_len > 0 else 1.0
                tf_weight = (tf * (self.k1 + 1.0)) / (tf + self.k1 * norm)
                scores[doc_id] = scores.get(doc_id, 0.0) + idf * tf_weight

        ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        return ranked[: max(0, top_k)]

    def save(self, file_path: Path) -> None:
        """Persist index to msgpack file."""
        file_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "k1": self.k1,
            "b": self.b,
            "postings": {
                term: list(doc_tfs.items()) for term, doc_tfs in self.postings.items()
            },
            "doc_lengths": self.doc_lengths,
            "doc_count": self.doc_count,
            "avg_doc_len": self.avg_doc_len,
        }
        file_path.write_bytes(msgpack.packb(payload, use_bin_type=True))

    @classmethod
    def load(
        cls,
        file_path: Path,
        k1: Optional[float] = None,
        b: Optional[float] = None,
    ) -> "BM25InvertedIndex":
        """Load index from msgpack file."""
        payload = msgpack.unpackb(file_path.read_bytes(), raw=False)

        index = cls(
            k1=payload.get("k1", 1.2) if k1 is None else k1,
            b=payload.get("b", 0.75) if b is None else b,
        )
        index.postings = {
            term: {doc_id: int(tf) for doc_id, tf in doc_tfs}
            for term, doc_tfs in payload.get("postings", {}).items()
        }
        index.doc_lengths = {doc_id: int(v) for doc_id, v in payload.get("doc_lengths", {}).items()}
        index.doc_count = int(payload.get("doc_count", len(index.doc_lengths)))
        index.avg_doc_len = float(payload.get("avg_doc_len", 0.0))
        return index
