"""
TF-IDF inverted index with cosine similarity scoring.

Uses scikit-learn's TfidfVectorizer for efficient TF-IDF computation
and sparse matrix storage.  The built model (vocabulary + IDF weights +
document vectors) is persisted via msgpack so it can be loaded without
re-fitting.
"""

from __future__ import annotations

import logging
import math
from pathlib import Path
from typing import Optional

import msgpack
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

logger = logging.getLogger(__name__)


class TFIDFIndex:
    """Sparse TF-IDF index with cosine similarity ranking."""

    def __init__(self) -> None:
        self._vectorizer: Optional[TfidfVectorizer] = None
        self._doc_ids: list[str] = []
        self._tfidf_matrix: Optional[np.ndarray] = None  # dense for persistence simplicity
        self.doc_count: int = 0

    def build(self, documents: dict[str, list[str]]) -> None:
        """
        Build the TF-IDF matrix from ``{doc_id: [token, ...]}``.

        Tokens are joined back into a string for TfidfVectorizer (which
        re-tokenizes on whitespace, matching our pre-stemmed tokens).
        """
        if not documents:
            self.doc_count = 0
            return

        self._doc_ids = list(documents.keys())
        corpus = [" ".join(tokens) for tokens in documents.values()]

        self._vectorizer = TfidfVectorizer(
            analyzer="word",
            token_pattern=r"\S+",  # each whitespace-delimited token
            lowercase=False,       # tokens are already lowered
        )
        sparse = self._vectorizer.fit_transform(corpus)
        self._tfidf_matrix = sparse.toarray().astype(np.float32)
        self.doc_count = len(self._doc_ids)
        logger.info("Built TF-IDF index: %d docs, %d terms", self.doc_count, len(self._vectorizer.vocabulary_))

    def score(self, query_tokens: list[str], top_k: int = 20) -> list[tuple[str, float]]:
        """Score documents against *query_tokens* using cosine similarity."""
        if not query_tokens or self._vectorizer is None or self.doc_count == 0:
            return []

        query_str = " ".join(query_tokens)
        query_vec = self._vectorizer.transform([query_str]).toarray().astype(np.float32)

        sims = cosine_similarity(query_vec, self._tfidf_matrix).flatten()
        top_indices = np.argsort(sims)[::-1][:top_k]

        results: list[tuple[str, float]] = []
        for idx in top_indices:
            sim = float(sims[idx])
            if sim > 0:
                results.append((self._doc_ids[idx], sim))
        return results

    # ---- persistence ----

    def save(self, path: Path) -> None:
        """Serialize the index to a msgpack file."""
        path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "doc_ids": self._doc_ids,
            "vocabulary": self._vectorizer.vocabulary_ if self._vectorizer else {},
            "idf": self._vectorizer.idf_.tolist() if self._vectorizer else [],
            "matrix": self._tfidf_matrix.tolist() if self._tfidf_matrix is not None else [],
        }
        with open(path, "wb") as f:
            msgpack.pack(data, f)
        logger.info("Saved TF-IDF index to %s", path)

    @classmethod
    def load(cls, path: Path) -> "TFIDFIndex":
        """Deserialize a TF-IDF index from a msgpack file."""
        with open(path, "rb") as f:
            data = msgpack.unpack(f, raw=False)

        idx = cls()
        idx._doc_ids = data["doc_ids"]
        idx.doc_count = len(idx._doc_ids)

        if idx.doc_count == 0:
            return idx

        vocab = data["vocabulary"]
        idf = np.array(data["idf"], dtype=np.float64)
        matrix = np.array(data["matrix"], dtype=np.float32)

        vectorizer = TfidfVectorizer(
            analyzer="word",
            token_pattern=r"\S+",
            lowercase=False,
        )
        # Manually set fitted state
        vectorizer.vocabulary_ = vocab
        vectorizer.idf_ = idf
        # sklearn needs _tfidf.idf_ for transform()
        from sklearn.feature_extraction.text import TfidfTransformer
        vectorizer._tfidf = TfidfTransformer()
        vectorizer._tfidf.idf_ = idf

        idx._vectorizer = vectorizer
        idx._tfidf_matrix = matrix
        logger.info("Loaded TF-IDF index: %d docs from %s", idx.doc_count, path)
        return idx
