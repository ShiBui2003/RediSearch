"""
FAISS-backed vector index for semantic search.

Documents are encoded with a sentence-transformer model.  The resulting
embeddings are stored in a FAISS flat (or IVF) index for fast nearest-
neighbour retrieval.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import faiss
import numpy as np

logger = logging.getLogger(__name__)


class VectorIndex:
    """Dense vector index backed by FAISS."""

    def __init__(self, embedding_dim: int = 384) -> None:
        self._dim = embedding_dim
        self._doc_ids: list[str] = []
        self._index: Optional[faiss.Index] = None
        self.doc_count: int = 0

    def build(self, doc_ids: list[str], embeddings: np.ndarray) -> None:
        """
        Build a FAISS index from pre-computed embeddings.

        Args:
            doc_ids: Ordered list matching rows of *embeddings*.
            embeddings: 2-D float32 array of shape ``(n_docs, dim)``.
        """
        if len(doc_ids) == 0 or embeddings.shape[0] == 0:
            self.doc_count = 0
            return

        self._doc_ids = list(doc_ids)
        self.doc_count = len(self._doc_ids)
        self._dim = embeddings.shape[1]

        # Normalize for cosine similarity (inner-product on unit vectors)
        norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
        norms[norms == 0] = 1.0
        normed = (embeddings / norms).astype(np.float32)

        self._index = faiss.IndexFlatIP(self._dim)
        self._index.add(normed)
        logger.info("Built FAISS index: %d vectors (dim=%d)", self.doc_count, self._dim)

    def search(self, query_embedding: np.ndarray, top_k: int = 20) -> list[tuple[str, float]]:
        """
        Find the *top_k* closest documents to *query_embedding*.

        Returns ``[(doc_id, similarity_score), ...]`` in descending order.
        """
        if self._index is None or self.doc_count == 0:
            return []

        qvec = query_embedding.reshape(1, -1).astype(np.float32)
        norm = np.linalg.norm(qvec)
        if norm > 0:
            qvec = qvec / norm

        k = min(top_k, self.doc_count)
        distances, indices = self._index.search(qvec, k)

        results: list[tuple[str, float]] = []
        for dist, idx in zip(distances[0], indices[0]):
            if idx >= 0:
                results.append((self._doc_ids[idx], float(dist)))
        return results

    # ---- persistence ----

    def save(self, path: Path) -> None:
        """Save the FAISS index and doc-ID mapping to disk."""
        path.mkdir(parents=True, exist_ok=True)

        faiss.write_index(self._index, str(path / "faiss.index"))

        import msgpack
        with open(path / "doc_ids.msgpack", "wb") as f:
            msgpack.pack({"doc_ids": self._doc_ids, "dim": self._dim}, f)

        logger.info("Saved FAISS index to %s", path)

    @classmethod
    def load(cls, path: Path) -> "VectorIndex":
        """Load a FAISS index + doc-IDs from disk."""
        import msgpack

        with open(path / "doc_ids.msgpack", "rb") as f:
            meta = msgpack.unpack(f, raw=False)

        idx = cls(embedding_dim=meta["dim"])
        idx._doc_ids = meta["doc_ids"]
        idx.doc_count = len(idx._doc_ids)
        idx._index = faiss.read_index(str(path / "faiss.index"))
        logger.info("Loaded FAISS index: %d vectors from %s", idx.doc_count, path)
        return idx
