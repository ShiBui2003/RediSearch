"""
Vector (semantic) search over active FAISS indexes.

Encodes the query with the same sentence-transformer used at build time,
then performs k-NN retrieval on each active shard.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import numpy as np

from redisearch.config.settings import Settings, VectorSettings, get_settings
from redisearch.indexing.vector_index import VectorIndex
from redisearch.search.shard_router import ShardRouter
from redisearch.storage.index_version_store import IndexVersionStore

logger = logging.getLogger(__name__)


@dataclass
class VectorSearchHit:
    """Search hit with cosine similarity score from vector search."""

    id: str
    score: float
    shard_id: str


class VectorSearcher:
    """Loads active FAISS indexes and runs nearest-neighbour queries."""

    def __init__(
        self,
        version_store: Optional[IndexVersionStore] = None,
        project_root: Optional[Path] = None,
        vector_settings: Optional[VectorSettings] = None,
        shard_router: Optional[ShardRouter] = None,
    ) -> None:
        settings: Settings = get_settings()
        self._version_store = version_store or IndexVersionStore()
        self._project_root = project_root or settings.project_root
        self._vs = vector_settings or settings.vector
        self._shard_router = shard_router or ShardRouter(version_store=self._version_store)
        self._cache: dict[str, VectorIndex] = {}
        self._encoder = None

    def _ensure_encoder(self):
        if self._encoder is None:
            from sentence_transformers import SentenceTransformer
            self._encoder = SentenceTransformer(self._vs.model_name)
        return self._encoder

    def search(
        self,
        query: str,
        subreddit: Optional[str] = None,
        top_k: int = 20,
    ) -> list[VectorSearchHit]:
        """Encode query and search FAISS indexes."""
        if not query.strip():
            return []

        shards = self._shard_router.resolve(subreddit=subreddit, index_type="vector")
        if not shards:
            return []

        encoder = self._ensure_encoder()
        query_vec = encoder.encode([query], convert_to_numpy=True).astype(np.float32)[0]

        all_hits: list[VectorSearchHit] = []
        for shard_id in shards:
            index = self._load(shard_id)
            if index is None:
                continue
            for doc_id, score in index.search(query_vec, top_k=top_k):
                all_hits.append(VectorSearchHit(id=doc_id, score=score, shard_id=shard_id))

        all_hits.sort(key=lambda h: h.score, reverse=True)
        return all_hits[:top_k]

    def _load(self, shard_id: str) -> Optional[VectorIndex]:
        active = self._version_store.get_active("vector", shard_id)
        if not active:
            return None

        file_path = Path(active.file_path)
        abs_path = file_path if file_path.is_absolute() else (self._project_root / file_path)
        key = str(abs_path.resolve())

        if key not in self._cache:
            if not abs_path.exists():
                logger.warning("FAISS index missing for %s: %s", shard_id, abs_path)
                return None
            self._cache[key] = VectorIndex.load(abs_path)

        return self._cache[key]
