"""
TF-IDF search over active TF-IDF index versions.

Mirrors BM25Searcher but loads TF-IDF indexes.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from redisearch.config.settings import Settings, get_settings
from redisearch.indexing.tfidf_index import TFIDFIndex
from redisearch.preprocessing.pipeline import PreprocessingProfile, TextPreprocessor
from redisearch.search.shard_router import ShardRouter
from redisearch.storage.index_version_store import IndexVersionStore

logger = logging.getLogger(__name__)


@dataclass
class TFIDFSearchHit:
    """Search hit with cosine similarity score."""

    id: str
    score: float
    shard_id: str


class TFIDFSearcher:
    """Loads active TF-IDF indexes and executes cosine-similarity ranking."""

    def __init__(
        self,
        version_store: Optional[IndexVersionStore] = None,
        project_root: Optional[Path] = None,
        preprocessor: Optional[TextPreprocessor] = None,
        shard_router: Optional[ShardRouter] = None,
    ) -> None:
        settings: Settings = get_settings()
        self._version_store = version_store or IndexVersionStore()
        self._project_root = project_root or settings.project_root
        self._preprocessor = preprocessor or TextPreprocessor(settings.preprocessing)
        self._shard_router = shard_router or ShardRouter(version_store=self._version_store)
        self._cache: dict[str, TFIDFIndex] = {}

    def search(
        self,
        query: str,
        subreddit: Optional[str] = None,
        top_k: int = 20,
    ) -> list[TFIDFSearchHit]:
        """Search TF-IDF indexes and return top hits."""
        tokens = self._preprocessor.preprocess(query, profile=PreprocessingProfile.QUERY)
        if not tokens:
            return []

        shards = self._shard_router.resolve(subreddit=subreddit, index_type="tfidf")

        all_hits: list[TFIDFSearchHit] = []
        for shard_id in shards:
            index = self._load(shard_id)
            if index is None:
                continue
            for doc_id, score in index.score(tokens, top_k=top_k):
                all_hits.append(TFIDFSearchHit(id=doc_id, score=score, shard_id=shard_id))

        all_hits.sort(key=lambda h: h.score, reverse=True)
        return all_hits[:top_k]

    def _load(self, shard_id: str) -> Optional[TFIDFIndex]:
        active = self._version_store.get_active("tfidf", shard_id)
        if not active:
            return None

        file_path = Path(active.file_path)
        abs_path = file_path if file_path.is_absolute() else (self._project_root / file_path)
        key = str(abs_path.resolve())

        if key not in self._cache:
            if not abs_path.exists():
                logger.warning("TF-IDF index missing for %s: %s", shard_id, abs_path)
                return None
            self._cache[key] = TFIDFIndex.load(abs_path)

        return self._cache[key]
