"""BM25 search over active index versions."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from redisearch.config.settings import BM25Settings, Settings, get_settings
from redisearch.indexing.bm25_index import BM25InvertedIndex
from redisearch.preprocessing.pipeline import PreprocessingProfile, TextPreprocessor
from redisearch.storage.index_version_store import IndexVersionStore

logger = logging.getLogger(__name__)


@dataclass
class BM25SearchHit:
    """Search hit containing document ID and BM25 score."""

    id: str
    score: float
    shard_id: str


class BM25Searcher:
    """Loads active BM25 indexes and executes query ranking."""

    def __init__(
        self,
        version_store: Optional[IndexVersionStore] = None,
        bm25_settings: Optional[BM25Settings] = None,
        project_root: Optional[Path] = None,
        preprocessor: Optional[TextPreprocessor] = None,
    ) -> None:
        settings: Settings = get_settings()
        self._version_store = version_store or IndexVersionStore()
        self._bm25_settings = bm25_settings or settings.bm25
        self._project_root = project_root or settings.project_root
        self._preprocessor = preprocessor or TextPreprocessor(settings.preprocessing)
        self._cache: dict[str, BM25InvertedIndex] = {}

    def search(
        self,
        query: str,
        subreddit: Optional[str] = None,
        top_k: int = 20,
    ) -> list[BM25SearchHit]:
        """Search one subreddit or all active BM25 shards and return top hits."""
        query_tokens = self._preprocessor.preprocess(query, profile=PreprocessingProfile.QUERY)
        if not query_tokens:
            return []

        shards = [f"shard_{subreddit.strip().lower()}"] if subreddit else [
            v.shard_id for v in self._version_store.get_all_active() if v.index_type == "bm25"
        ]

        all_hits: list[BM25SearchHit] = []
        for shard_id in shards:
            index = self._load_active_index(shard_id)
            if index is None:
                continue
            for doc_id, score in index.score(query_tokens, top_k=top_k):
                all_hits.append(BM25SearchHit(id=doc_id, score=score, shard_id=shard_id))

        all_hits.sort(key=lambda h: h.score, reverse=True)
        return all_hits[: max(0, top_k)]

    def _load_active_index(self, shard_id: str) -> Optional[BM25InvertedIndex]:
        """Load active index for shard, using in-memory cache by file path."""
        active = self._version_store.get_active("bm25", shard_id)
        if not active:
            return None

        file_path = Path(active.file_path)
        absolute_path = file_path if file_path.is_absolute() else (self._project_root / file_path)
        cache_key = str(absolute_path.resolve())

        if cache_key not in self._cache:
            if not absolute_path.exists():
                logger.warning("Active BM25 index file missing for %s: %s", shard_id, absolute_path)
                return None
            self._cache[cache_key] = BM25InvertedIndex.load(
                absolute_path,
                k1=self._bm25_settings.k1,
                b=self._bm25_settings.b,
            )

        return self._cache[cache_key]
