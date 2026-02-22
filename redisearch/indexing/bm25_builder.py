"""BM25 index builder from processed posts."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

from redisearch.config.settings import BM25Settings, Settings, get_settings
from redisearch.indexing.bm25_index import BM25InvertedIndex
from redisearch.storage.index_version_store import IndexVersionStore
from redisearch.storage.models import IndexVersion
from redisearch.storage.processed_store import ProcessedPostStore
from redisearch.storage.raw_store import RawPostStore

logger = logging.getLogger(__name__)


class BM25IndexBuilder:
    """Builds and activates BM25 indexes for subreddits."""

    def __init__(
        self,
        processed_store: Optional[ProcessedPostStore] = None,
        raw_store: Optional[RawPostStore] = None,
        version_store: Optional[IndexVersionStore] = None,
        bm25_settings: Optional[BM25Settings] = None,
        indexes_root: Optional[Path] = None,
        project_root: Optional[Path] = None,
    ) -> None:
        settings: Settings = get_settings()
        self._processed_store = processed_store or ProcessedPostStore()
        self._raw_store = raw_store or RawPostStore()
        self._version_store = version_store or IndexVersionStore()
        self._bm25_settings = bm25_settings or settings.bm25
        self._indexes_root = indexes_root or settings.indexes_dir
        self._project_root = project_root or settings.project_root

    def build_subreddit(self, subreddit: str) -> dict:
        """Build BM25 index for a single subreddit and activate it."""
        subreddit_name = subreddit.strip().lower()
        shard_id = f"shard_{subreddit_name}"

        processed_posts = self._processed_store.get_all_for_subreddit(subreddit_name)
        documents: dict[str, list[str]] = {}
        for post in processed_posts:
            tokens = json.loads(post.all_tokens or "[]")
            documents[post.id] = [str(t) for t in tokens]

        if not documents:
            return {
                "subreddit": subreddit_name,
                "shard_id": shard_id,
                "version": 0,
                "doc_count": 0,
                "file_path": None,
            }

        index = BM25InvertedIndex(k1=self._bm25_settings.k1, b=self._bm25_settings.b)
        index.build(documents)

        version = self._version_store.get_latest_version_number("bm25", shard_id) + 1
        relative_file_path = Path("data") / "indexes" / "bm25" / shard_id / f"v{version}" / "index.msgpack"
        absolute_file_path = self._project_root / relative_file_path
        index.save(absolute_file_path)

        self._version_store.insert(
            IndexVersion(
                index_type="bm25",
                shard_id=shard_id,
                version=version,
                status="building",
                doc_count=index.doc_count,
                file_path=str(relative_file_path).replace("\\", "/"),
            )
        )
        self._version_store.activate("bm25", shard_id, version)

        logger.info(
            "Built BM25 index for r/%s: %d docs (v%d)",
            subreddit_name,
            index.doc_count,
            version,
        )

        return {
            "subreddit": subreddit_name,
            "shard_id": shard_id,
            "version": version,
            "doc_count": index.doc_count,
            "file_path": str(relative_file_path).replace("\\", "/"),
        }

    def build_all(self) -> list[dict]:
        """Build BM25 indexes for all known subreddits with processed posts."""
        summaries: list[dict] = []
        for subreddit in self._raw_store.get_subreddits():
            summaries.append(self.build_subreddit(subreddit))
        return summaries
