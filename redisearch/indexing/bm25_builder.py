"""BM25 index builder from processed posts."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

from redisearch.config.settings import BM25Settings, Settings, get_settings
from redisearch.indexing.bm25_index import BM25InvertedIndex
from redisearch.indexing.shard_manager import ShardManager, ShardPlan
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
        shard_manager: Optional[ShardManager] = None,
        bm25_settings: Optional[BM25Settings] = None,
        indexes_root: Optional[Path] = None,
        project_root: Optional[Path] = None,
    ) -> None:
        settings: Settings = get_settings()
        self._processed_store = processed_store or ProcessedPostStore()
        self._raw_store = raw_store or RawPostStore()
        self._version_store = version_store or IndexVersionStore()
        self._shard_manager = shard_manager or ShardManager()
        self._bm25_settings = bm25_settings or settings.bm25
        self._indexes_root = indexes_root or settings.indexes_dir
        self._project_root = project_root or settings.project_root

    def build_subreddit(self, subreddit: str) -> dict:
        """Build BM25 index for a single subreddit and activate it."""
        subreddit_name = subreddit.strip().lower()
        shard_id = self._shard_manager.get_shard_id(subreddit_name)

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

        return self._build_and_activate(shard_id, documents, subreddit_name)

    def build_shard(self, shard_id: str, subreddits: list[str]) -> dict:
        """Build a BM25 index for an explicit list of subreddits under one shard."""
        documents: dict[str, list[str]] = {}
        for sub in subreddits:
            for post in self._processed_store.get_all_for_subreddit(sub.strip().lower()):
                tokens = json.loads(post.all_tokens or "[]")
                documents[post.id] = [str(t) for t in tokens]

        if not documents:
            return {
                "shard_id": shard_id,
                "version": 0,
                "doc_count": 0,
                "file_path": None,
            }

        return self._build_and_activate(shard_id, documents)

    def _build_and_activate(
        self,
        shard_id: str,
        documents: dict[str, list[str]],
        label: str | None = None,
    ) -> dict:
        """Build an index from *documents*, persist it, and activate the version."""
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

        display = label or shard_id
        logger.info(
            "Built BM25 index for %s: %d docs (v%d)",
            display,
            index.doc_count,
            version,
        )

        return {
            "shard_id": shard_id,
            "version": version,
            "doc_count": index.doc_count,
            "file_path": str(relative_file_path).replace("\\", "/"),
        }

    def build_all(self) -> list[dict]:
        """
        Build BM25 indexes using the shard plan.

        1. Compute shard plan from subreddit doc counts.
        2. Persist the plan.
        3. Build each shard (dedicated or grouped).
        """
        subreddits = self._raw_store.get_subreddits()
        doc_counts = {sub: self._raw_store.count(sub) for sub in subreddits}

        plan: ShardPlan = self._shard_manager.compute_plan(doc_counts)
        self._shard_manager.save_plan(plan)

        summaries: list[dict] = []

        # Build each shard
        for shard_id in plan.shard_ids():
            subs = plan.subreddits_in(shard_id)
            summary = self.build_shard(shard_id, subs)
            summary["subreddits"] = subs
            summaries.append(summary)

        return summaries
