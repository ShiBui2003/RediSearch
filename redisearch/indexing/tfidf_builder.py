"""Build TF-IDF indexes from processed posts — mirrors BM25IndexBuilder."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

from redisearch.config.settings import Settings, get_settings
from redisearch.indexing.shard_manager import ShardManager, ShardPlan
from redisearch.indexing.tfidf_index import TFIDFIndex
from redisearch.storage.index_version_store import IndexVersionStore
from redisearch.storage.models import IndexVersion
from redisearch.storage.processed_store import ProcessedPostStore
from redisearch.storage.raw_store import RawPostStore

logger = logging.getLogger(__name__)


class TFIDFIndexBuilder:
    """Builds and activates TF-IDF indexes for subreddits."""

    def __init__(
        self,
        processed_store: Optional[ProcessedPostStore] = None,
        raw_store: Optional[RawPostStore] = None,
        version_store: Optional[IndexVersionStore] = None,
        shard_manager: Optional[ShardManager] = None,
        indexes_root: Optional[Path] = None,
        project_root: Optional[Path] = None,
    ) -> None:
        settings: Settings = get_settings()
        self._processed_store = processed_store or ProcessedPostStore()
        self._raw_store = raw_store or RawPostStore()
        self._version_store = version_store or IndexVersionStore()
        self._shard_manager = shard_manager or ShardManager()
        self._indexes_root = indexes_root or settings.indexes_dir
        self._project_root = project_root or settings.project_root

    def build_subreddit(self, subreddit: str) -> dict:
        """Build TF-IDF index for a single subreddit and activate it."""
        subreddit_name = subreddit.strip().lower()
        shard_id = self._shard_manager.get_shard_id(subreddit_name)

        documents = self._collect_documents([subreddit_name])
        if not documents:
            return {"shard_id": shard_id, "version": 0, "doc_count": 0, "file_path": None}

        return self._build_and_activate(shard_id, documents)

    def build_shard(self, shard_id: str, subreddits: list[str]) -> dict:
        """Build a TF-IDF index for multiple subreddits under one shard."""
        documents = self._collect_documents(subreddits)
        if not documents:
            return {"shard_id": shard_id, "version": 0, "doc_count": 0, "file_path": None}
        return self._build_and_activate(shard_id, documents)

    def build_all(self) -> list[dict]:
        """Build TF-IDF indexes for all shards according to the current plan."""
        plan: ShardPlan = self._shard_manager.load_plan()
        if not plan.assignments:
            # No plan saved yet — one shard per subreddit
            subreddits = self._raw_store.get_subreddits()
            plan = ShardPlan(assignments={s: f"shard_{s}" for s in subreddits})

        summaries: list[dict] = []
        for shard_id in plan.shard_ids():
            subs = plan.subreddits_in(shard_id)
            summary = self.build_shard(shard_id, subs)
            summary["subreddits"] = subs
            summaries.append(summary)
        return summaries

    # ---- helpers ----

    def _collect_documents(self, subreddits: list[str]) -> dict[str, list[str]]:
        documents: dict[str, list[str]] = {}
        for sub in subreddits:
            for post in self._processed_store.get_all_for_subreddit(sub.strip().lower()):
                tokens = json.loads(post.all_tokens or "[]")
                documents[post.id] = [str(t) for t in tokens]
        return documents

    def _build_and_activate(self, shard_id: str, documents: dict[str, list[str]]) -> dict:
        index = TFIDFIndex()
        index.build(documents)

        version = self._version_store.get_latest_version_number("tfidf", shard_id) + 1
        rel = Path("data") / "indexes" / "tfidf" / shard_id / f"v{version}" / "index.msgpack"
        abs_path = self._project_root / rel
        index.save(abs_path)

        self._version_store.insert(
            IndexVersion(
                index_type="tfidf",
                shard_id=shard_id,
                version=version,
                status="building",
                doc_count=index.doc_count,
                file_path=str(rel).replace("\\", "/"),
            )
        )
        self._version_store.activate("tfidf", shard_id, version)

        logger.info("Built TF-IDF index %s: %d docs (v%d)", shard_id, index.doc_count, version)
        return {
            "shard_id": shard_id,
            "version": version,
            "doc_count": index.doc_count,
            "file_path": str(rel).replace("\\", "/"),
        }
