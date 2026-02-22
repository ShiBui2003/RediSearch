"""
Shard router — resolves which index shards to query.

Given an optional subreddit filter, the router returns the set of
shard_ids the searcher should scan.  It uses the persisted shard
assignments so that subreddits grouped into ``shard_small`` are
correctly resolved.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

from redisearch.config.settings import Settings, get_settings
from redisearch.indexing.shard_manager import ShardManager
from redisearch.storage.index_version_store import IndexVersionStore

logger = logging.getLogger(__name__)


class ShardRouter:
    """Resolves query targets to a list of shard_ids."""

    def __init__(
        self,
        shard_manager: Optional[ShardManager] = None,
        version_store: Optional[IndexVersionStore] = None,
        db_path: Optional[Path] = None,
    ) -> None:
        settings: Settings = get_settings()
        self._shard_manager = shard_manager or ShardManager(db_path=db_path)
        self._version_store = version_store or IndexVersionStore(db_path=db_path)

    def resolve(
        self,
        subreddit: Optional[str] = None,
        index_type: str = "bm25",
    ) -> list[str]:
        """
        Return the shard_ids the searcher should query.

        * If *subreddit* is given, look up its shard assignment and return
          that single shard.
        * If *subreddit* is ``None``, return all active shards for the
          given *index_type*.
        """
        if subreddit:
            shard_id = self._shard_manager.get_shard_id(subreddit)
            # Only return it if there is actually an active index for it
            active = self._version_store.get_active(index_type, shard_id)
            if active:
                return [shard_id]
            # Fallback: maybe it's under the legacy per-subreddit shard name
            legacy = f"shard_{subreddit.strip().lower()}"
            if legacy != shard_id:
                active = self._version_store.get_active(index_type, legacy)
                if active:
                    return [legacy]
            return []

        # No subreddit filter — return every active shard of this type
        all_active = self._version_store.get_all_active()
        return [v.shard_id for v in all_active if v.index_type == index_type]
