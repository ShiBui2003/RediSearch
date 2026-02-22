"""Tests for ShardRouter."""

from __future__ import annotations

from pathlib import Path

import pytest

from redisearch.indexing.shard_manager import ShardManager, ShardPlan
from redisearch.search.shard_router import ShardRouter
from redisearch.storage.index_version_store import IndexVersionStore
from redisearch.storage.models import IndexVersion
from redisearch.storage.schema import initialize_database


@pytest.fixture
def version_store(db, db_path: Path) -> IndexVersionStore:
    return IndexVersionStore(db_path)


@pytest.fixture
def shard_mgr(db, db_path: Path) -> ShardManager:
    return ShardManager(db_path=db_path)


@pytest.fixture
def router(shard_mgr: ShardManager, version_store: IndexVersionStore, db_path: Path) -> ShardRouter:
    return ShardRouter(shard_manager=shard_mgr, version_store=version_store, db_path=db_path)


def _activate_shard(version_store: IndexVersionStore, shard_id: str) -> None:
    """Insert and activate a dummy index version for a shard."""
    version_store.insert(
        IndexVersion(
            index_type="bm25",
            shard_id=shard_id,
            version=1,
            status="building",
            doc_count=100,
            file_path=f"data/indexes/bm25/{shard_id}/v1/index.msgpack",
        )
    )
    version_store.activate("bm25", shard_id, 1)


class TestShardRouter:
    def test_resolve_all_shards(self, router: ShardRouter, version_store: IndexVersionStore):
        _activate_shard(version_store, "shard_python")
        _activate_shard(version_store, "shard_small")
        shards = router.resolve(subreddit=None, index_type="bm25")
        assert sorted(shards) == ["shard_python", "shard_small"]

    def test_resolve_specific_subreddit(
        self, router: ShardRouter, version_store: IndexVersionStore, shard_mgr: ShardManager
    ):
        shard_mgr.save_plan(ShardPlan(assignments={"python": "shard_python"}))
        _activate_shard(version_store, "shard_python")

        shards = router.resolve(subreddit="python", index_type="bm25")
        assert shards == ["shard_python"]

    def test_resolve_grouped_subreddit(
        self, router: ShardRouter, version_store: IndexVersionStore, shard_mgr: ShardManager
    ):
        shard_mgr.save_plan(ShardPlan(assignments={"rust": "shard_small"}))
        _activate_shard(version_store, "shard_small")

        shards = router.resolve(subreddit="rust", index_type="bm25")
        assert shards == ["shard_small"]

    def test_resolve_no_active_index(
        self, router: ShardRouter, shard_mgr: ShardManager
    ):
        shard_mgr.save_plan(ShardPlan(assignments={"go": "shard_go"}))
        shards = router.resolve(subreddit="go", index_type="bm25")
        assert shards == []

    def test_resolve_empty(self, router: ShardRouter):
        shards = router.resolve(subreddit=None, index_type="bm25")
        assert shards == []

    def test_legacy_fallback(
        self, router: ShardRouter, version_store: IndexVersionStore
    ):
        """If no plan entry but a legacy shard_<sub> exists, fall back to it."""
        _activate_shard(version_store, "shard_python")
        shards = router.resolve(subreddit="python", index_type="bm25")
        assert shards == ["shard_python"]
