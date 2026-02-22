"""Tests for ShardManager and ShardPlan."""

from __future__ import annotations

from pathlib import Path

import pytest

from redisearch.config.settings import ShardSettings
from redisearch.indexing.shard_manager import ShardManager, ShardPlan
from redisearch.storage.schema import initialize_database


@pytest.fixture
def shard_manager(db_path: Path, db) -> ShardManager:
    """Provide a ShardManager connected to the test database."""
    return ShardManager(db_path=db_path, shard_settings=ShardSettings(min_docs_for_own_shard=100))


# ---------------------------------------------------------------------------
# ShardPlan
# ---------------------------------------------------------------------------


class TestShardPlan:
    def test_shard_for_known(self):
        plan = ShardPlan(assignments={"python": "shard_python", "rust": "shard_small"})
        assert plan.shard_for("python") == "shard_python"
        assert plan.shard_for("rust") == "shard_small"

    def test_shard_for_unknown_fallback(self):
        plan = ShardPlan(assignments={})
        assert plan.shard_for("kotlin") == "shard_kotlin"

    def test_subreddits_in(self):
        plan = ShardPlan(assignments={"a": "shard_small", "b": "shard_small", "c": "shard_c"})
        assert sorted(plan.subreddits_in("shard_small")) == ["a", "b"]
        assert plan.subreddits_in("shard_c") == ["c"]

    def test_shard_ids(self):
        plan = ShardPlan(assignments={"a": "shard_small", "b": "shard_b"})
        assert sorted(plan.shard_ids()) == ["shard_b", "shard_small"]

    def test_empty_plan(self):
        plan = ShardPlan()
        assert plan.shard_ids() == []
        assert plan.subreddits_in("any") == []


# ---------------------------------------------------------------------------
# ShardManager
# ---------------------------------------------------------------------------


class TestShardManager:
    def test_compute_plan_big_subreddit_gets_own_shard(self, shard_manager: ShardManager):
        plan = shard_manager.compute_plan({"python": 200, "rust": 50})
        assert plan.shard_for("python") == "shard_python"
        assert plan.shard_for("rust") == "shard_small"

    def test_compute_plan_all_small(self, shard_manager: ShardManager):
        plan = shard_manager.compute_plan({"a": 10, "b": 20})
        assert plan.shard_for("a") == "shard_small"
        assert plan.shard_for("b") == "shard_small"

    def test_save_and_load_plan(self, shard_manager: ShardManager):
        plan = shard_manager.compute_plan({"python": 200, "rust": 50})
        shard_manager.save_plan(plan)

        loaded = shard_manager.load_plan()
        assert loaded.assignments == plan.assignments

    def test_get_shard_id_from_db(self, shard_manager: ShardManager):
        plan = ShardPlan(assignments={"python": "shard_python"})
        shard_manager.save_plan(plan)
        assert shard_manager.get_shard_id("python") == "shard_python"

    def test_get_shard_id_fallback(self, shard_manager: ShardManager):
        assert shard_manager.get_shard_id("unknown") == "shard_unknown"

    def test_get_all_assignments(self, shard_manager: ShardManager):
        plan = ShardPlan(assignments={"a": "shard_a", "b": "shard_small"})
        shard_manager.save_plan(plan)
        mapping = shard_manager.get_all_assignments()
        assert mapping == {"a": "shard_a", "b": "shard_small"}

    def test_save_plan_upserts(self, shard_manager: ShardManager):
        """Saving a new plan replaces existing assignments."""
        shard_manager.save_plan(ShardPlan(assignments={"a": "shard_a"}))
        shard_manager.save_plan(ShardPlan(assignments={"a": "shard_small"}))
        assert shard_manager.get_shard_id("a") == "shard_small"
