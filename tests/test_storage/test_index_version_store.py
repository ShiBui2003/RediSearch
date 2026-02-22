"""Tests for the IndexVersionStore."""

from redisearch.storage.models import IndexVersion


class TestIndexVersionCRUD:
    """Tests for index version tracking."""

    def test_insert_and_get_active(self, index_version_store):
        iv = IndexVersion(
            index_type="bm25",
            shard_id="shard_python",
            version=1,
            status="active",
            doc_count=100,
            file_path="data/indexes/shard_python_v1",
        )
        row_id = index_version_store.insert(iv)
        assert row_id > 0

        active = index_version_store.get_active("bm25", "shard_python")
        assert active is not None
        assert active.version == 1
        assert active.doc_count == 100

    def test_get_active_returns_none_when_no_active(self, index_version_store):
        iv = IndexVersion(
            index_type="bm25",
            shard_id="shard_python",
            version=1,
            status="building",
            file_path="data/indexes/shard_python_v1",
        )
        index_version_store.insert(iv)

        active = index_version_store.get_active("bm25", "shard_python")
        assert active is None

    def test_latest_version_number(self, index_version_store):
        for v in range(1, 4):
            iv = IndexVersion(
                index_type="bm25",
                shard_id="shard_python",
                version=v,
                status="stale",
                file_path=f"data/indexes/shard_python_v{v}",
            )
            index_version_store.insert(iv)

        assert index_version_store.get_latest_version_number("bm25", "shard_python") == 3
        assert index_version_store.get_latest_version_number("bm25", "shard_java") == 0

    def test_activate_swaps_correctly(self, index_version_store):
        """Build v2. Activate it. Verify v1 becomes stale and v2 becomes active."""
        iv1 = IndexVersion(
            index_type="bm25", shard_id="shard_python", version=1,
            status="active", file_path="v1",
        )
        iv2 = IndexVersion(
            index_type="bm25", shard_id="shard_python", version=2,
            status="building", file_path="v2",
        )
        index_version_store.insert(iv1)
        index_version_store.insert(iv2)

        # Swap
        index_version_store.activate("bm25", "shard_python", 2)

        active = index_version_store.get_active("bm25", "shard_python")
        assert active.version == 2

        stale = index_version_store.get_stale()
        assert len(stale) == 1
        assert stale[0].version == 1

    def test_get_all_active(self, index_version_store):
        index_version_store.insert(IndexVersion(
            index_type="bm25", shard_id="shard_python", version=1,
            status="active", file_path="v1",
        ))
        index_version_store.insert(IndexVersion(
            index_type="vector", shard_id="shard_python", version=1,
            status="active", file_path="v1",
        ))
        index_version_store.insert(IndexVersion(
            index_type="bm25", shard_id="shard_js", version=1,
            status="building", file_path="v1",
        ))

        all_active = index_version_store.get_all_active()
        assert len(all_active) == 2
