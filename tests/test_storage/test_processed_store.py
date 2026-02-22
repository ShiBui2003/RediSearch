"""Tests for the ProcessedPostStore CRUD operations."""

from tests.conftest import make_raw_post, make_processed_post


class TestProcessedPostWrite:
    """Tests for writing processed posts."""

    def test_upsert_insert(self, raw_store, processed_store):
        raw_store.insert(make_raw_post())
        post = make_processed_post()
        processed_store.upsert(post)

        result = processed_store.get_by_id("t3_test001")
        assert result is not None
        assert result.id == "t3_test001"
        assert result.pipeline_version == 1

    def test_upsert_replaces_existing(self, raw_store, processed_store):
        raw_store.insert(make_raw_post())
        processed_store.upsert(make_processed_post(pipeline_version=1))

        # Re-process at higher version
        processed_store.upsert(make_processed_post(pipeline_version=2))

        result = processed_store.get_by_id("t3_test001")
        assert result.pipeline_version == 2

    def test_upsert_many(self, raw_store, processed_store):
        for i in range(5):
            raw_store.insert(
                make_raw_post(post_id=f"t3_test{i:03d}", permalink=f"/r/python/comments/test{i:03d}/slug/")
            )

        posts = [make_processed_post(post_id=f"t3_test{i:03d}") for i in range(5)]
        count = processed_store.upsert_many(posts)
        assert count == 5


class TestProcessedPostRead:
    """Tests for reading processed posts."""

    def test_get_by_id_found(self, raw_store, processed_store):
        raw_store.insert(make_raw_post())
        processed_store.upsert(make_processed_post())

        result = processed_store.get_by_id("t3_test001")
        assert result is not None
        assert result.title_tokens == '["test", "post", "titl"]'

    def test_get_by_id_not_found(self, processed_store):
        assert processed_store.get_by_id("t3_nonexistent") is None

    def test_get_by_ids(self, raw_store, processed_store):
        for i in range(3):
            raw_store.insert(
                make_raw_post(post_id=f"t3_test{i:03d}", permalink=f"/r/python/comments/test{i:03d}/slug/")
            )
            processed_store.upsert(make_processed_post(post_id=f"t3_test{i:03d}"))

        results = processed_store.get_by_ids(["t3_test000", "t3_test002"])
        assert len(results) == 2

    def test_get_all_for_subreddit(self, raw_store, processed_store):
        raw_store.insert(make_raw_post(post_id="t3_py1", permalink="/r/python/comments/py1/s/", subreddit="python"))
        raw_store.insert(make_raw_post(post_id="t3_js1", permalink="/r/javascript/comments/js1/s/", subreddit="javascript"))
        processed_store.upsert(make_processed_post(post_id="t3_py1"))
        processed_store.upsert(make_processed_post(post_id="t3_js1"))

        results = processed_store.get_all_for_subreddit("python")
        assert len(results) == 1
        assert results[0].id == "t3_py1"

    def test_get_stale(self, raw_store, processed_store):
        raw_store.insert(make_raw_post())
        processed_store.upsert(make_processed_post(pipeline_version=1))

        stale = processed_store.get_stale(current_version=2)
        assert len(stale) == 1
        assert stale[0].pipeline_version == 1

    def test_get_stale_none_when_up_to_date(self, raw_store, processed_store):
        raw_store.insert(make_raw_post())
        processed_store.upsert(make_processed_post(pipeline_version=2))

        stale = processed_store.get_stale(current_version=2)
        assert len(stale) == 0

    def test_count(self, raw_store, processed_store):
        for i in range(3):
            raw_store.insert(
                make_raw_post(post_id=f"t3_test{i:03d}", permalink=f"/r/python/comments/test{i:03d}/slug/")
            )
            processed_store.upsert(make_processed_post(post_id=f"t3_test{i:03d}"))

        assert processed_store.count() == 3


class TestProcessedPostDelete:
    """Tests for deleting processed posts."""

    def test_delete_by_ids(self, raw_store, processed_store):
        for i in range(3):
            raw_store.insert(
                make_raw_post(post_id=f"t3_test{i:03d}", permalink=f"/r/python/comments/test{i:03d}/slug/")
            )
            processed_store.upsert(make_processed_post(post_id=f"t3_test{i:03d}"))

        deleted = processed_store.delete_by_ids(["t3_test000", "t3_test001"])
        assert deleted == 2
        assert processed_store.count() == 1

    def test_delete_all(self, raw_store, processed_store):
        for i in range(3):
            raw_store.insert(
                make_raw_post(post_id=f"t3_test{i:03d}", permalink=f"/r/python/comments/test{i:03d}/slug/")
            )
            processed_store.upsert(make_processed_post(post_id=f"t3_test{i:03d}"))

        deleted = processed_store.delete_all()
        assert deleted == 3
        assert processed_store.count() == 0
