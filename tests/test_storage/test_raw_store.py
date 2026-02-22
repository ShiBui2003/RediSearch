"""Tests for the RawPostStore CRUD operations."""

from tests.conftest import make_raw_post


class TestRawPostInsert:
    """Tests for inserting raw posts."""

    def test_insert_single_post(self, raw_store):
        post = make_raw_post()
        assert raw_store.insert(post) is True

    def test_insert_duplicate_is_ignored(self, raw_store):
        post = make_raw_post()
        assert raw_store.insert(post) is True
        assert raw_store.insert(post) is False  # Duplicate

    def test_insert_many(self, raw_store):
        posts = [
            make_raw_post(post_id=f"t3_test{i:03d}", permalink=f"/r/python/comments/test{i:03d}/slug/")
            for i in range(5)
        ]
        count = raw_store.insert_many(posts)
        assert count == 5

    def test_insert_many_skips_duplicates(self, raw_store):
        post = make_raw_post()
        raw_store.insert(post)

        posts = [post, make_raw_post(post_id="t3_new001", permalink="/r/python/comments/new001/slug/")]
        count = raw_store.insert_many(posts)
        assert count == 1  # Only the new one


class TestRawPostRead:
    """Tests for reading raw posts."""

    def test_get_by_id_found(self, raw_store):
        post = make_raw_post()
        raw_store.insert(post)

        result = raw_store.get_by_id("t3_test001")
        assert result is not None
        assert result.id == "t3_test001"
        assert result.title == "Test Post Title"
        assert result.subreddit == "python"

    def test_get_by_id_not_found(self, raw_store):
        assert raw_store.get_by_id("t3_nonexistent") is None

    def test_get_by_ids(self, raw_store):
        for i in range(3):
            raw_store.insert(
                make_raw_post(post_id=f"t3_test{i:03d}", permalink=f"/r/python/comments/test{i:03d}/slug/")
            )

        results = raw_store.get_by_ids(["t3_test000", "t3_test002"])
        assert len(results) == 2
        ids = {r.id for r in results}
        assert ids == {"t3_test000", "t3_test002"}

    def test_get_by_ids_empty_list(self, raw_store):
        assert raw_store.get_by_ids([]) == []

    def test_get_by_subreddit(self, raw_store):
        raw_store.insert(make_raw_post(post_id="t3_py1", permalink="/r/python/comments/py1/s/", subreddit="python"))
        raw_store.insert(make_raw_post(post_id="t3_js1", permalink="/r/javascript/comments/js1/s/", subreddit="javascript"))
        raw_store.insert(make_raw_post(post_id="t3_py2", permalink="/r/python/comments/py2/s/", subreddit="python"))

        results = raw_store.get_by_subreddit("python")
        assert len(results) == 2
        assert all(r.subreddit == "python" for r in results)

    def test_exists_true(self, raw_store):
        raw_store.insert(make_raw_post())
        assert raw_store.exists("t3_test001") is True

    def test_exists_false(self, raw_store):
        assert raw_store.exists("t3_nonexistent") is False

    def test_get_all_ids(self, raw_store):
        for i in range(3):
            raw_store.insert(
                make_raw_post(post_id=f"t3_test{i:03d}", permalink=f"/r/python/comments/test{i:03d}/slug/")
            )
        ids = raw_store.get_all_ids()
        assert len(ids) == 3

    def test_count_total(self, raw_store):
        for i in range(3):
            raw_store.insert(
                make_raw_post(post_id=f"t3_test{i:03d}", permalink=f"/r/python/comments/test{i:03d}/slug/")
            )
        assert raw_store.count() == 3

    def test_count_by_subreddit(self, raw_store):
        raw_store.insert(make_raw_post(post_id="t3_py1", permalink="/r/python/comments/py1/s/", subreddit="python"))
        raw_store.insert(make_raw_post(post_id="t3_js1", permalink="/r/javascript/comments/js1/s/", subreddit="javascript"))
        assert raw_store.count("python") == 1
        assert raw_store.count("javascript") == 1

    def test_get_subreddits(self, raw_store):
        raw_store.insert(make_raw_post(post_id="t3_py1", permalink="/r/python/comments/py1/s/", subreddit="python"))
        raw_store.insert(make_raw_post(post_id="t3_js1", permalink="/r/javascript/comments/js1/s/", subreddit="javascript"))
        subs = raw_store.get_subreddits()
        assert subs == ["javascript", "python"]  # Alphabetical

    def test_raw_html_roundtrip(self, raw_store):
        """Binary data (compressed HTML) survives insert/read cycle."""
        html_blob = b"<html><body>compressed content</body></html>"
        post = make_raw_post(raw_html=html_blob)
        raw_store.insert(post)

        result = raw_store.get_by_id("t3_test001")
        assert result.raw_html == html_blob


class TestRawPostUnprocessed:
    """Tests for finding unprocessed posts."""

    def test_unprocessed_returns_all_when_none_processed(self, raw_store):
        for i in range(3):
            raw_store.insert(
                make_raw_post(post_id=f"t3_test{i:03d}", permalink=f"/r/python/comments/test{i:03d}/slug/")
            )
        ids = raw_store.get_unprocessed_ids(current_version=1)
        assert len(ids) == 3

    def test_unprocessed_excludes_processed_at_current_version(self, raw_store, processed_store):
        from tests.conftest import make_processed_post

        for i in range(3):
            raw_store.insert(
                make_raw_post(post_id=f"t3_test{i:03d}", permalink=f"/r/python/comments/test{i:03d}/slug/")
            )

        # Process one of them
        processed_store.upsert(make_processed_post(post_id="t3_test001", pipeline_version=1))

        ids = raw_store.get_unprocessed_ids(current_version=1)
        assert len(ids) == 2
        assert "t3_test001" not in ids

    def test_unprocessed_includes_stale_version(self, raw_store, processed_store):
        from tests.conftest import make_processed_post

        raw_store.insert(make_raw_post())
        processed_store.upsert(make_processed_post(pipeline_version=1))

        # Now bump version — the processed post should be stale
        ids = raw_store.get_unprocessed_ids(current_version=2)
        assert len(ids) == 1
