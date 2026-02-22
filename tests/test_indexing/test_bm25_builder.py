"""Tests for BM25IndexBuilder end-to-end with test database."""

import json

from tests.conftest import make_raw_post, make_processed_post
from redisearch.indexing.bm25_builder import BM25IndexBuilder


def test_build_subreddit_creates_active_index(
    raw_store, processed_store, index_version_store, tmp_path, db_path
):
    """Builder should create index file and activate it in version store."""
    raw_store.insert(make_raw_post(post_id="t3_b001", permalink="/r/python/comments/b001/s/"))
    processed_store.upsert(
        make_processed_post(
            post_id="t3_b001",
            all_tokens=json.dumps(["python", "decorators", "advanced"]),
            token_count=3,
        )
    )

    builder = BM25IndexBuilder(
        processed_store=processed_store,
        raw_store=raw_store,
        version_store=index_version_store,
        project_root=tmp_path,
    )
    summary = builder.build_subreddit("python")

    assert summary["subreddit"] == "python"
    assert summary["doc_count"] == 1
    assert summary["version"] == 1

    active = index_version_store.get_active("bm25", "shard_python")
    assert active is not None
    assert active.status == "active"
    assert active.doc_count == 1


def test_build_subreddit_empty_returns_zero(
    raw_store, processed_store, index_version_store, tmp_path
):
    """Builder should return zero doc_count when no processed posts exist."""
    builder = BM25IndexBuilder(
        processed_store=processed_store,
        raw_store=raw_store,
        version_store=index_version_store,
        project_root=tmp_path,
    )
    summary = builder.build_subreddit("empty")
    assert summary["doc_count"] == 0
    assert summary["version"] == 0


def test_build_increments_version(
    raw_store, processed_store, index_version_store, tmp_path
):
    """Consecutive builds should produce incrementing version numbers."""
    raw_store.insert(make_raw_post(post_id="t3_v001", permalink="/r/python/comments/v001/s/"))
    processed_store.upsert(
        make_processed_post(
            post_id="t3_v001",
            all_tokens=json.dumps(["python", "test"]),
            token_count=2,
        )
    )

    builder = BM25IndexBuilder(
        processed_store=processed_store,
        raw_store=raw_store,
        version_store=index_version_store,
        project_root=tmp_path,
    )
    s1 = builder.build_subreddit("python")
    s2 = builder.build_subreddit("python")

    assert s1["version"] == 1
    assert s2["version"] == 2
    assert index_version_store.get_active("bm25", "shard_python").version == 2
