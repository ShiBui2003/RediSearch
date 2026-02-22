"""Tests for BM25Searcher against a built index."""

import json

from tests.conftest import make_raw_post, make_processed_post
from redisearch.indexing.bm25_builder import BM25IndexBuilder
from redisearch.search.bm25_searcher import BM25Searcher


def _seed_and_build(raw_store, processed_store, index_version_store, tmp_path):
    """Insert sample posts, preprocess them, and build a BM25 index."""
    posts = [
        ("t3_s001", "python", "Python decorators guide", ["python", "decor", "guid"]),
        ("t3_s002", "python", "Python basics tutorial", ["python", "basic", "tutori"]),
        ("t3_s003", "python", "JavaScript closures", ["javascript", "closur"]),
    ]
    for pid, sub, title, tokens in posts:
        raw_store.insert(make_raw_post(post_id=pid, subreddit=sub, title=title, permalink=f"/r/{sub}/comments/{pid[3:]}/s/"))
        processed_store.upsert(
            make_processed_post(
                post_id=pid,
                all_tokens=json.dumps(tokens),
                token_count=len(tokens),
            )
        )

    builder = BM25IndexBuilder(
        processed_store=processed_store,
        raw_store=raw_store,
        version_store=index_version_store,
        project_root=tmp_path,
    )
    builder.build_subreddit("python")


def test_search_returns_ranked_hits(
    raw_store, processed_store, index_version_store, tmp_path, db_path
):
    _seed_and_build(raw_store, processed_store, index_version_store, tmp_path)

    searcher = BM25Searcher(
        version_store=index_version_store,
        project_root=tmp_path,
    )
    hits = searcher.search("python decorators", subreddit="python", top_k=3)

    assert len(hits) > 0
    assert hits[0].id == "t3_s001"
    assert hits[0].score > 0


def test_search_empty_query_returns_nothing(
    raw_store, processed_store, index_version_store, tmp_path
):
    _seed_and_build(raw_store, processed_store, index_version_store, tmp_path)

    searcher = BM25Searcher(
        version_store=index_version_store,
        project_root=tmp_path,
    )
    hits = searcher.search("", subreddit="python")
    assert hits == []


def test_search_no_active_index_returns_nothing(
    raw_store, processed_store, index_version_store, tmp_path
):
    searcher = BM25Searcher(
        version_store=index_version_store,
        project_root=tmp_path,
    )
    hits = searcher.search("python", subreddit="nonexistent")
    assert hits == []
