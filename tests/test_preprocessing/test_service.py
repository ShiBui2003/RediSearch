"""Tests for preprocessing service integration with storage."""

from tests.conftest import make_raw_post
from redisearch.preprocessing.service import PreprocessingService


def test_process_unprocessed_upserts_processed_rows(raw_store, processed_store):
    raw_store.insert(
        make_raw_post(
            post_id="t3_pre001",
            permalink="/r/python/comments/pre001/pre/",
            title="Running tests",
            body="This is a body",
        )
    )
    raw_store.insert(
        make_raw_post(
            post_id="t3_pre002",
            permalink="/r/python/comments/pre002/pre/",
            title="Another post",
            body="with content",
        )
    )

    service = PreprocessingService(raw_store=raw_store, processed_store=processed_store, pipeline_version=1)
    summary = service.process_unprocessed(limit=10)

    assert summary["selected"] == 2
    assert summary["processed"] == 2
    assert processed_store.count(pipeline_version=1) == 2

    processed = processed_store.get_by_id("t3_pre001")
    assert processed is not None
    assert processed.token_count > 0


def test_process_unprocessed_filters_by_subreddit(raw_store, processed_store):
    raw_store.insert(
        make_raw_post(
            post_id="t3_py003",
            subreddit="python",
            permalink="/r/python/comments/py003/pre/",
        )
    )
    raw_store.insert(
        make_raw_post(
            post_id="t3_js003",
            subreddit="javascript",
            permalink="/r/javascript/comments/js003/pre/",
        )
    )

    service = PreprocessingService(raw_store=raw_store, processed_store=processed_store, pipeline_version=1)
    summary = service.process_unprocessed(limit=10, subreddit="python")

    assert summary["subreddit"] == "python"
    assert summary["selected"] == 1
    assert summary["processed"] == 1
    assert processed_store.get_by_id("t3_py003") is not None
    assert processed_store.get_by_id("t3_js003") is None
