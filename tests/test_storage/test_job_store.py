"""Tests for the JobStore CRUD operations."""

import json


class TestJobEnqueue:
    """Tests for enqueuing jobs."""

    def test_enqueue_returns_id(self, job_store):
        job_id = job_store.enqueue("crawl", {"subreddit": "python"})
        assert isinstance(job_id, int)
        assert job_id > 0

    def test_enqueue_sets_pending_status(self, job_store):
        job_id = job_store.enqueue("crawl")
        job = job_store.get_by_id(job_id)
        assert job.status == "pending"

    def test_enqueue_stores_payload(self, job_store):
        payload = {"subreddit": "python", "max_pages": 5}
        job_id = job_store.enqueue("crawl", payload)
        job = job_store.get_by_id(job_id)
        assert json.loads(job.payload) == payload


class TestJobClaim:
    """Tests for claiming jobs from the queue."""

    def test_claim_next_returns_job(self, job_store):
        job_store.enqueue("crawl")
        job = job_store.claim_next()
        assert job is not None
        assert job.status == "running"
        assert job.started_at is not None

    def test_claim_next_respects_priority(self, job_store):
        job_store.enqueue("build_index", priority=20)
        job_store.enqueue("crawl", priority=5)  # Higher priority

        job = job_store.claim_next()
        assert job.job_type == "crawl"

    def test_claim_next_filters_by_type(self, job_store):
        job_store.enqueue("crawl")
        job_store.enqueue("preprocess")

        job = job_store.claim_next(job_type="preprocess")
        assert job.job_type == "preprocess"

    def test_claim_next_returns_none_when_empty(self, job_store):
        assert job_store.claim_next() is None

    def test_claim_does_not_double_claim(self, job_store):
        job_store.enqueue("crawl")
        job1 = job_store.claim_next()
        job2 = job_store.claim_next()
        assert job1 is not None
        assert job2 is None  # Already claimed


class TestJobLifecycle:
    """Tests for the full job lifecycle."""

    def test_complete_marks_done(self, job_store):
        job_id = job_store.enqueue("crawl")
        job_store.claim_next()
        job_store.complete(job_id)

        job = job_store.get_by_id(job_id)
        assert job.status == "completed"
        assert job.completed_at is not None

    def test_fail_stores_error(self, job_store):
        job_id = job_store.enqueue("crawl")
        job_store.claim_next()
        job_store.fail(job_id, "Connection refused")

        job = job_store.get_by_id(job_id)
        assert job.status == "failed"
        assert job.error == "Connection refused"
        assert job.retries == 1

    def test_retry_resets_to_pending(self, job_store):
        job_id = job_store.enqueue("crawl")
        job_store.claim_next()
        job_store.fail(job_id, "Timeout")
        job_store.retry(job_id)

        job = job_store.get_by_id(job_id)
        assert job.status == "pending"
        assert job.started_at is None
        assert job.error is None


class TestJobQueries:
    """Tests for job query operations."""

    def test_get_pending_count(self, job_store):
        job_store.enqueue("crawl")
        job_store.enqueue("crawl")
        job_store.enqueue("preprocess")

        assert job_store.get_pending_count() == 3
        assert job_store.get_pending_count("crawl") == 2
        assert job_store.get_pending_count("preprocess") == 1

    def test_get_running(self, job_store):
        job_store.enqueue("crawl")
        job_store.enqueue("preprocess")
        job_store.claim_next()

        running = job_store.get_running()
        assert len(running) == 1

    def test_get_failed(self, job_store):
        job_id = job_store.enqueue("crawl")
        job_store.claim_next()
        job_store.fail(job_id, "Error")

        failed = job_store.get_failed()
        assert len(failed) == 1

    def test_cleanup_completed(self, job_store):
        for i in range(5):
            job_id = job_store.enqueue("crawl")
            job_store.claim_next()
            job_store.complete(job_id)

        cleaned = job_store.cleanup_completed(keep_last=2)
        assert cleaned == 3

    def test_get_payload(self, job_store):
        payload = {"subreddit": "python", "pages": 10}
        job_id = job_store.enqueue("crawl", payload)
        job = job_store.get_by_id(job_id)
        result = job_store.get_payload(job)
        assert result == payload
