"""Tests for the Scheduler."""

from __future__ import annotations

from pathlib import Path

import pytest

from redisearch.config.settings import JobSettings
from redisearch.jobs.scheduler import Scheduler
from redisearch.storage.job_store import JobStore
from redisearch.storage.schema import initialize_database


@pytest.fixture
def sched_env(tmp_path: Path):
    db_path = tmp_path / "test.db"
    initialize_database(db_path)
    store = JobStore(db_path)
    settings = JobSettings(num_workers=2, poll_interval=0.1, max_retries=2)
    return store, settings


class TestScheduler:
    def test_start_and_stop(self, sched_env):
        store, settings = sched_env
        s = Scheduler(job_store=store, settings=settings)
        s.start()
        assert s.is_running
        assert s.worker_count == 2
        s.stop(timeout=3.0)
        assert not s.is_running
        assert s.worker_count == 0

    def test_duplicate_start_ignored(self, sched_env):
        store, settings = sched_env
        s = Scheduler(job_store=store, settings=settings)
        s.start()
        s.start()  # Should be a no-op
        assert s.worker_count == 2
        s.stop(timeout=3.0)

    def test_enqueue_crawl(self, sched_env):
        store, settings = sched_env
        s = Scheduler(job_store=store, settings=settings)
        job_id = s.enqueue_crawl("python", max_pages=5)
        job = store.get_by_id(job_id)
        assert job.job_type == "crawl"
        assert '"subreddit": "python"' in job.payload

    def test_enqueue_preprocess(self, sched_env):
        store, settings = sched_env
        s = Scheduler(job_store=store, settings=settings)
        job_id = s.enqueue_preprocess("rust")
        job = store.get_by_id(job_id)
        assert job.job_type == "preprocess"

    def test_enqueue_build_index(self, sched_env):
        store, settings = sched_env
        s = Scheduler(job_store=store, settings=settings)
        job_id = s.enqueue_build_index("bm25", "python")
        job = store.get_by_id(job_id)
        assert job.job_type == "build_index"

    def test_enqueue_rebuild(self, sched_env):
        store, settings = sched_env
        s = Scheduler(job_store=store, settings=settings)
        job_id = s.enqueue_rebuild()
        job = store.get_by_id(job_id)
        assert job.job_type == "rebuild"
        assert job.priority == 5  # high priority

    def test_cleanup(self, sched_env):
        store, settings = sched_env
        s = Scheduler(job_store=store, settings=settings)
        # Enqueue + complete 3 jobs
        for i in range(3):
            jid = store.enqueue("test", {})
            store.claim_next()
            store.complete(jid)
        count = s.cleanup(keep_last=1)
        assert count == 2
