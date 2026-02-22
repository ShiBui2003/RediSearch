"""Tests for the background job worker."""

from __future__ import annotations

from pathlib import Path

import pytest

from redisearch.config.settings import JobSettings
from redisearch.jobs.worker import Worker
from redisearch.storage.job_store import JobStore
from redisearch.storage.schema import initialize_database


@pytest.fixture
def job_env(tmp_path: Path):
    """Set up a job store and return (job_store, settings)."""
    db_path = tmp_path / "test.db"
    initialize_database(db_path)
    store = JobStore(db_path)
    settings = JobSettings(num_workers=1, poll_interval=0.1, max_retries=2)
    return store, settings


class TestWorkerRunOnce:
    def test_processes_job(self, job_env):
        store, settings = job_env
        results = []

        def handler(payload):
            results.append(payload)

        w = Worker(store, handlers={"test": handler}, settings=settings)
        store.enqueue("test", {"key": "value"})
        job_id = w.run_once()

        assert job_id is not None
        assert len(results) == 1
        assert results[0]["key"] == "value"
        job = store.get_by_id(job_id)
        assert job.status == "completed"

    def test_empty_queue_returns_none(self, job_env):
        store, settings = job_env
        w = Worker(store, handlers={}, settings=settings)
        assert w.run_once() is None

    def test_missing_handler_fails_job(self, job_env):
        store, settings = job_env
        w = Worker(store, handlers={}, settings=settings)
        store.enqueue("unknown_type", {})
        job_id = w.run_once()

        assert job_id is not None
        job = store.get_by_id(job_id)
        assert job.status == "failed"
        assert "No handler" in job.error

    def test_handler_exception_fails_and_retries(self, job_env):
        store, settings = job_env

        def bad_handler(payload):
            raise ValueError("boom")

        w = Worker(store, handlers={"test": bad_handler}, settings=settings)
        store.enqueue("test", {})
        job_id = w.run_once()

        # Job should be re-enqueued for retry (retries=1 < max_retries=2)
        job = store.get_by_id(job_id)
        assert job.status == "pending"

    def test_handler_exhausts_retries(self, job_env):
        store, settings = job_env

        def bad_handler(payload):
            raise ValueError("boom")

        w = Worker(store, handlers={"test": bad_handler}, settings=settings)
        store.enqueue("test", {})

        # First run — retries=1, re-enqueued
        w.run_once()
        # Second run — retries=2, re-enqueued (retries < max_retries=2? No, 2 == 2)
        w.run_once()
        # Now retries == 2 == max_retries, should stay failed
        job = store.get_by_id(1)
        assert job.status == "failed"

    def test_priority_ordering(self, job_env):
        store, settings = job_env
        executed = []

        def handler(payload):
            executed.append(payload.get("order"))

        w = Worker(store, handlers={"test": handler}, settings=settings)
        store.enqueue("test", {"order": "low"}, priority=20)
        store.enqueue("test", {"order": "high"}, priority=1)

        w.run_once()  # Should pick high priority first
        w.run_once()

        assert executed == ["high", "low"]


class TestWorkerThread:
    def test_start_and_stop(self, job_env):
        store, settings = job_env
        w = Worker(store, handlers={}, settings=settings)
        w.start()
        assert w.is_running
        w.stop(timeout=2.0)
        assert not w.is_running

    def test_register_handler(self, job_env):
        store, settings = job_env
        w = Worker(store, settings=settings)
        w.register("test", lambda p: None)
        assert "test" in w._handlers
