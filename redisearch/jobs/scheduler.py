"""
Job scheduler — manages a pool of workers and provides
convenience methods for enqueueing common job types.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

from redisearch.config.settings import JobSettings, Settings, get_settings
from redisearch.jobs.worker import Worker
from redisearch.storage.job_store import JobStore

logger = logging.getLogger(__name__)


class Scheduler:
    """
    Manages a pool of Workers and coordinates background processing.

    Typical usage::

        scheduler = Scheduler(job_store)
        scheduler.register("crawl", my_crawl_handler)
        scheduler.register("preprocess", my_preprocess_handler)
        scheduler.start()
        # ... later ...
        scheduler.stop()
    """

    def __init__(
        self,
        job_store: Optional[JobStore] = None,
        settings: Optional[JobSettings] = None,
        db_path: Optional[Path] = None,
    ) -> None:
        s: Settings = get_settings()
        self._job_store = job_store or JobStore(db_path or s.db_path)
        self._settings = settings or s.jobs
        self._workers: list[Worker] = []
        self._handlers: dict[str, Worker.register.__class__] = {}

    @property
    def job_store(self) -> JobStore:
        return self._job_store

    def register(self, job_type: str, handler) -> None:
        """Register a handler that all workers will use."""
        self._handlers[job_type] = handler

    def start(self) -> None:
        """Launch num_workers background threads."""
        if self._workers:
            logger.warning("Scheduler already running — skipping start()")
            return

        for i in range(self._settings.num_workers):
            w = Worker(
                job_store=self._job_store,
                handlers=dict(self._handlers),
                settings=self._settings,
                name=f"worker-{i}",
            )
            w.start()
            self._workers.append(w)

        logger.info("Scheduler started %d workers", len(self._workers))

    def stop(self, timeout: float = 10.0) -> None:
        """Stop all workers."""
        per_worker_timeout = timeout / max(len(self._workers), 1)
        for w in self._workers:
            w.stop(timeout=per_worker_timeout)
        self._workers.clear()
        logger.info("Scheduler stopped all workers")

    @property
    def is_running(self) -> bool:
        return any(w.is_running for w in self._workers)

    @property
    def worker_count(self) -> int:
        return len(self._workers)

    # ----- Convenience enqueue helpers -----

    def enqueue_crawl(self, subreddit: str, max_pages: int = 10, priority: int = 10) -> int:
        """Enqueue a crawl job for a subreddit."""
        return self._job_store.enqueue(
            "crawl",
            {"subreddit": subreddit, "max_pages": max_pages},
            priority=priority,
        )

    def enqueue_preprocess(self, subreddit: Optional[str] = None, priority: int = 20) -> int:
        """Enqueue a preprocessing job."""
        return self._job_store.enqueue(
            "preprocess",
            {"subreddit": subreddit or "all"},
            priority=priority,
        )

    def enqueue_build_index(
        self,
        index_type: str = "bm25",
        subreddit: Optional[str] = None,
        priority: int = 30,
    ) -> int:
        """Enqueue an index build job."""
        return self._job_store.enqueue(
            "build_index",
            {"index_type": index_type, "subreddit": subreddit or "all"},
            priority=priority,
        )

    def enqueue_rebuild(self, priority: int = 5) -> int:
        """Enqueue a full rebuild (crawl + preprocess + build all indexes)."""
        return self._job_store.enqueue("rebuild", {}, priority=priority)

    # ----- Maintenance -----

    def recover_stale(self, max_age_seconds: int = 3600) -> int:
        """Recover jobs stuck in 'running' state."""
        return self._job_store.recover_stale_running(max_age_seconds)

    def cleanup(self, keep_last: int = 100) -> int:
        """Remove old completed jobs."""
        return self._job_store.cleanup_completed(keep_last)
