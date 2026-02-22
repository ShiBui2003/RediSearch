"""
Background job worker.

Each worker runs in its own thread, polling the job queue for work.
When a job is claimed it gets dispatched to the appropriate handler
based on ``job_type``.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from typing import Callable, Optional

from redisearch.config.settings import JobSettings, Settings, get_settings
from redisearch.storage.job_store import JobStore
from redisearch.storage.models import Job

logger = logging.getLogger(__name__)

# Registry of handlers: job_type -> callable(payload_dict) -> None
JobHandler = Callable[[dict], None]


class Worker:
    """
    Single-threaded job worker.

    Polls the job queue at a configurable interval, claims the
    next pending job, and dispatches it to a registered handler.
    Handles retries and failure logging.
    """

    def __init__(
        self,
        job_store: JobStore,
        handlers: dict[str, JobHandler] | None = None,
        settings: JobSettings | None = None,
        name: str = "worker-0",
    ) -> None:
        s: Settings = get_settings()
        self._job_store = job_store
        self._handlers: dict[str, JobHandler] = handlers or {}
        self._settings = settings or s.jobs
        self._name = name
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def register(self, job_type: str, handler: JobHandler) -> None:
        """Register a handler for a specific job type."""
        self._handlers[job_type] = handler

    def start(self) -> None:
        """Start the worker in a background thread."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._loop, name=self._name, daemon=True
        )
        self._thread.start()
        logger.info("Worker '%s' started", self._name)

    def stop(self, timeout: float = 5.0) -> None:
        """Signal the worker to stop and wait for the thread to finish."""
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)
        logger.info("Worker '%s' stopped", self._name)

    @property
    def is_running(self) -> bool:
        return self._running

    def _loop(self) -> None:
        """Main poll loop."""
        while self._running:
            try:
                self._tick()
            except Exception:
                logger.exception("Unhandled error in worker '%s'", self._name)
            time.sleep(self._settings.poll_interval)

    def _tick(self) -> None:
        """Attempt to claim and execute one job."""
        job = self._job_store.claim_next()
        if job is None:
            return

        handler = self._handlers.get(job.job_type)
        if handler is None:
            self._job_store.fail(job.id, f"No handler registered for '{job.job_type}'")
            logger.error("No handler for job type '%s' (job #%d)", job.job_type, job.id)
            return

        try:
            payload = self._job_store.get_payload(job)
            logger.info("[%s] Executing job #%d (%s)", self._name, job.id, job.job_type)
            handler(payload)
            self._job_store.complete(job.id)
        except Exception as exc:
            error_msg = f"{type(exc).__name__}: {exc}"
            self._job_store.fail(job.id, error_msg)
            logger.exception("[%s] Job #%d failed: %s", self._name, job.id, error_msg)

            # Auto-retry if under max retries
            updated = self._job_store.get_by_id(job.id)
            if updated and updated.retries < self._settings.max_retries:
                self._job_store.retry(job.id)
                logger.info("[%s] Job #%d re-enqueued (retry %d/%d)",
                            self._name, job.id, updated.retries, self._settings.max_retries)

    def run_once(self) -> Optional[int]:
        """
        Execute a single tick synchronously (useful for testing).
        Returns the job ID that was executed, or None if queue was empty.
        """
        job = self._job_store.claim_next()
        if job is None:
            return None

        handler = self._handlers.get(job.job_type)
        if handler is None:
            self._job_store.fail(job.id, f"No handler for '{job.job_type}'")
            return job.id

        try:
            payload = self._job_store.get_payload(job)
            handler(payload)
            self._job_store.complete(job.id)
        except Exception as exc:
            self._job_store.fail(job.id, f"{type(exc).__name__}: {exc}")
            updated = self._job_store.get_by_id(job.id)
            if updated and updated.retries < self._settings.max_retries:
                self._job_store.retry(job.id)

        return job.id
