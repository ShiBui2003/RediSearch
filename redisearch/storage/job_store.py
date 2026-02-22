"""
CRUD operations for the jobs table.

This is a SQLite-backed job queue. Workers claim jobs atomically
by updating the status from 'pending' to 'running' in a single
statement. This avoids the need for external message brokers.

The queue is durable: jobs survive process restarts. If a worker
crashes mid-job, the job stays in 'running' status and can be
reclaimed by a recovery sweep.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from redisearch.storage.connection import get_connection
from redisearch.storage.models import Job

logger = logging.getLogger(__name__)


class JobStore:
    """CRUD interface for the jobs table (background job queue)."""

    def __init__(self, db_path: Optional[Path] = None) -> None:
        self._db_path = db_path

    @property
    def _conn(self):
        return get_connection(self._db_path)

    def _row_to_job(self, row) -> Job:
        return Job(
            id=row["id"],
            job_type=row["job_type"],
            status=row["status"],
            payload=row["payload"],
            priority=row["priority"],
            created_at=row["created_at"],
            started_at=row["started_at"],
            completed_at=row["completed_at"],
            error=row["error"],
            retries=row["retries"],
        )

    # ----- Queue operations -----

    def enqueue(
        self,
        job_type: str,
        payload: dict[str, Any] | None = None,
        priority: int = 10,
    ) -> int:
        """
        Add a job to the queue. Returns the job ID.

        Args:
            job_type: One of "crawl", "preprocess", "build_index", "rebuild"
            payload: Task-specific parameters (will be JSON-serialized)
            priority: Lower = higher priority (default 10)
        """
        now = datetime.now(timezone.utc).isoformat()
        payload_json = json.dumps(payload or {})

        sql = """
            INSERT INTO jobs (job_type, status, payload, priority, created_at, retries)
            VALUES (?, 'pending', ?, ?, ?, 0)
        """
        with self._conn:
            cursor = self._conn.execute(sql, (job_type, payload_json, priority, now))
            job_id = cursor.lastrowid
            logger.info("Enqueued job #%d: %s (priority=%d)", job_id, job_type, priority)
            return job_id

    def claim_next(self, job_type: Optional[str] = None) -> Optional[Job]:
        """
        Atomically claim the next pending job.

        Picks the highest-priority (lowest number) pending job,
        sets its status to 'running', and returns it.

        Args:
            job_type: If given, only claim jobs of this type.

        Returns:
            The claimed Job, or None if no pending jobs exist.
        """
        now = datetime.now(timezone.utc).isoformat()

        if job_type:
            # Find the next pending job of the specified type
            where_clause = "WHERE status = 'pending' AND job_type = ?"
            params: tuple = (job_type,)
        else:
            where_clause = "WHERE status = 'pending'"
            params = ()

        with self._conn:
            # Find the job to claim
            row = self._conn.execute(
                f"SELECT id FROM jobs {where_clause} ORDER BY priority ASC, created_at ASC LIMIT 1",
                params,
            ).fetchone()

            if row is None:
                return None

            job_id = row["id"]

            # Claim it atomically
            self._conn.execute(
                "UPDATE jobs SET status = 'running', started_at = ? WHERE id = ? AND status = 'pending'",
                (now, job_id),
            )

            # Fetch the full job
            claimed_row = self._conn.execute(
                "SELECT * FROM jobs WHERE id = ?", (job_id,)
            ).fetchone()

        if claimed_row and claimed_row["status"] == "running":
            job = self._row_to_job(claimed_row)
            logger.info("Claimed job #%d: %s", job.id, job.job_type)
            return job

        return None

    def complete(self, job_id: int) -> None:
        """Mark a job as completed."""
        now = datetime.now(timezone.utc).isoformat()
        with self._conn:
            self._conn.execute(
                "UPDATE jobs SET status = 'completed', completed_at = ? WHERE id = ?",
                (now, job_id),
            )
        logger.info("Completed job #%d", job_id)

    def fail(self, job_id: int, error: str) -> None:
        """
        Mark a job as failed with an error message.
        Increments the retry counter.
        """
        now = datetime.now(timezone.utc).isoformat()
        with self._conn:
            self._conn.execute(
                """
                UPDATE jobs
                SET status = 'failed', completed_at = ?, error = ?, retries = retries + 1
                WHERE id = ?
                """,
                (now, error, job_id),
            )
        logger.warning("Failed job #%d: %s", job_id, error)

    def retry(self, job_id: int) -> bool:
        """
        Re-enqueue a failed job for retry.

        Returns True if the job was re-enqueued, False if it has
        exceeded max retries (checked by the caller).
        """
        with self._conn:
            self._conn.execute(
                "UPDATE jobs SET status = 'pending', started_at = NULL, completed_at = NULL, error = NULL WHERE id = ?",
                (job_id,),
            )
        logger.info("Re-enqueued job #%d for retry", job_id)
        return True

    # ----- Read operations -----

    def get_by_id(self, job_id: int) -> Optional[Job]:
        """Fetch a single job by ID."""
        row = self._conn.execute(
            "SELECT * FROM jobs WHERE id = ?", (job_id,)
        ).fetchone()
        return self._row_to_job(row) if row else None

    def get_pending_count(self, job_type: Optional[str] = None) -> int:
        """Count pending jobs, optionally filtered by type."""
        if job_type:
            row = self._conn.execute(
                "SELECT COUNT(*) as cnt FROM jobs WHERE status = 'pending' AND job_type = ?",
                (job_type,),
            ).fetchone()
        else:
            row = self._conn.execute(
                "SELECT COUNT(*) as cnt FROM jobs WHERE status = 'pending'"
            ).fetchone()
        return row["cnt"]

    def get_running(self) -> list[Job]:
        """Get all currently running jobs."""
        rows = self._conn.execute(
            "SELECT * FROM jobs WHERE status = 'running' ORDER BY started_at"
        ).fetchall()
        return [self._row_to_job(r) for r in rows]

    def get_failed(self, limit: int = 50) -> list[Job]:
        """Get recent failed jobs."""
        rows = self._conn.execute(
            "SELECT * FROM jobs WHERE status = 'failed' ORDER BY completed_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [self._row_to_job(r) for r in rows]

    def get_payload(self, job: Job) -> dict[str, Any]:
        """Parse the JSON payload of a job into a dict."""
        return json.loads(job.payload)

    def cleanup_completed(self, keep_last: int = 100) -> int:
        """
        Delete old completed jobs, keeping the most recent `keep_last`.
        Prevents the jobs table from growing unbounded.
        """
        with self._conn:
            cursor = self._conn.execute(
                """
                DELETE FROM jobs WHERE status = 'completed' AND id NOT IN (
                    SELECT id FROM jobs WHERE status = 'completed'
                    ORDER BY completed_at DESC LIMIT ?
                )
                """,
                (keep_last,),
            )
            count = cursor.rowcount
            if count > 0:
                logger.info("Cleaned up %d old completed jobs", count)
            return count

    def recover_stale_running(self, max_age_seconds: int = 3600) -> int:
        """
        Reset jobs stuck in 'running' state back to 'pending'.

        This handles the case where a worker crashes without marking the
        job as completed or failed. Jobs running longer than max_age_seconds
        are assumed to be dead.
        """
        with self._conn:
            cursor = self._conn.execute(
                """
                UPDATE jobs SET status = 'pending', started_at = NULL
                WHERE status = 'running'
                AND started_at < datetime('now', ?)
                """,
                (f"-{max_age_seconds} seconds",),
            )
            count = cursor.rowcount
            if count > 0:
                logger.warning("Recovered %d stale running jobs", count)
            return count
