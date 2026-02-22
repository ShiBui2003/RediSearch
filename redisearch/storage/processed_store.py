"""
CRUD operations for the processed_posts table.

Processed posts are derived from raw posts via the preprocessing pipeline.
They are disposable — if the pipeline changes, rows are deleted and rebuilt.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

from redisearch.storage.connection import get_connection
from redisearch.storage.models import ProcessedPost

logger = logging.getLogger(__name__)


class ProcessedPostStore:
    """CRUD interface for the processed_posts table."""

    def __init__(self, db_path: Optional[Path] = None) -> None:
        self._db_path = db_path

    @property
    def _conn(self):
        return get_connection(self._db_path)

    # ----- Write operations -----

    def upsert(self, post: ProcessedPost) -> None:
        """
        Insert or replace a processed post.

        Unlike raw posts, processed posts are replaceable — when the
        pipeline version changes, we re-process and overwrite.
        """
        sql = """
            INSERT OR REPLACE INTO processed_posts
                (id, title_tokens, body_tokens, all_tokens,
                 token_count, pipeline_version, processed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        with self._conn:
            self._conn.execute(
                sql,
                (
                    post.id,
                    post.title_tokens,
                    post.body_tokens,
                    post.all_tokens,
                    post.token_count,
                    post.pipeline_version,
                    post.processed_at,
                ),
            )
        logger.debug("Upserted processed post: %s (v%d)", post.id, post.pipeline_version)

    def upsert_many(self, posts: list[ProcessedPost]) -> int:
        """
        Insert or replace multiple processed posts.
        Returns the number of rows affected.
        """
        sql = """
            INSERT OR REPLACE INTO processed_posts
                (id, title_tokens, body_tokens, all_tokens,
                 token_count, pipeline_version, processed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        rows = [
            (
                p.id, p.title_tokens, p.body_tokens, p.all_tokens,
                p.token_count, p.pipeline_version, p.processed_at,
            )
            for p in posts
        ]
        with self._conn:
            cursor = self._conn.executemany(sql, rows)
            count = cursor.rowcount
            logger.info("Bulk upsert: %d processed posts", count)
            return count

    # ----- Read operations -----

    def _row_to_post(self, row) -> ProcessedPost:
        """Convert a sqlite3.Row to a ProcessedPost dataclass."""
        return ProcessedPost(
            id=row["id"],
            title_tokens=row["title_tokens"],
            body_tokens=row["body_tokens"],
            all_tokens=row["all_tokens"],
            token_count=row["token_count"],
            pipeline_version=row["pipeline_version"],
            processed_at=row["processed_at"],
        )

    def get_by_id(self, post_id: str) -> Optional[ProcessedPost]:
        """Fetch a single processed post by ID."""
        row = self._conn.execute(
            "SELECT * FROM processed_posts WHERE id = ?", (post_id,)
        ).fetchone()
        return self._row_to_post(row) if row else None

    def get_by_ids(self, post_ids: list[str]) -> list[ProcessedPost]:
        """Fetch multiple processed posts by ID."""
        if not post_ids:
            return []
        placeholders = ",".join("?" for _ in post_ids)
        rows = self._conn.execute(
            f"SELECT * FROM processed_posts WHERE id IN ({placeholders})", post_ids
        ).fetchall()
        return [self._row_to_post(r) for r in rows]

    def get_all_for_subreddit(self, subreddit: str) -> list[ProcessedPost]:
        """
        Fetch all processed posts for a subreddit.

        Joins with raw_posts to filter by subreddit since processed_posts
        doesn't store the subreddit (it's denormalized in raw only).
        """
        rows = self._conn.execute(
            """
            SELECT p.* FROM processed_posts p
            JOIN raw_posts r ON p.id = r.id
            WHERE r.subreddit = ?
            ORDER BY r.created_utc DESC
            """,
            (subreddit.lower(),),
        ).fetchall()
        return [self._row_to_post(r) for r in rows]

    def get_stale(self, current_version: int, limit: int = 1000) -> list[ProcessedPost]:
        """
        Fetch processed posts with a pipeline version older than current.
        These need reprocessing.
        """
        rows = self._conn.execute(
            "SELECT * FROM processed_posts WHERE pipeline_version < ? LIMIT ?",
            (current_version, limit),
        ).fetchall()
        return [self._row_to_post(r) for r in rows]

    def count(self, pipeline_version: Optional[int] = None) -> int:
        """Count processed posts, optionally filtered by pipeline version."""
        if pipeline_version is not None:
            row = self._conn.execute(
                "SELECT COUNT(*) as cnt FROM processed_posts WHERE pipeline_version = ?",
                (pipeline_version,),
            ).fetchone()
        else:
            row = self._conn.execute(
                "SELECT COUNT(*) as cnt FROM processed_posts"
            ).fetchone()
        return row["cnt"]

    def delete_by_ids(self, post_ids: list[str]) -> int:
        """Delete processed posts by ID. Returns count deleted."""
        if not post_ids:
            return 0
        placeholders = ",".join("?" for _ in post_ids)
        with self._conn:
            cursor = self._conn.execute(
                f"DELETE FROM processed_posts WHERE id IN ({placeholders})", post_ids
            )
            return cursor.rowcount

    def delete_all(self) -> int:
        """Delete all processed posts. Used for full reprocessing."""
        with self._conn:
            cursor = self._conn.execute("DELETE FROM processed_posts")
            count = cursor.rowcount
            logger.warning("Deleted all %d processed posts", count)
            return count
