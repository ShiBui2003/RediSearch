"""
CRUD operations for the raw_posts table.

This is the write-once store for crawled Reddit posts. Once a post is
inserted, it is never modified. If extraction logic changes, the
raw_html blob is re-parsed — the row itself stays unchanged.

All methods accept an optional db_path for testability.
"""

from __future__ import annotations

import logging
from dataclasses import asdict
from pathlib import Path
from typing import Optional

from redisearch.storage.connection import get_connection
from redisearch.storage.models import RawPost

logger = logging.getLogger(__name__)


class RawPostStore:
    """CRUD interface for the raw_posts table."""

    def __init__(self, db_path: Optional[Path] = None) -> None:
        self._db_path = db_path

    @property
    def _conn(self):
        return get_connection(self._db_path)

    # ----- Write operations -----

    def insert(self, post: RawPost) -> bool:
        """
        Insert a single raw post. Returns True if inserted, False if
        the post already exists (dedup by primary key).
        """
        sql = """
            INSERT OR IGNORE INTO raw_posts
                (id, subreddit, permalink, title, body, author, score,
                 comment_count, created_utc, crawled_at, raw_html, post_type)
            VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        with self._conn:
            cursor = self._conn.execute(
                sql,
                (
                    post.id,
                    post.subreddit,
                    post.permalink,
                    post.title,
                    post.body,
                    post.author,
                    post.score,
                    post.comment_count,
                    post.created_utc,
                    post.crawled_at,
                    post.raw_html,
                    post.post_type,
                ),
            )
            inserted = cursor.rowcount > 0
            if inserted:
                logger.debug("Inserted raw post: %s", post.id)
            else:
                logger.debug("Skipped duplicate raw post: %s", post.id)
            return inserted

    def insert_many(self, posts: list[RawPost]) -> int:
        """
        Insert multiple raw posts. Returns the count of newly inserted posts.
        Duplicates are silently skipped.
        """
        sql = """
            INSERT OR IGNORE INTO raw_posts
                (id, subreddit, permalink, title, body, author, score,
                 comment_count, created_utc, crawled_at, raw_html, post_type)
            VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        rows = [
            (
                p.id, p.subreddit, p.permalink, p.title, p.body, p.author,
                p.score, p.comment_count, p.created_utc, p.crawled_at,
                p.raw_html, p.post_type,
            )
            for p in posts
        ]
        with self._conn:
            cursor = self._conn.executemany(sql, rows)
            count = cursor.rowcount
            logger.info("Bulk insert: %d new posts (of %d attempted)", count, len(posts))
            return count

    # ----- Read operations -----

    def _row_to_post(self, row) -> RawPost:
        """Convert a sqlite3.Row to a RawPost dataclass."""
        return RawPost(
            id=row["id"],
            subreddit=row["subreddit"],
            permalink=row["permalink"],
            title=row["title"],
            body=row["body"],
            author=row["author"],
            score=row["score"],
            comment_count=row["comment_count"],
            created_utc=row["created_utc"],
            crawled_at=row["crawled_at"],
            raw_html=row["raw_html"],
            post_type=row["post_type"],
        )

    def get_by_id(self, post_id: str) -> Optional[RawPost]:
        """Fetch a single post by its Reddit fullname (e.g., 't3_abc123')."""
        row = self._conn.execute(
            "SELECT * FROM raw_posts WHERE id = ?", (post_id,)
        ).fetchone()
        return self._row_to_post(row) if row else None

    def get_by_ids(self, post_ids: list[str]) -> list[RawPost]:
        """Fetch multiple posts by ID. Returns in arbitrary order."""
        if not post_ids:
            return []
        placeholders = ",".join("?" for _ in post_ids)
        rows = self._conn.execute(
            f"SELECT * FROM raw_posts WHERE id IN ({placeholders})", post_ids
        ).fetchall()
        return [self._row_to_post(r) for r in rows]

    def get_by_subreddit(
        self, subreddit: str, limit: int = 1000, offset: int = 0
    ) -> list[RawPost]:
        """Fetch posts for a specific subreddit, ordered by creation time desc."""
        rows = self._conn.execute(
            "SELECT * FROM raw_posts WHERE subreddit = ? ORDER BY created_utc DESC LIMIT ? OFFSET ?",
            (subreddit.lower(), limit, offset),
        ).fetchall()
        return [self._row_to_post(r) for r in rows]

    def exists(self, post_id: str) -> bool:
        """Check if a post ID already exists in the store."""
        row = self._conn.execute(
            "SELECT 1 FROM raw_posts WHERE id = ?", (post_id,)
        ).fetchone()
        return row is not None

    def get_all_ids(self) -> list[str]:
        """Return all post IDs in the store. Used for dedup Bloom filter rebuild."""
        rows = self._conn.execute("SELECT id FROM raw_posts").fetchall()
        return [r["id"] for r in rows]

    def count(self, subreddit: Optional[str] = None) -> int:
        """Count posts, optionally filtered by subreddit."""
        if subreddit:
            row = self._conn.execute(
                "SELECT COUNT(*) as cnt FROM raw_posts WHERE subreddit = ?",
                (subreddit.lower(),),
            ).fetchone()
        else:
            row = self._conn.execute("SELECT COUNT(*) as cnt FROM raw_posts").fetchone()
        return row["cnt"]

    def get_subreddits(self) -> list[str]:
        """Return a list of all distinct subreddits in the store."""
        rows = self._conn.execute(
            "SELECT DISTINCT subreddit FROM raw_posts ORDER BY subreddit"
        ).fetchall()
        return [r["subreddit"] for r in rows]

    def get_unprocessed_ids(self, current_version: int) -> list[str]:
        """
        Return IDs of raw posts that don't have a corresponding processed_post
        at the current pipeline version.

        This drives incremental preprocessing: only unprocessed or stale
        documents are sent through the pipeline.
        """
        rows = self._conn.execute(
            """
            SELECT r.id FROM raw_posts r
            LEFT JOIN processed_posts p ON r.id = p.id
            WHERE p.id IS NULL OR p.pipeline_version < ?
            """,
            (current_version,),
        ).fetchall()
        return [r["id"] for r in rows]
