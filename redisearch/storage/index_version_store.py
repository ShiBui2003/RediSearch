"""
CRUD operations for the index_versions table.

Tracks built index files and their statuses (building, active, stale).
Enables zero-downtime index swaps: build a new version, atomically
switch the active pointer, then clean up the old one.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

from redisearch.storage.connection import get_connection
from redisearch.storage.models import IndexVersion

logger = logging.getLogger(__name__)


class IndexVersionStore:
    """CRUD interface for the index_versions table."""

    def __init__(self, db_path: Optional[Path] = None) -> None:
        self._db_path = db_path

    @property
    def _conn(self):
        return get_connection(self._db_path)

    def _row_to_version(self, row) -> IndexVersion:
        return IndexVersion(
            id=row["id"],
            index_type=row["index_type"],
            shard_id=row["shard_id"],
            version=row["version"],
            status=row["status"],
            doc_count=row["doc_count"],
            file_path=row["file_path"],
            created_at=row["created_at"],
        )

    def insert(self, version: IndexVersion) -> int:
        """Insert a new index version. Returns the auto-generated ID."""
        sql = """
            INSERT INTO index_versions
                (index_type, shard_id, version, status, doc_count, file_path, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        with self._conn:
            cursor = self._conn.execute(
                sql,
                (
                    version.index_type,
                    version.shard_id,
                    version.version,
                    version.status,
                    version.doc_count,
                    version.file_path,
                    version.created_at,
                ),
            )
            row_id = cursor.lastrowid
            logger.info(
                "Created index version: %s/%s v%d (status=%s)",
                version.shard_id, version.index_type, version.version, version.status,
            )
            return row_id

    def get_active(self, index_type: str, shard_id: str) -> Optional[IndexVersion]:
        """Get the currently active index for a given type and shard."""
        row = self._conn.execute(
            "SELECT * FROM index_versions WHERE index_type = ? AND shard_id = ? AND status = 'active'",
            (index_type, shard_id),
        ).fetchone()
        return self._row_to_version(row) if row else None

    def get_latest_version_number(self, index_type: str, shard_id: str) -> int:
        """Get the highest version number for a type/shard combo. Returns 0 if none exist."""
        row = self._conn.execute(
            "SELECT MAX(version) as max_ver FROM index_versions WHERE index_type = ? AND shard_id = ?",
            (index_type, shard_id),
        ).fetchone()
        return row["max_ver"] or 0 if row else 0

    def activate(self, index_type: str, shard_id: str, version: int) -> None:
        """
        Atomically swap the active index for a type/shard pair.

        Sets the specified version to 'active' and all others to 'stale'.
        This MUST be done in a single transaction to avoid a window where
        no index is active.
        """
        with self._conn:
            # Mark all existing as stale
            self._conn.execute(
                "UPDATE index_versions SET status = 'stale' WHERE index_type = ? AND shard_id = ? AND status = 'active'",
                (index_type, shard_id),
            )
            # Activate the new version
            self._conn.execute(
                "UPDATE index_versions SET status = 'active' WHERE index_type = ? AND shard_id = ? AND version = ?",
                (index_type, shard_id, version),
            )
        logger.info(
            "Activated index: %s/%s v%d", shard_id, index_type, version,
        )

    def get_stale(self) -> list[IndexVersion]:
        """Get all stale index versions (candidates for cleanup)."""
        rows = self._conn.execute(
            "SELECT * FROM index_versions WHERE status = 'stale'"
        ).fetchall()
        return [self._row_to_version(r) for r in rows]

    def delete(self, version_id: int) -> None:
        """Delete an index version record by ID."""
        with self._conn:
            self._conn.execute("DELETE FROM index_versions WHERE id = ?", (version_id,))

    def get_all_active(self) -> list[IndexVersion]:
        """Get all currently active indexes across all shards and types."""
        rows = self._conn.execute(
            "SELECT * FROM index_versions WHERE status = 'active' ORDER BY shard_id, index_type"
        ).fetchall()
        return [self._row_to_version(r) for r in rows]
