"""
SQLite schema definitions (DDL) and migration logic.

All table creation lives here. The schema is versioned via a
`schema_version` pragma so we can add migrations later without
breaking existing databases.

Tables:
    raw_posts        — crawled posts (source of truth)
    processed_posts  — preprocessed tokens (derived, rebuildable)
    index_versions   — index file metadata and status tracking
    jobs             — background job queue
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Optional

from redisearch.storage.connection import get_connection

logger = logging.getLogger(__name__)

# Current schema version. Bump when adding migrations.
SCHEMA_VERSION = 1

# ---------------------------------------------------------------------------
# Table DDL
# ---------------------------------------------------------------------------

_RAW_POSTS_DDL = """
CREATE TABLE IF NOT EXISTS raw_posts (
    id              TEXT PRIMARY KEY,
    subreddit       TEXT NOT NULL,
    permalink       TEXT UNIQUE NOT NULL,
    title           TEXT NOT NULL,
    body            TEXT,
    author          TEXT,
    score           INTEGER DEFAULT 0,
    comment_count   INTEGER DEFAULT 0,
    created_utc     INTEGER DEFAULT 0,
    crawled_at      TEXT NOT NULL,
    raw_html        BLOB,
    post_type       TEXT DEFAULT 'self'
);
"""

_RAW_POSTS_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_raw_posts_subreddit ON raw_posts(subreddit);",
    "CREATE INDEX IF NOT EXISTS idx_raw_posts_created_utc ON raw_posts(created_utc);",
    "CREATE INDEX IF NOT EXISTS idx_raw_posts_crawled_at ON raw_posts(crawled_at);",
]

_PROCESSED_POSTS_DDL = """
CREATE TABLE IF NOT EXISTS processed_posts (
    id                TEXT PRIMARY KEY,
    title_tokens      TEXT DEFAULT '[]',
    body_tokens       TEXT DEFAULT '[]',
    all_tokens        TEXT DEFAULT '[]',
    token_count       INTEGER DEFAULT 0,
    pipeline_version  INTEGER DEFAULT 1,
    processed_at      TEXT NOT NULL,
    FOREIGN KEY (id) REFERENCES raw_posts(id) ON DELETE CASCADE
);
"""

_PROCESSED_POSTS_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_processed_posts_version ON processed_posts(pipeline_version);",
]

_INDEX_VERSIONS_DDL = """
CREATE TABLE IF NOT EXISTS index_versions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    index_type  TEXT NOT NULL,
    shard_id    TEXT NOT NULL,
    version     INTEGER NOT NULL,
    status      TEXT DEFAULT 'building',
    doc_count   INTEGER DEFAULT 0,
    file_path   TEXT NOT NULL,
    created_at  TEXT NOT NULL
);
"""

_INDEX_VERSIONS_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_index_versions_shard_type ON index_versions(shard_id, index_type);",
    "CREATE INDEX IF NOT EXISTS idx_index_versions_status ON index_versions(status);",
    # Enforce at most one active index per (index_type, shard_id)
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_index_versions_active ON index_versions(index_type, shard_id) WHERE status = 'active';",
]

_JOBS_DDL = """
CREATE TABLE IF NOT EXISTS jobs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    job_type        TEXT NOT NULL,
    status          TEXT DEFAULT 'pending',
    payload         TEXT DEFAULT '{}',
    priority        INTEGER DEFAULT 10,
    created_at      TEXT NOT NULL,
    started_at      TEXT,
    completed_at    TEXT,
    error           TEXT,
    retries         INTEGER DEFAULT 0
);
"""

_JOBS_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_jobs_dequeue ON jobs(status, priority, created_at);",
    "CREATE INDEX IF NOT EXISTS idx_jobs_type ON jobs(job_type);",
]


# ---------------------------------------------------------------------------
# Initialization
# ---------------------------------------------------------------------------


def initialize_database(db_path: Optional[Path] = None) -> None:
    """
    Create all tables and indexes if they don't exist.

    Safe to call multiple times — all statements use IF NOT EXISTS.

    Args:
        db_path: Path to SQLite file. If None, uses default from settings.
    """
    conn = get_connection(db_path)

    logger.info("Initializing database schema (version %d)...", SCHEMA_VERSION)

    with conn:
        # Create tables
        conn.execute(_RAW_POSTS_DDL)
        conn.execute(_PROCESSED_POSTS_DDL)
        conn.execute(_INDEX_VERSIONS_DDL)
        conn.execute(_JOBS_DDL)

        # Create indexes
        all_indexes = (
            _RAW_POSTS_INDEXES
            + _PROCESSED_POSTS_INDEXES
            + _INDEX_VERSIONS_INDEXES
            + _JOBS_INDEXES
        )
        for idx_sql in all_indexes:
            conn.execute(idx_sql)

        # Store schema version
        conn.execute(f"PRAGMA user_version={SCHEMA_VERSION}")

    logger.info("Database schema initialized successfully.")


def get_schema_version(db_path: Optional[Path] = None) -> int:
    """Return the current schema version of the database."""
    conn = get_connection(db_path)
    row = conn.execute("PRAGMA user_version").fetchone()
    return row[0] if row else 0
