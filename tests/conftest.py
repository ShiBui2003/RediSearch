"""
Shared test fixtures for the RediSearch test suite.

Provides an isolated in-memory-like SQLite database per test via
a temporary file. Every test gets a clean database with the schema
already initialized.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from redisearch.storage.connection import get_connection, close_connection
from redisearch.storage.schema import initialize_database
from redisearch.storage.models import RawPost, ProcessedPost
from redisearch.storage.raw_store import RawPostStore
from redisearch.storage.processed_store import ProcessedPostStore
from redisearch.storage.index_version_store import IndexVersionStore
from redisearch.storage.job_store import JobStore


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    """
    Provide a temporary SQLite database path.

    Uses pytest's tmp_path fixture for automatic cleanup.
    """
    return tmp_path / "test_redisearch.db"


@pytest.fixture
def db(db_path: Path):
    """
    Provide an initialized database connection.

    Creates all tables, yields the connection, then cleans up.
    """
    initialize_database(db_path)
    conn = get_connection(db_path)
    yield conn
    close_connection(db_path)


@pytest.fixture
def raw_store(db, db_path: Path) -> RawPostStore:
    """Provide a RawPostStore connected to the test database."""
    return RawPostStore(db_path)


@pytest.fixture
def processed_store(db, db_path: Path) -> ProcessedPostStore:
    """Provide a ProcessedPostStore connected to the test database."""
    return ProcessedPostStore(db_path)


@pytest.fixture
def index_version_store(db, db_path: Path) -> IndexVersionStore:
    """Provide an IndexVersionStore connected to the test database."""
    return IndexVersionStore(db_path)


@pytest.fixture
def job_store(db, db_path: Path) -> JobStore:
    """Provide a JobStore connected to the test database."""
    return JobStore(db_path)


# ---------------------------------------------------------------------------
# Sample data factories
# ---------------------------------------------------------------------------


def make_raw_post(
    post_id: str = "t3_test001",
    subreddit: str = "python",
    title: str = "Test Post Title",
    body: str = "This is the body of a test post.",
    **kwargs,
) -> RawPost:
    """Create a RawPost with sensible defaults. Override any field via kwargs."""
    defaults = dict(
        id=post_id,
        subreddit=subreddit,
        permalink=f"/r/{subreddit}/comments/{post_id[3:]}/test_post/",
        title=title,
        body=body,
        author="testuser",
        score=42,
        comment_count=10,
        created_utc=1700000000,
        crawled_at="2025-01-01T00:00:00+00:00",
        raw_html=b"<html>test</html>",
        post_type="self",
    )
    defaults.update(kwargs)
    return RawPost(**defaults)


def make_processed_post(
    post_id: str = "t3_test001",
    title_tokens: str = '["test", "post", "titl"]',
    body_tokens: str = '["bodi", "test", "post"]',
    all_tokens: str = '["test", "post", "titl", "bodi", "test", "post"]',
    pipeline_version: int = 1,
    **kwargs,
) -> ProcessedPost:
    """Create a ProcessedPost with sensible defaults."""
    defaults = dict(
        id=post_id,
        title_tokens=title_tokens,
        body_tokens=body_tokens,
        all_tokens=all_tokens,
        token_count=6,
        pipeline_version=pipeline_version,
        processed_at="2025-01-01T00:00:01+00:00",
    )
    defaults.update(kwargs)
    return ProcessedPost(**defaults)
