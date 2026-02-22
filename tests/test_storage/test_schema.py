"""Tests for SQLite schema initialization."""

from redisearch.storage.schema import initialize_database, get_schema_version, SCHEMA_VERSION
from redisearch.storage.connection import get_connection


def test_schema_creates_all_tables(db_path):
    """Verify all four tables are created."""
    initialize_database(db_path)
    conn = get_connection(db_path)

    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    table_names = [t["name"] for t in tables]

    assert "raw_posts" in table_names
    assert "processed_posts" in table_names
    assert "index_versions" in table_names
    assert "jobs" in table_names


def test_schema_version_is_set(db_path):
    """Verify the schema version pragma is set correctly."""
    initialize_database(db_path)
    version = get_schema_version(db_path)
    assert version == SCHEMA_VERSION


def test_schema_is_idempotent(db_path):
    """Calling initialize_database twice should not error."""
    initialize_database(db_path)
    initialize_database(db_path)  # Must not raise
    version = get_schema_version(db_path)
    assert version == SCHEMA_VERSION


def test_wal_mode_enabled(db_path):
    """Verify WAL journal mode is active."""
    initialize_database(db_path)
    conn = get_connection(db_path)
    row = conn.execute("PRAGMA journal_mode").fetchone()
    assert row[0].lower() == "wal"
