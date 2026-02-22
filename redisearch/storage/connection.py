"""
SQLite connection factory.

Provides thread-safe connections with WAL mode enabled.
All database access in the project goes through get_connection().

Design decisions:
- WAL mode: allows concurrent reads while a write is in progress.
- check_same_thread=False: connections can be shared across threads
  (we manage thread safety via SQLite's internal locking + busy timeout).
- Row factory: rows are returned as sqlite3.Row (dict-like access).
- Foreign keys: enforced (OFF by default in SQLite).
"""

from __future__ import annotations

import logging
import sqlite3
import threading
from pathlib import Path
from typing import Optional

from redisearch.config.settings import get_settings

logger = logging.getLogger(__name__)

# Module-level lock for connection creation
_lock = threading.Lock()

# Singleton connection per database path
_connections: dict[str, sqlite3.Connection] = {}


def get_connection(db_path: Optional[Path] = None) -> sqlite3.Connection:
    """
    Get a thread-safe SQLite connection.

    Uses WAL journal mode for concurrent read/write access.
    Returns the same connection object for the same db_path
    (singleton per path).

    Args:
        db_path: Path to the SQLite database file. If None, uses
                 the default path from settings.

    Returns:
        A configured sqlite3.Connection.
    """
    if db_path is None:
        settings = get_settings()
        db_path = settings.db_path

    db_key = str(db_path)

    with _lock:
        if db_key in _connections:
            return _connections[db_key]

        # Ensure parent directory exists
        db_path.parent.mkdir(parents=True, exist_ok=True)

        logger.info("Opening SQLite database: %s", db_path)

        conn = sqlite3.connect(
            str(db_path),
            check_same_thread=False,
            timeout=10.0,
        )

        # Configure for performance and correctness
        settings = get_settings()
        conn.execute(f"PRAGMA journal_mode={settings.storage.journal_mode}")
        conn.execute(f"PRAGMA busy_timeout={settings.storage.busy_timeout_ms}")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA synchronous=NORMAL")  # Safe with WAL

        # Return rows as sqlite3.Row for dict-like access
        conn.row_factory = sqlite3.Row

        _connections[db_key] = conn
        logger.info("Database connection established (WAL mode)")

        return conn


def close_connection(db_path: Optional[Path] = None) -> None:
    """
    Close the connection for a given db_path (or the default).

    Useful in tests and shutdown hooks.
    """
    if db_path is None:
        settings = get_settings()
        db_path = settings.db_path

    db_key = str(db_path)

    with _lock:
        conn = _connections.pop(db_key, None)
        if conn is not None:
            conn.close()
            logger.info("Database connection closed: %s", db_path)


def close_all_connections() -> None:
    """Close all open connections. Used during shutdown."""
    with _lock:
        for key, conn in list(_connections.items()):
            conn.close()
            logger.info("Database connection closed: %s", key)
        _connections.clear()
