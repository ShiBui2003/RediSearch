"""
Structured logging configuration for the RediSearch system.

Sets up console + rotating file logging. All modules should use:
    import logging
    logger = logging.getLogger(__name__)
"""

from __future__ import annotations

import logging
import logging.handlers
import sys
from pathlib import Path


def setup_logging(
    log_dir: Path | None = None,
    level: int = logging.INFO,
    log_file: str = "redisearch.log",
) -> None:
    """
    Configure logging for the entire application.

    Args:
        log_dir: Directory for log files. If None, only console logging is set up.
        level: Minimum log level.
        log_file: Name of the log file.
    """
    root_logger = logging.getLogger("redisearch")
    root_logger.setLevel(level)

    # Prevent duplicate handlers on repeated calls
    if root_logger.handlers:
        return

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler — always enabled
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # File handler — only if log_dir is provided and writable
    if log_dir is not None:
        try:
            log_dir.mkdir(parents=True, exist_ok=True)
            file_handler = logging.handlers.RotatingFileHandler(
                filename=log_dir / log_file,
                maxBytes=10 * 1024 * 1024,  # 10 MB
                backupCount=5,
                encoding="utf-8",
            )
            file_handler.setLevel(level)
            file_handler.setFormatter(formatter)
            root_logger.addHandler(file_handler)
        except OSError as e:
            root_logger.warning("Could not set up file logging: %s", e)
