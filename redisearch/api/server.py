"""Uvicorn entrypoint for running the API server."""

from __future__ import annotations

import argparse
import logging

import uvicorn

from redisearch.config.logging_config import setup_logging
from redisearch.config.settings import get_settings


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the RediSearch API server.")
    parser.add_argument("--host", default="127.0.0.1", help="Bind address.")
    parser.add_argument("--port", type=int, default=8000, help="Bind port.")
    parser.add_argument("--reload", action="store_true", help="Enable auto-reload.")
    return parser


def main() -> int:
    args = build_parser().parse_args()

    settings = get_settings()
    setup_logging(log_dir=settings.logs_dir)
    logger = logging.getLogger(__name__)

    logger.info("Starting API server on %s:%d", args.host, args.port)
    uvicorn.run(
        "redisearch.api.app:create_app",
        factory=True,
        host=args.host,
        port=args.port,
        reload=args.reload,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
