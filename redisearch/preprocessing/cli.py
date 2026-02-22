"""CLI entrypoint for preprocessing raw posts into processed posts."""

from __future__ import annotations

import argparse
import logging

from redisearch.config.logging_config import setup_logging
from redisearch.config.settings import get_settings
from redisearch.preprocessing.service import PreprocessingService
from redisearch.storage.processed_store import ProcessedPostStore
from redisearch.storage.schema import initialize_database


def build_parser() -> argparse.ArgumentParser:
    """Build CLI argument parser."""
    parser = argparse.ArgumentParser(description="Preprocess raw posts into processed store.")
    parser.add_argument(
        "--limit",
        type=int,
        default=1000,
        help="Maximum number of posts to process in this run.",
    )
    parser.add_argument(
        "--subreddit",
        default=None,
        help="Optional subreddit filter.",
    )
    parser.add_argument(
        "--full-rebuild",
        action="store_true",
        help="Delete all processed rows before processing.",
    )
    return parser


def main() -> int:
    """Run preprocessing CLI and print run summary."""
    args = build_parser().parse_args()

    settings = get_settings()
    setup_logging(log_dir=settings.logs_dir)
    initialize_database(settings.db_path)

    logger = logging.getLogger(__name__)

    try:
        if args.full_rebuild:
            deleted = ProcessedPostStore(settings.db_path).delete_all()
            logger.info("Full rebuild requested; deleted %d processed rows", deleted)

        service = PreprocessingService()
        summary = service.process_unprocessed(limit=args.limit, subreddit=args.subreddit)
    except Exception as exc:
        logger.exception("Preprocessing run failed: %s", exc)
        return 1

    print(
        "version={pipeline_version} selected={selected} "
        "processed={processed} remaining={remaining}".format(**summary)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
