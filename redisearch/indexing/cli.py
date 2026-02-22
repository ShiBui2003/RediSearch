"""CLI for building BM25 indexes from processed posts."""

from __future__ import annotations

import argparse
import logging

from redisearch.config.logging_config import setup_logging
from redisearch.config.settings import get_settings
from redisearch.indexing.bm25_builder import BM25IndexBuilder
from redisearch.storage.schema import initialize_database


def build_parser() -> argparse.ArgumentParser:
    """Build CLI argument parser."""
    parser = argparse.ArgumentParser(description="Build BM25 index from processed posts.")
    parser.add_argument(
        "--subreddit",
        default=None,
        help="Build only this subreddit (without r/). If omitted, builds all subreddits.",
    )
    return parser


def main() -> int:
    """Run BM25 index build and print summaries."""
    args = build_parser().parse_args()

    settings = get_settings()
    setup_logging(log_dir=settings.logs_dir)
    initialize_database(settings.db_path)

    logger = logging.getLogger(__name__)

    try:
        builder = BM25IndexBuilder()
        summaries = [builder.build_subreddit(args.subreddit)] if args.subreddit else builder.build_all()
    except Exception as exc:
        logger.exception("BM25 build failed: %s", exc)
        return 1

    for summary in summaries:
        print(
            "subreddit={subreddit} shard={shard_id} version={version} docs={doc_count}".format(
                **summary
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
