"""CLI entrypoint for crawling subreddit listing pages."""

from __future__ import annotations

import argparse
import logging

from redisearch.config.settings import get_settings
from redisearch.config.logging_config import setup_logging
from redisearch.crawler.crawler import SubredditCrawler
from redisearch.storage.schema import initialize_database


def build_parser() -> argparse.ArgumentParser:
    """Build CLI argument parser."""
    parser = argparse.ArgumentParser(description="Crawl old.reddit subreddit listing pages.")
    parser.add_argument("--subreddit", required=True, help="Subreddit name (without r/).")
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Maximum listing pages to crawl.",
    )
    return parser


def main() -> int:
    """Run crawler CLI and print crawl summary."""
    args = build_parser().parse_args()

    settings = get_settings()
    setup_logging(log_dir=settings.logs_dir)
    initialize_database(settings.db_path)

    logger = logging.getLogger(__name__)

    try:
        crawler = SubredditCrawler()
        summary = crawler.crawl_subreddit(args.subreddit, max_pages=args.max_pages)
    except Exception as exc:
        logger.exception("Crawler run failed: %s", exc)
        return 1

    print(
        "subreddit={subreddit} pages={pages_crawled} seen={posts_seen} "
        "inserted={posts_inserted} duplicates={duplicates}".format(**summary)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
