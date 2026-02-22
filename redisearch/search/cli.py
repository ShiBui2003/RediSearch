"""CLI for BM25 search against active indexes."""

from __future__ import annotations

import argparse
import logging

from redisearch.config.logging_config import setup_logging
from redisearch.config.settings import get_settings
from redisearch.search.bm25_searcher import BM25Searcher
from redisearch.storage.raw_store import RawPostStore
from redisearch.storage.schema import initialize_database


def build_parser() -> argparse.ArgumentParser:
    """Build CLI argument parser."""
    parser = argparse.ArgumentParser(description="Search active BM25 index.")
    parser.add_argument("--query", required=True, help="Search query text.")
    parser.add_argument("--subreddit", default=None, help="Optional subreddit scope (without r/).")
    parser.add_argument("--top-k", type=int, default=10, help="Number of hits to return.")
    return parser


def main() -> int:
    """Run BM25 query and print ranked results."""
    args = build_parser().parse_args()

    settings = get_settings()
    setup_logging(log_dir=settings.logs_dir)
    initialize_database(settings.db_path)

    logger = logging.getLogger(__name__)

    try:
        searcher = BM25Searcher()
        hits = searcher.search(args.query, subreddit=args.subreddit, top_k=args.top_k)

        raw_store = RawPostStore()
        raw_posts = {p.id: p for p in raw_store.get_by_ids([h.id for h in hits])}
    except Exception as exc:
        logger.exception("BM25 search failed: %s", exc)
        return 1

    if not hits:
        print("No results.")
        return 0

    for rank, hit in enumerate(hits, start=1):
        post = raw_posts.get(hit.id)
        title = post.title if post else "(title unavailable)"
        permalink = post.permalink if post else ""
        print(f"{rank}. score={hit.score:.4f} id={hit.id} title={title} {permalink}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
