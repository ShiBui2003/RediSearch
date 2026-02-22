"""Autocomplete CLI — build trie and/or query suggestions."""

from __future__ import annotations

import argparse
import json
import logging

from redisearch.autocomplete.builder import AutocompleteBuilder
from redisearch.autocomplete.suggester import PrefixSuggester
from redisearch.config.logging_config import setup_logging
from redisearch.config.settings import get_settings


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Autocomplete tools.")
    sub = parser.add_subparsers(dest="command")

    build = sub.add_parser("build", help="Build autocomplete trie.")
    build.add_argument("--subreddit", default=None, help="Subreddit to build for (default: all).")

    query = sub.add_parser("query", help="Query autocomplete suggestions.")
    query.add_argument("prefix", help="Prefix to search.")
    query.add_argument("--subreddit", default=None, help="Subreddit trie to use.")
    query.add_argument("--top-k", type=int, default=10, help="Max suggestions.")

    return parser


def main() -> int:
    args = build_parser().parse_args()

    settings = get_settings()
    setup_logging(log_dir=settings.logs_dir)

    if args.command == "build":
        builder = AutocompleteBuilder()
        summary = builder.build(subreddit=args.subreddit)
        print(json.dumps(summary, indent=2))

    elif args.command == "query":
        suggester = PrefixSuggester()
        suggestions = suggester.suggest(args.prefix, subreddit=args.subreddit, top_k=args.top_k)
        for s in suggestions:
            print(f"  {s.score:8.1f}  {s.term}")

    else:
        build_parser().print_help()
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
