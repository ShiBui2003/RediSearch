"""FastAPI application factory."""

from __future__ import annotations

from fastapi import FastAPI

from redisearch.api.rate_limiter import RateLimiter
from redisearch.api.routes.autocomplete import router as autocomplete_router
from redisearch.api.routes.health import router as health_router
from redisearch.api.routes.search import router as search_router
from redisearch.autocomplete.suggester import PrefixSuggester
from redisearch.config.settings import Settings, get_settings
from redisearch.search.bm25_searcher import BM25Searcher
from redisearch.storage.index_version_store import IndexVersionStore
from redisearch.storage.processed_store import ProcessedPostStore
from redisearch.storage.raw_store import RawPostStore
from redisearch.storage.schema import initialize_database


def create_app(settings: Settings | None = None) -> FastAPI:
    """
    Build and return a fully wired FastAPI application.

    Initializes the database, creates shared stores and searcher,
    and attaches rate limiters before mounting routes.
    """
    settings = settings or get_settings()
    initialize_database(settings.db_path)

    app = FastAPI(
        title="RediSearch API",
        version="0.1.0",
        description="BM25 search engine for Reddit posts",
    )

    # Shared state — accessible via request.app.state in routes
    app.state.settings = settings
    app.state.raw_store = RawPostStore(settings.db_path)
    app.state.processed_store = ProcessedPostStore(settings.db_path)
    app.state.version_store = IndexVersionStore(settings.db_path)
    app.state.searcher = BM25Searcher(
        version_store=app.state.version_store,
        project_root=settings.project_root,
    )

    # Autocomplete
    app.state.suggester = PrefixSuggester(
        ac_settings=settings.autocomplete,
        project_root=settings.project_root,
    )

    # Rate limiters
    rl = settings.rate_limit
    app.state.search_rate_limiter = RateLimiter(
        capacity=rl.search_bucket_capacity,
        refill_rate=rl.search_refill_rate,
        eviction_ttl=rl.eviction_ttl,
    )
    app.state.autocomplete_rate_limiter = RateLimiter(
        capacity=rl.autocomplete_bucket_capacity,
        refill_rate=rl.autocomplete_refill_rate,
        eviction_ttl=rl.eviction_ttl,
    )

    # Mount routes
    app.include_router(health_router)
    app.include_router(search_router)
    app.include_router(autocomplete_router)

    return app
