"""Health and stats routes."""

from __future__ import annotations

from fastapi import APIRouter, Request

from redisearch.api.schemas import StatsResponse

router = APIRouter(tags=["system"])


@router.get("/health")
def health() -> dict:
    """Simple liveness check."""
    return {"status": "ok"}


@router.get("/stats", response_model=StatsResponse)
def stats(request: Request) -> StatsResponse:
    """Return high-level system statistics."""
    raw_store = request.app.state.raw_store
    processed_store = request.app.state.processed_store
    version_store = request.app.state.version_store

    return StatsResponse(
        raw_post_count=raw_store.count(),
        processed_post_count=processed_store.count(),
        subreddits=raw_store.get_subreddits(),
        active_indexes=len(version_store.get_all_active()),
    )
