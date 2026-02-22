"""Autocomplete API route."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request

from redisearch.api.schemas import AutocompleteResponse, SuggestionResponse

router = APIRouter(tags=["autocomplete"])


@router.get("/autocomplete", response_model=AutocompleteResponse)
def autocomplete(
    request: Request,
    q: str = Query(..., min_length=1, max_length=200, description="Prefix to complete"),
    subreddit: str | None = Query(None, description="Subreddit to scope suggestions"),
    top_k: int = Query(10, ge=1, le=50, description="Max suggestions"),
) -> AutocompleteResponse:
    """Return autocomplete suggestions for a prefix."""
    rate_limiter = request.app.state.autocomplete_rate_limiter
    client_ip = request.client.host if request.client else "unknown"

    if not rate_limiter.is_allowed(client_ip):
        raise HTTPException(status_code=429, detail="Rate limit exceeded")

    suggester = request.app.state.suggester
    suggestions = suggester.suggest(q, subreddit=subreddit, top_k=top_k)

    return AutocompleteResponse(
        prefix=q,
        suggestions=[
            SuggestionResponse(term=s.term, score=round(s.score, 2))
            for s in suggestions
        ],
    )
