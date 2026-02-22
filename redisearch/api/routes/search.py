"""Search API route."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request

from redisearch.api.pagination import Page, decode_cursor
from redisearch.api.schemas import SearchHitResponse, SearchResponse

router = APIRouter(tags=["search"])


@router.get("/search", response_model=SearchResponse)
def search(
    request: Request,
    q: str = Query(..., min_length=1, max_length=500, description="Search query"),
    subreddit: str | None = Query(None, description="Filter by subreddit"),
    cursor: str | None = Query(None, description="Pagination cursor"),
    page_size: int = Query(20, ge=1, le=100, description="Results per page"),
) -> SearchResponse:
    """Execute a BM25 search and return paginated results."""
    rate_limiter = request.app.state.search_rate_limiter
    client_ip = request.client.host if request.client else "unknown"

    if not rate_limiter.is_allowed(client_ip):
        raise HTTPException(status_code=429, detail="Rate limit exceeded")

    searcher = request.app.state.searcher
    raw_store = request.app.state.raw_store
    settings = request.app.state.settings

    top_k = settings.search.top_k_per_index
    hits = searcher.search(q, subreddit=subreddit, top_k=top_k)

    offset = decode_cursor(cursor) if cursor else 0
    page = Page.from_results(hits, offset=offset, page_size=page_size)

    post_ids = [h.id for h in page.items]
    raw_posts = {p.id: p for p in raw_store.get_by_ids(post_ids)}

    response_hits = []
    for hit in page.items:
        post = raw_posts.get(hit.id)
        response_hits.append(
            SearchHitResponse(
                id=hit.id,
                title=post.title if post else "(unavailable)",
                subreddit=post.subreddit if post else "",
                permalink=post.permalink if post else "",
                score=round(hit.score, 4),
                author=post.author if post else None,
                comment_count=post.comment_count if post else 0,
                post_score=post.score if post else 0,
            )
        )

    return SearchResponse(
        query=q,
        hits=response_hits,
        total_hits=page.total_hits,
        page_size=page.page_size,
        next_cursor=page.next_cursor,
    )
