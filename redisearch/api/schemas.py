"""Pydantic response/request models for the API."""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class SearchHitResponse(BaseModel):
    """A single search result."""

    id: str
    title: str
    subreddit: str
    permalink: str
    score: float
    author: Optional[str] = None
    comment_count: int = 0
    post_score: int = 0


class SearchResponse(BaseModel):
    """Paginated search results."""

    query: str
    hits: list[SearchHitResponse]
    total_hits: int
    page_size: int
    next_cursor: Optional[str] = None


class StatsResponse(BaseModel):
    """System statistics."""

    raw_post_count: int
    processed_post_count: int
    subreddits: list[str]
    active_indexes: int


class ErrorResponse(BaseModel):
    """Standard error envelope."""

    detail: str
