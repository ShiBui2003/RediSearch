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


class SuggestionResponse(BaseModel):
    """A single autocomplete suggestion."""

    term: str
    score: float


class AutocompleteResponse(BaseModel):
    """Autocomplete results."""

    prefix: str
    suggestions: list[SuggestionResponse]


class JobEnqueueRequest(BaseModel):
    """Request body for enqueuing a job."""

    job_type: str
    payload: Optional[dict] = None
    priority: int = 10


class JobEnqueueResponse(BaseModel):
    """Response after enqueuing a job."""

    job_id: int
    status: str


class JobResponse(BaseModel):
    """A single job record."""

    id: int
    job_type: str
    status: str
    payload: str
    priority: int
    created_at: str
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    error: Optional[str] = None
    retries: int = 0


class JobListResponse(BaseModel):
    """List of jobs."""

    jobs: list[JobResponse]
    total: int
    note: Optional[str] = None


class ErrorResponse(BaseModel):
    """Standard error envelope."""

    detail: str
