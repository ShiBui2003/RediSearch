"""
Data models for the RediSearch storage layer.

These are plain dataclasses — no ORM, no magic. They represent rows
in SQLite tables and are used as the transport format between storage
and all other modules.

Every field maps 1:1 to a database column. Optional fields are nullable
columns. No business logic lives here.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional


def _utc_now_iso() -> str:
    """ISO 8601 timestamp in UTC, used as default for created/crawled timestamps."""
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Raw Posts — source of truth, never modified after insert
# ---------------------------------------------------------------------------


@dataclass
class RawPost:
    """
    A Reddit post as crawled from old.reddit.com.

    Stored in the `raw_posts` table. Once inserted, these rows are
    immutable. If re-extraction logic changes, the `raw_html` blob
    is used to re-derive fields without re-crawling.
    """

    # Reddit fullname, e.g., "t3_abc123". Primary key.
    id: str

    # Lowercase subreddit name, e.g., "python"
    subreddit: str

    # Full permalink path, e.g., "/r/python/comments/abc123/title_slug/"
    permalink: str

    # Original post title, unmodified
    title: str

    # Self-text body in plaintext. None for link posts.
    body: Optional[str] = None

    # Author username
    author: Optional[str] = None

    # Score at time of crawl
    score: int = 0

    # Number of comments at time of crawl
    comment_count: int = 0

    # Unix timestamp when the post was created on Reddit
    created_utc: int = 0

    # ISO 8601 timestamp when we crawled this post
    crawled_at: str = field(default_factory=_utc_now_iso)

    # zlib-compressed original HTML of the post page
    raw_html: Optional[bytes] = None

    # "self" or "link"
    post_type: str = "self"


# ---------------------------------------------------------------------------
# Processed Posts — derived from raw, disposable and rebuildable
# ---------------------------------------------------------------------------


@dataclass
class ProcessedPost:
    """
    A preprocessed version of a RawPost, ready for indexing.

    Stored in the `processed_posts` table. These rows are disposable:
    they can be dropped and rebuilt from raw_posts at any time.

    Tokens are stored as JSON-encoded lists of strings.
    """

    # Same ID as the corresponding RawPost
    id: str

    # JSON array of preprocessed title tokens
    title_tokens: str = "[]"

    # JSON array of preprocessed body tokens
    body_tokens: str = "[]"

    # JSON array of combined title + body tokens
    all_tokens: str = "[]"

    # Number of tokens in all_tokens
    token_count: int = 0

    # Version of the preprocessing pipeline used
    pipeline_version: int = 1

    # ISO 8601 timestamp when processing happened
    processed_at: str = field(default_factory=_utc_now_iso)


# ---------------------------------------------------------------------------
# Index Versions — tracks which index files exist and their status
# ---------------------------------------------------------------------------


@dataclass
class IndexVersion:
    """
    Metadata about a built index. Used for zero-downtime swaps.

    Stored in the `index_versions` table. At most one index per
    (index_type, shard_id) pair may have status='active'.
    """

    # Auto-incremented primary key (None before insertion)
    id: Optional[int] = None

    # Type of index: "bm25", "tfidf", or "vector"
    index_type: str = ""

    # Shard identifier, e.g., "shard_python"
    shard_id: str = ""

    # Monotonically increasing version number
    version: int = 1

    # Status: "building", "active", or "stale"
    status: str = "building"

    # Number of documents in this index
    doc_count: int = 0

    # Relative path to index files directory
    file_path: str = ""

    # ISO 8601 timestamp when this version was created
    created_at: str = field(default_factory=_utc_now_iso)


# ---------------------------------------------------------------------------
# Jobs — background job queue
# ---------------------------------------------------------------------------


@dataclass
class Job:
    """
    A background job (crawl, preprocess, build_index, rebuild).

    Stored in the `jobs` table. Workers claim jobs atomically by
    updating status from 'pending' to 'running'.
    """

    # Auto-incremented primary key (None before insertion)
    id: Optional[int] = None

    # Job type: "crawl", "preprocess", "build_index", "rebuild"
    job_type: str = ""

    # Status: "pending", "running", "completed", "failed"
    status: str = "pending"

    # JSON blob with task-specific parameters
    payload: str = "{}"

    # Lower number = higher priority
    priority: int = 10

    # ISO 8601 timestamps
    created_at: str = field(default_factory=_utc_now_iso)
    started_at: Optional[str] = None
    completed_at: Optional[str] = None

    # Error message if status == "failed"
    error: Optional[str] = None

    # Number of retries attempted so far
    retries: int = 0
