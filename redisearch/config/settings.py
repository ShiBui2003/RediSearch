"""
Central configuration for the entire RediSearch system.

All tunables live here. Nothing is hardcoded in module code.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from functools import lru_cache


def _project_root() -> Path:
    """Walk up from this file to find the project root (where pyproject.toml lives)."""
    current = Path(__file__).resolve().parent
    while current != current.parent:
        if (current / "pyproject.toml").exists():
            return current
        current = current.parent
    # Fallback: two levels up from config/settings.py
    return Path(__file__).resolve().parent.parent.parent


@dataclass(frozen=True)
class CrawlerSettings:
    """Settings for the web crawler."""

    # Base URL for old Reddit (simple HTML, no JS)
    base_url: str = "https://old.reddit.com"

    # Minimum delay between HTTP requests (seconds)
    min_delay: float = 2.0

    # Maximum random jitter added to delay (seconds)
    max_jitter: float = 3.0

    # Maximum retries per URL before marking as failed
    max_retries: int = 3

    # Backoff base for retries (seconds). Actual wait = base * 2^attempt
    backoff_base: float = 10.0

    # Maximum backoff wait (seconds)
    max_backoff: float = 300.0

    # Maximum listing pages to crawl per subreddit
    max_pages_per_subreddit: int = 40

    # User-Agent string sent with every request
    user_agent: str = "RedditSearchBot/0.1 (educational project)"

    # Request timeout (seconds)
    request_timeout: int = 30


@dataclass(frozen=True)
class StorageSettings:
    """Settings for SQLite storage."""

    # Path to the SQLite database file (relative to project root resolved at runtime)
    db_name: str = "redisearch.db"

    # SQLite journal mode
    journal_mode: str = "WAL"

    # SQLite busy timeout (milliseconds) — how long to wait for a locked DB
    busy_timeout_ms: int = 5000


@dataclass(frozen=True)
class PreprocessingSettings:
    """Settings for the text preprocessing pipeline."""

    # Bump this when you change the preprocessing pipeline logic.
    # All documents with a lower version will be marked for reprocessing.
    pipeline_version: int = 1

    # Minimum token length to keep after tokenization
    min_token_length: int = 2

    # Maximum token length to keep (avoids garbage tokens)
    max_token_length: int = 50


@dataclass(frozen=True)
class BM25Settings:
    """BM25 scoring parameters."""

    # Term frequency saturation parameter
    k1: float = 1.2

    # Document length normalization parameter (0 = no normalization, 1 = full)
    b: float = 0.75


@dataclass(frozen=True)
class VectorSettings:
    """Settings for vector/semantic search."""

    # Sentence-transformers model name
    model_name: str = "sentence-transformers/all-MiniLM-L6-v2"

    # Embedding dimension (must match the model)
    embedding_dim: int = 384

    # Batch size for encoding documents
    encode_batch_size: int = 64

    # FAISS index type threshold: use flat index below this, IVF above
    ivf_threshold: int = 50_000


@dataclass(frozen=True)
class SearchSettings:
    """Settings for the query/search engine."""

    # Weight for BM25 in hybrid scoring (1 - alpha goes to vector)
    hybrid_alpha: float = 0.7

    # Maximum results to retrieve per index per shard before fusion
    top_k_per_index: int = 100

    # Default page size
    default_page_size: int = 20

    # Maximum allowed page size
    max_page_size: int = 100

    # Maximum query length (characters)
    max_query_length: int = 500


@dataclass(frozen=True)
class AutocompleteSettings:
    """Settings for the autocomplete/suggestion system."""

    # Maximum suggestions returned per prefix query
    max_suggestions: int = 10

    # Recency boost multiplier for posts created in the last N days
    recency_days: int = 30
    recency_multiplier: float = 1.5


@dataclass(frozen=True)
class RateLimitSettings:
    """Settings for API rate limiting (token bucket)."""

    # Search endpoint: tokens and refill rate
    search_bucket_capacity: int = 30
    search_refill_rate: float = 0.5  # tokens per second (= 30/min)

    # Autocomplete endpoint
    autocomplete_bucket_capacity: int = 120
    autocomplete_refill_rate: float = 2.0  # tokens per second (= 120/min)

    # Admin endpoints
    admin_bucket_capacity: int = 5
    admin_refill_rate: float = 0.00139  # tokens per second (≈ 5/hour)

    # Stale bucket eviction interval (seconds)
    eviction_interval: float = 300.0

    # Evict buckets not seen for this many seconds
    eviction_ttl: float = 600.0


@dataclass(frozen=True)
class JobSettings:
    """Settings for background job processing."""

    # Number of worker threads
    num_workers: int = 3

    # Polling interval for new jobs (seconds)
    poll_interval: float = 2.0

    # Maximum retries for a failed job
    max_retries: int = 3


@dataclass(frozen=True)
class ShardSettings:
    """Settings for index sharding."""

    # Minimum documents for a subreddit to get its own shard
    min_docs_for_own_shard: int = 5000

    # Name prefix for grouped small-subreddit shards
    grouped_shard_prefix: str = "shard_small"


@dataclass
class Settings:
    """
    Top-level settings container. Aggregates all subsystem settings.

    Usage:
        settings = get_settings()
        print(settings.crawler.min_delay)
        print(settings.bm25.k1)
    """

    project_root: Path = field(default_factory=_project_root)
    crawler: CrawlerSettings = field(default_factory=CrawlerSettings)
    storage: StorageSettings = field(default_factory=StorageSettings)
    preprocessing: PreprocessingSettings = field(default_factory=PreprocessingSettings)
    bm25: BM25Settings = field(default_factory=BM25Settings)
    vector: VectorSettings = field(default_factory=VectorSettings)
    search: SearchSettings = field(default_factory=SearchSettings)
    autocomplete: AutocompleteSettings = field(default_factory=AutocompleteSettings)
    rate_limit: RateLimitSettings = field(default_factory=RateLimitSettings)
    jobs: JobSettings = field(default_factory=JobSettings)
    sharding: ShardSettings = field(default_factory=ShardSettings)

    @property
    def data_dir(self) -> Path:
        """Root directory for all runtime data (DB, indexes, logs)."""
        return self.project_root / "data"

    @property
    def db_path(self) -> Path:
        """Full path to the SQLite database file."""
        return self.data_dir / "db" / self.storage.db_name

    @property
    def indexes_dir(self) -> Path:
        """Root directory for index files."""
        return self.data_dir / "indexes"

    @property
    def logs_dir(self) -> Path:
        """Root directory for log files."""
        return self.data_dir / "logs"

    def ensure_dirs(self) -> None:
        """Create all required data directories if they don't exist."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.indexes_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """
    Returns the singleton Settings instance.

    Call this instead of constructing Settings() directly so the entire
    application shares one config object.
    """
    settings = Settings()
    settings.ensure_dirs()
    return settings
