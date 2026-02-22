"""In-memory token bucket rate limiter with automatic eviction."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field


@dataclass
class _Bucket:
    """A single token bucket for one client."""

    tokens: float
    capacity: float
    refill_rate: float
    last_refill: float = field(default_factory=time.monotonic)
    last_seen: float = field(default_factory=time.monotonic)

    def try_consume(self) -> bool:
        """Refill tokens and try to consume one. Returns True if allowed."""
        now = time.monotonic()
        elapsed = now - self.last_refill
        self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
        self.last_refill = now
        self.last_seen = now

        if self.tokens >= 1.0:
            self.tokens -= 1.0
            return True
        return False


class RateLimiter:
    """
    Per-client token bucket rate limiter.

    Each (client_key, endpoint) pair gets its own bucket.
    Stale buckets are evicted periodically to prevent memory leaks.
    """

    def __init__(
        self,
        capacity: int = 30,
        refill_rate: float = 0.5,
        eviction_ttl: float = 600.0,
    ) -> None:
        self._capacity = capacity
        self._refill_rate = refill_rate
        self._eviction_ttl = eviction_ttl
        self._buckets: dict[str, _Bucket] = {}
        self._lock = threading.Lock()

    def is_allowed(self, client_key: str) -> bool:
        """Check if request from client_key is allowed. Thread-safe."""
        with self._lock:
            bucket = self._buckets.get(client_key)
            if bucket is None:
                bucket = _Bucket(
                    tokens=float(self._capacity),
                    capacity=float(self._capacity),
                    refill_rate=self._refill_rate,
                )
                self._buckets[client_key] = bucket
            return bucket.try_consume()

    def evict_stale(self) -> int:
        """Remove buckets not seen for longer than eviction_ttl. Returns count evicted."""
        now = time.monotonic()
        with self._lock:
            stale_keys = [
                k for k, b in self._buckets.items()
                if (now - b.last_seen) > self._eviction_ttl
            ]
            for k in stale_keys:
                del self._buckets[k]
            return len(stale_keys)

    @property
    def bucket_count(self) -> int:
        """Number of active buckets (for monitoring)."""
        with self._lock:
            return len(self._buckets)
