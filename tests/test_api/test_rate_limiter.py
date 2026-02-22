"""Tests for the rate limiter."""

from __future__ import annotations

import time

from redisearch.api.rate_limiter import RateLimiter


class TestRateLimiter:
    """Rate limiter basic behaviour."""

    def test_allows_within_capacity(self):
        rl = RateLimiter(capacity=3, refill_rate=0.0)
        assert rl.is_allowed("client1")
        assert rl.is_allowed("client1")
        assert rl.is_allowed("client1")

    def test_blocks_when_exhausted(self):
        rl = RateLimiter(capacity=1, refill_rate=0.0)
        assert rl.is_allowed("client1")
        assert not rl.is_allowed("client1")

    def test_separate_clients_have_own_buckets(self):
        rl = RateLimiter(capacity=1, refill_rate=0.0)
        assert rl.is_allowed("a")
        assert rl.is_allowed("b")
        assert not rl.is_allowed("a")
        assert not rl.is_allowed("b")

    def test_refill_restores_tokens(self):
        rl = RateLimiter(capacity=1, refill_rate=1000.0)  # instant refill
        assert rl.is_allowed("c")
        time.sleep(0.01)  # allow tiny refill
        assert rl.is_allowed("c")

    def test_evict_stale_removes_old_buckets(self):
        rl = RateLimiter(capacity=5, refill_rate=1.0, eviction_ttl=0.0)
        rl.is_allowed("x")
        assert rl.bucket_count == 1
        evicted = rl.evict_stale()
        assert evicted == 1
        assert rl.bucket_count == 0

    def test_evict_stale_keeps_fresh_buckets(self):
        rl = RateLimiter(capacity=5, refill_rate=1.0, eviction_ttl=9999)
        rl.is_allowed("fresh")
        assert rl.evict_stale() == 0
        assert rl.bucket_count == 1
