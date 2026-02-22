"""
Autocomplete index builder.

Extracts terms from post titles, scores them by popularity
(Reddit score + optional recency boost), and builds a trie.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Optional

from redisearch.autocomplete.trie import Trie
from redisearch.config.settings import AutocompleteSettings, Settings, get_settings
from redisearch.preprocessing.pipeline import PreprocessingProfile, TextPreprocessor
from redisearch.storage.raw_store import RawPostStore

logger = logging.getLogger(__name__)

# 1 day in seconds
_DAY = 86400


class AutocompleteBuilder:
    """Build a Trie from raw post titles for prefix-based autocomplete."""

    def __init__(
        self,
        raw_store: Optional[RawPostStore] = None,
        preprocessor: Optional[TextPreprocessor] = None,
        ac_settings: Optional[AutocompleteSettings] = None,
        project_root: Optional[Path] = None,
    ) -> None:
        settings: Settings = get_settings()
        self._raw_store = raw_store or RawPostStore()
        self._preprocessor = preprocessor or TextPreprocessor(settings.preprocessing)
        self._ac = ac_settings or settings.autocomplete
        self._project_root = project_root or settings.project_root

    def build(self, subreddit: Optional[str] = None) -> dict:
        """
        Build a trie from post titles.

        * Each unique lowercased title becomes a trie entry.
        * Score = Reddit score + recency multiplier if recent.
        * Returns summary dict.
        """
        trie = Trie()
        now_utc = int(time.time())
        recency_cutoff = now_utc - (self._ac.recency_days * _DAY)

        if subreddit:
            posts = self._raw_store.get_by_subreddit(subreddit.strip().lower(), limit=100_000)
        else:
            # All subreddits — gather from each
            posts = []
            for sub in self._raw_store.get_subreddits():
                posts.extend(self._raw_store.get_by_subreddit(sub, limit=100_000))

        for post in posts:
            title = post.title.strip().lower()
            if not title:
                continue
            base_score = max(1, post.score)
            if post.created_utc >= recency_cutoff:
                base_score *= self._ac.recency_multiplier
            trie.insert(title, score=base_score)

        # Also insert individual important words (>= 3 chars) for partial matching
        for post in posts:
            words = post.title.strip().lower().split()
            for word in words:
                if len(word) >= 3:
                    trie.insert(word, score=max(1, post.score) * 0.5)

        label = subreddit or "all"
        save_path = self._project_root / "data" / "indexes" / "autocomplete" / f"{label}.msgpack"
        trie.save(save_path)

        logger.info("Built autocomplete trie for '%s': %d terms", label, trie.size)
        return {
            "subreddit": label,
            "term_count": trie.size,
            "file_path": str(save_path),
        }
