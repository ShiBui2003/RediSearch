"""
Prefix suggester — thin wrapper around a persisted Trie.

Loads the trie for a subreddit (or the global trie) and returns
ranked suggestions for a prefix query.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

from redisearch.autocomplete.trie import Suggestion, Trie
from redisearch.config.settings import AutocompleteSettings, Settings, get_settings

logger = logging.getLogger(__name__)


class PrefixSuggester:
    """Load and query autocomplete tries."""

    def __init__(
        self,
        ac_settings: Optional[AutocompleteSettings] = None,
        project_root: Optional[Path] = None,
    ) -> None:
        settings: Settings = get_settings()
        self._ac = ac_settings or settings.autocomplete
        self._project_root = project_root or settings.project_root
        self._cache: dict[str, Trie] = {}

    def suggest(
        self,
        prefix: str,
        subreddit: Optional[str] = None,
        top_k: Optional[int] = None,
    ) -> list[Suggestion]:
        """
        Return up to *top_k* suggestions starting with *prefix*.

        Loads the subreddit-specific trie if it exists, otherwise
        falls back to the global ``all.msgpack`` trie.
        """
        top_k = top_k or self._ac.max_suggestions
        label = subreddit.strip().lower() if subreddit else "all"
        trie = self._load_trie(label)
        if trie is None and label != "all":
            trie = self._load_trie("all")
        if trie is None:
            return []
        return trie.search(prefix, top_k=top_k)

    def _load_trie(self, label: str) -> Optional[Trie]:
        if label in self._cache:
            return self._cache[label]

        path = self._project_root / "data" / "indexes" / "autocomplete" / f"{label}.msgpack"
        if not path.exists():
            return None

        trie = Trie.load(path)
        self._cache[label] = trie
        return trie
