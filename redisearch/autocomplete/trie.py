"""
Prefix trie for autocomplete suggestions.

Each node stores a character and optional completion data (term text
and aggregate score).  The trie supports weighted prefix search: given
a prefix string, it returns the top-k completions ranked by score.
"""

from __future__ import annotations

import heapq
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import msgpack

logger = logging.getLogger(__name__)


@dataclass
class TrieNode:
    """Single node in the trie."""

    children: dict[str, "TrieNode"] = field(default_factory=dict)
    is_terminal: bool = False
    term: str = ""
    score: float = 0.0


@dataclass
class Suggestion:
    """A single autocomplete suggestion."""

    term: str
    score: float


class Trie:
    """Weighted prefix trie with top-k retrieval."""

    def __init__(self) -> None:
        self._root = TrieNode()
        self._size = 0

    @property
    def size(self) -> int:
        """Number of distinct terms stored."""
        return self._size

    def insert(self, term: str, score: float = 1.0) -> None:
        """
        Insert *term* with *score*.

        If the term already exists, the score is updated to the
        maximum of the old and new values.
        """
        node = self._root
        for ch in term.lower():
            if ch not in node.children:
                node.children[ch] = TrieNode()
            node = node.children[ch]

        if not node.is_terminal:
            self._size += 1
        node.is_terminal = True
        node.term = term.lower()
        node.score = max(node.score, score)

    def search(self, prefix: str, top_k: int = 10) -> list[Suggestion]:
        """
        Return up to *top_k* suggestions whose terms start with *prefix*,
        ordered by descending score.
        """
        prefix = prefix.lower()
        node = self._root
        for ch in prefix:
            if ch not in node.children:
                return []
            node = node.children[ch]

        # Collect all terminal descendants using a heap for efficiency
        heap: list[tuple[float, str]] = []
        self._collect(node, heap, top_k)
        heap.sort(key=lambda x: x[0], reverse=True)
        return [Suggestion(term=t, score=s) for s, t in heap]

    def _collect(
        self,
        node: TrieNode,
        heap: list[tuple[float, str]],
        k: int,
    ) -> None:
        """DFS to collect top-k terminals under *node*."""
        if node.is_terminal:
            if len(heap) < k:
                heapq.heappush(heap, (node.score, node.term))
            elif node.score > heap[0][0]:
                heapq.heapreplace(heap, (node.score, node.term))

        for child in node.children.values():
            self._collect(child, heap, k)

    # ---- persistence ----

    def save(self, path: Path) -> None:
        """Serialize the trie to a msgpack file."""
        path.parent.mkdir(parents=True, exist_ok=True)
        data = self._serialize_node(self._root)
        with open(path, "wb") as f:
            msgpack.pack(data, f)
        logger.info("Saved trie (%d terms) to %s", self._size, path)

    @classmethod
    def load(cls, path: Path) -> "Trie":
        """Deserialize a trie from a msgpack file."""
        with open(path, "rb") as f:
            data = msgpack.unpack(f, raw=False)
        trie = cls()
        trie._root = trie._deserialize_node(data)
        trie._size = trie._count_terminals(trie._root)
        logger.info("Loaded trie (%d terms) from %s", trie._size, path)
        return trie

    def _serialize_node(self, node: TrieNode) -> dict:
        return {
            "t": node.is_terminal,
            "w": node.term,
            "s": node.score,
            "c": {ch: self._serialize_node(child) for ch, child in node.children.items()},
        }

    def _deserialize_node(self, data: dict) -> TrieNode:
        node = TrieNode(
            is_terminal=data["t"],
            term=data["w"],
            score=data["s"],
        )
        for ch, child_data in data["c"].items():
            node.children[ch] = self._deserialize_node(child_data)
        return node

    def _count_terminals(self, node: TrieNode) -> int:
        count = 1 if node.is_terminal else 0
        for child in node.children.values():
            count += self._count_terminals(child)
        return count
