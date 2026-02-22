"""Cursor-based pagination utilities."""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from typing import Any, Optional


def encode_cursor(offset: int) -> str:
    """Encode an integer offset into an opaque cursor string."""
    return base64.urlsafe_b64encode(json.dumps({"o": offset}).encode()).decode()


def decode_cursor(cursor: str) -> int:
    """Decode cursor string back to integer offset. Returns 0 on invalid input."""
    try:
        data = json.loads(base64.urlsafe_b64decode(cursor.encode()))
        return max(0, int(data.get("o", 0)))
    except Exception:
        return 0


@dataclass
class Page:
    """A page of search results with cursor metadata."""

    items: list[Any]
    next_cursor: Optional[str]
    total_hits: int
    page_size: int

    @classmethod
    def from_results(
        cls,
        all_items: list[Any],
        offset: int,
        page_size: int,
    ) -> "Page":
        """Slice results at offset and build cursor for next page."""
        page_items = all_items[offset: offset + page_size]
        has_more = (offset + page_size) < len(all_items)
        next_cursor = encode_cursor(offset + page_size) if has_more else None

        return cls(
            items=page_items,
            next_cursor=next_cursor,
            total_hits=len(all_items),
            page_size=page_size,
        )
