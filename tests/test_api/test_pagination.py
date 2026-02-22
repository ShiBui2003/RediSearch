"""Tests for cursor-based pagination."""

from __future__ import annotations

from redisearch.api.pagination import Page, decode_cursor, encode_cursor


class TestCursorEncoding:
    def test_roundtrip(self):
        cursor = encode_cursor(42)
        assert decode_cursor(cursor) == 42

    def test_invalid_cursor_returns_zero(self):
        assert decode_cursor("garbage") == 0

    def test_empty_cursor_returns_zero(self):
        assert decode_cursor("") == 0


class TestPage:
    def test_first_page(self):
        items = list(range(50))
        page = Page.from_results(items, offset=0, page_size=20)
        assert len(page.items) == 20
        assert page.items == list(range(20))
        assert page.total_hits == 50
        assert page.next_cursor is not None

    def test_last_page_has_no_cursor(self):
        items = list(range(5))
        page = Page.from_results(items, offset=0, page_size=20)
        assert len(page.items) == 5
        assert page.next_cursor is None

    def test_exact_boundary(self):
        items = list(range(20))
        page = Page.from_results(items, offset=0, page_size=20)
        assert page.next_cursor is None
        assert len(page.items) == 20

    def test_second_page_via_cursor(self):
        items = list(range(30))
        page1 = Page.from_results(items, offset=0, page_size=10)
        assert page1.next_cursor is not None
        offset2 = decode_cursor(page1.next_cursor)
        page2 = Page.from_results(items, offset=offset2, page_size=10)
        assert page2.items == list(range(10, 20))
        assert page2.next_cursor is not None
