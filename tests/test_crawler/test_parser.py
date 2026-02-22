"""Tests for listing-page parser behavior."""

from redisearch.crawler.parser import ListingPageParser


def test_parse_listing_extracts_posts_and_next_url():
    html = """
    <html><body>
      <div class="thing link self" data-fullname="t3_abc123" data-subreddit="python" data-permalink="/r/python/comments/abc123/first_post/" data-score="123">
        <a class="title" href="https://example.com">First Post</a>
        <p class="tagline">
          submitted <time datetime="2025-01-01T12:00:00+00:00"></time>
          by <a class="author">alice</a>
        </p>
        <a class="comments">45 comments</a>
        <div class="expando"><div class="usertext-body">Body preview text</div></div>
      </div>

      <div class="thing link" data-fullname="t3_def456" data-subreddit="python">
        <a class="title">Second Post</a>
        <a class="comments">comment</a>
      </div>

      <span class="next-button"><a href="/r/python/new/?count=25&amp;after=t3_def456">next &rsaquo;</a></span>
    </body></html>
    """

    parser = ListingPageParser()
    parsed = parser.parse(html, subreddit="python", page_url="https://old.reddit.com/r/python/new/")

    assert len(parsed.posts) == 2
    assert parsed.next_url == "https://old.reddit.com/r/python/new/?count=25&after=t3_def456"

    first = parsed.posts[0]
    assert first.id == "t3_abc123"
    assert first.subreddit == "python"
    assert first.permalink == "/r/python/comments/abc123/first_post/"
    assert first.title == "First Post"
    assert first.author == "alice"
    assert first.score == 123
    assert first.comment_count == 45
    assert first.post_type == "self"
    assert first.body == "Body preview text"
    assert first.created_utc > 0
    assert first.raw_html is not None

    second = parsed.posts[1]
    assert second.id == "t3_def456"
    assert second.permalink == ""
    assert second.author is None
    assert second.score == 0
    assert second.comment_count == 0
    assert second.created_utc == 0
    assert second.post_type == "link"
    assert second.body is None
