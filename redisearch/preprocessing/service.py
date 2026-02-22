"""Preprocessing service for converting raw posts into processed posts."""

from __future__ import annotations

import json
import logging
from typing import Optional

from redisearch.config.settings import get_settings
from redisearch.preprocessing.pipeline import PreprocessingProfile, TextPreprocessor
from redisearch.storage.models import ProcessedPost
from redisearch.storage.processed_store import ProcessedPostStore
from redisearch.storage.raw_store import RawPostStore

logger = logging.getLogger(__name__)


class PreprocessingService:
    """Coordinates raw-post selection, preprocessing, and upsert."""

    def __init__(
        self,
        raw_store: Optional[RawPostStore] = None,
        processed_store: Optional[ProcessedPostStore] = None,
        preprocessor: Optional[TextPreprocessor] = None,
        pipeline_version: Optional[int] = None,
    ) -> None:
        settings = get_settings()
        self._raw_store = raw_store or RawPostStore()
        self._processed_store = processed_store or ProcessedPostStore()
        self._preprocessor = preprocessor or TextPreprocessor(settings.preprocessing)
        self._pipeline_version = pipeline_version or settings.preprocessing.pipeline_version

    def process_unprocessed(self, limit: int = 1000, subreddit: Optional[str] = None) -> dict:
        """Process unprocessed or stale posts and return a run summary."""
        candidate_ids = self._raw_store.get_unprocessed_ids(self._pipeline_version)
        if subreddit:
            subreddit_name = subreddit.strip().lower()
            candidate_posts = self._raw_store.get_by_ids(candidate_ids)
            candidate_ids = [p.id for p in candidate_posts if p.subreddit == subreddit_name]

        target_ids = candidate_ids[: max(0, limit)]
        raw_posts = self._raw_store.get_by_ids(target_ids)
        processed_posts = [self._to_processed_post(post) for post in raw_posts]

        upserted = self._processed_store.upsert_many(processed_posts) if processed_posts else 0

        summary = {
            "pipeline_version": self._pipeline_version,
            "selected": len(target_ids),
            "processed": upserted,
            "remaining": max(0, len(candidate_ids) - len(target_ids)),
        }
        if subreddit:
            summary["subreddit"] = subreddit.strip().lower()

        logger.info("Preprocessing run summary: %s", summary)
        return summary

    def _to_processed_post(self, raw_post) -> ProcessedPost:
        """Create a ProcessedPost model from a RawPost."""
        title_tokens = self._preprocessor.preprocess(
            raw_post.title,
            profile=PreprocessingProfile.DOCUMENT,
        )
        body_tokens = self._preprocessor.preprocess(
            raw_post.body,
            profile=PreprocessingProfile.DOCUMENT,
        )
        all_tokens = [*title_tokens, *body_tokens]

        return ProcessedPost(
            id=raw_post.id,
            title_tokens=json.dumps(title_tokens, ensure_ascii=False),
            body_tokens=json.dumps(body_tokens, ensure_ascii=False),
            all_tokens=json.dumps(all_tokens, ensure_ascii=False),
            token_count=len(all_tokens),
            pipeline_version=self._pipeline_version,
        )
