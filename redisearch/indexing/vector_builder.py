"""Build FAISS vector indexes from raw post text using sentence-transformers."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import numpy as np

from redisearch.config.settings import Settings, VectorSettings, get_settings
from redisearch.indexing.shard_manager import ShardManager, ShardPlan
from redisearch.indexing.vector_index import VectorIndex
from redisearch.storage.index_version_store import IndexVersionStore
from redisearch.storage.models import IndexVersion
from redisearch.storage.raw_store import RawPostStore

logger = logging.getLogger(__name__)


def _get_encoder(model_name: str):
    """Lazy-load the sentence-transformer model (heavy import)."""
    from sentence_transformers import SentenceTransformer
    return SentenceTransformer(model_name)


class VectorIndexBuilder:
    """Builds and activates FAISS vector indexes for subreddits."""

    def __init__(
        self,
        raw_store: Optional[RawPostStore] = None,
        version_store: Optional[IndexVersionStore] = None,
        shard_manager: Optional[ShardManager] = None,
        vector_settings: Optional[VectorSettings] = None,
        project_root: Optional[Path] = None,
    ) -> None:
        settings: Settings = get_settings()
        self._raw_store = raw_store or RawPostStore()
        self._version_store = version_store or IndexVersionStore()
        self._shard_manager = shard_manager or ShardManager()
        self._vs = vector_settings or settings.vector
        self._project_root = project_root or settings.project_root
        self._encoder = None  # lazy

    def _ensure_encoder(self):
        if self._encoder is None:
            self._encoder = _get_encoder(self._vs.model_name)
        return self._encoder

    def build_subreddit(self, subreddit: str) -> dict:
        """Encode posts for a subreddit and build a FAISS index."""
        sub = subreddit.strip().lower()
        shard_id = self._shard_manager.get_shard_id(sub)
        return self._build_for_subs(shard_id, [sub])

    def build_shard(self, shard_id: str, subreddits: list[str]) -> dict:
        return self._build_for_subs(shard_id, subreddits)

    def build_all(self) -> list[dict]:
        plan: ShardPlan = self._shard_manager.load_plan()
        if not plan.assignments:
            subreddits = self._raw_store.get_subreddits()
            plan = ShardPlan(assignments={s: f"shard_{s}" for s in subreddits})

        summaries: list[dict] = []
        for shard_id in plan.shard_ids():
            subs = plan.subreddits_in(shard_id)
            summary = self.build_shard(shard_id, subs)
            summary["subreddits"] = subs
            summaries.append(summary)
        return summaries

    # ---- helpers ----

    def _build_for_subs(self, shard_id: str, subreddits: list[str]) -> dict:
        doc_ids: list[str] = []
        texts: list[str] = []

        for sub in subreddits:
            for post in self._raw_store.get_by_subreddit(sub.strip().lower(), limit=100_000):
                doc_ids.append(post.id)
                texts.append(f"{post.title} {post.body or ''}")

        if not doc_ids:
            return {"shard_id": shard_id, "version": 0, "doc_count": 0, "file_path": None}

        encoder = self._ensure_encoder()
        embeddings = encoder.encode(
            texts,
            batch_size=self._vs.encode_batch_size,
            show_progress_bar=False,
            convert_to_numpy=True,
        ).astype(np.float32)

        index = VectorIndex(embedding_dim=embeddings.shape[1])
        index.build(doc_ids, embeddings)

        version = self._version_store.get_latest_version_number("vector", shard_id) + 1
        rel = Path("data") / "indexes" / "vector" / shard_id / f"v{version}"
        abs_path = self._project_root / rel
        index.save(abs_path)

        self._version_store.insert(
            IndexVersion(
                index_type="vector",
                shard_id=shard_id,
                version=version,
                status="building",
                doc_count=index.doc_count,
                file_path=str(rel).replace("\\", "/"),
            )
        )
        self._version_store.activate("vector", shard_id, version)

        logger.info("Built FAISS index %s: %d vectors (v%d)", shard_id, index.doc_count, version)
        return {
            "shard_id": shard_id,
            "version": version,
            "doc_count": index.doc_count,
            "file_path": str(rel).replace("\\", "/"),
        }
