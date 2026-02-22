"""
Shard assignment manager.

Decides which shard each subreddit belongs to, based on document
counts and the ShardSettings thresholds.

Subreddits with enough documents get their own dedicated shard
(`shard_<subreddit>`).  Small subreddits are grouped together
into a shared shard (`shard_small`) so we don't create hundreds
of tiny indexes.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from redisearch.config.settings import Settings, ShardSettings, get_settings
from redisearch.storage.connection import get_connection

logger = logging.getLogger(__name__)


@dataclass
class ShardPlan:
    """
    A mapping from subreddit → shard_id.

    Also exposes reverse lookups.
    """

    assignments: dict[str, str] = field(default_factory=dict)

    def shard_for(self, subreddit: str) -> str:
        """Return the shard_id assigned to *subreddit*, falling back to 'shard_<sub>'."""
        return self.assignments.get(subreddit.strip().lower(), f"shard_{subreddit.strip().lower()}")

    def subreddits_in(self, shard_id: str) -> list[str]:
        """Return all subreddits assigned to *shard_id*."""
        return [sub for sub, sid in self.assignments.items() if sid == shard_id]

    def shard_ids(self) -> list[str]:
        """Return a list of unique shard IDs in this plan."""
        return sorted(set(self.assignments.values()))


class ShardManager:
    """
    Computes and persists shard assignments.

    The assignments are stored in a ``shard_assignments`` SQLite table
    so they survive restarts and are queryable by downstream components
    (builder, searcher, router).
    """

    def __init__(
        self,
        db_path: Optional[Path] = None,
        shard_settings: Optional[ShardSettings] = None,
    ) -> None:
        settings: Settings = get_settings()
        self._db_path = db_path or settings.db_path
        self._settings = shard_settings or settings.sharding

    @property
    def _conn(self):
        return get_connection(self._db_path)

    # ---- planning ----

    def compute_plan(self, subreddit_doc_counts: dict[str, int]) -> ShardPlan:
        """
        Given ``{subreddit: doc_count}``, decide shard assignments.

        * Subreddits with >= ``min_docs_for_own_shard`` get ``shard_<sub>``.
        * Others are grouped into ``shard_small``.
        """
        assignments: dict[str, str] = {}
        threshold = self._settings.min_docs_for_own_shard
        grouped_prefix = self._settings.grouped_shard_prefix

        for sub, count in subreddit_doc_counts.items():
            sub = sub.strip().lower()
            if count >= threshold:
                assignments[sub] = f"shard_{sub}"
            else:
                assignments[sub] = grouped_prefix

        plan = ShardPlan(assignments=assignments)
        logger.info(
            "Shard plan: %d subreddits → %d shards",
            len(assignments),
            len(plan.shard_ids()),
        )
        return plan

    # ---- persistence ----

    def save_plan(self, plan: ShardPlan) -> None:
        """Persist a ShardPlan to the ``shard_assignments`` table (upsert)."""
        with self._conn:
            self._conn.executemany(
                """
                INSERT OR REPLACE INTO shard_assignments (subreddit, shard_id)
                VALUES (?, ?)
                """,
                list(plan.assignments.items()),
            )
        logger.info("Saved shard plan (%d entries).", len(plan.assignments))

    def load_plan(self) -> ShardPlan:
        """Load the current shard plan from SQLite."""
        rows = self._conn.execute(
            "SELECT subreddit, shard_id FROM shard_assignments ORDER BY subreddit"
        ).fetchall()
        return ShardPlan(assignments={r["subreddit"]: r["shard_id"] for r in rows})

    def get_shard_id(self, subreddit: str) -> str:
        """
        Look up which shard *subreddit* belongs to.

        Falls back to ``shard_<subreddit>`` if no assignment exists.
        """
        sub = subreddit.strip().lower()
        row = self._conn.execute(
            "SELECT shard_id FROM shard_assignments WHERE subreddit = ?", (sub,)
        ).fetchone()
        return row["shard_id"] if row else f"shard_{sub}"

    def get_all_assignments(self) -> dict[str, str]:
        """Return the full subreddit → shard_id mapping."""
        rows = self._conn.execute(
            "SELECT subreddit, shard_id FROM shard_assignments ORDER BY subreddit"
        ).fetchall()
        return {r["subreddit"]: r["shard_id"] for r in rows}
