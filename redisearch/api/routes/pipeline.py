"""
Pipeline API route — trigger crawl → preprocess → build index in one call.

Runs synchronously for small crawls. For production use, these should
be background jobs, but for the frontend demo we want immediate feedback.
"""

from __future__ import annotations

import logging
import time
import threading
from typing import Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from redisearch.autocomplete.builder import AutocompleteBuilder
from redisearch.crawler.crawler import SubredditCrawler
from redisearch.indexing.bm25_builder import BM25IndexBuilder
from redisearch.preprocessing.service import PreprocessingService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["pipeline"])

# Track pipeline status per subreddit
_pipeline_status: dict[str, dict] = {}
_pipeline_lock = threading.Lock()


class PipelineRequest(BaseModel):
    subreddit: str = Field(..., min_length=1, max_length=50)
    max_pages: int = Field(2, ge=1, le=20)


class PipelineStatus(BaseModel):
    subreddit: str
    status: str  # "running", "completed", "failed"
    stage: str   # "crawl", "preprocess", "index", "autocomplete", "done"
    detail: Optional[str] = None
    crawl_result: Optional[dict] = None
    preprocess_result: Optional[dict] = None
    index_result: Optional[dict] = None
    elapsed_seconds: Optional[float] = None


def _run_pipeline(subreddit: str, max_pages: int, db_path, settings) -> None:
    """Run the full pipeline in a background thread."""
    start = time.time()
    sub = subreddit.strip().lower()

    try:
        # Stage 1: Crawl
        with _pipeline_lock:
            _pipeline_status[sub] = {
                "subreddit": sub, "status": "running", "stage": "crawl",
                "detail": f"Crawling r/{sub} ({max_pages} pages)...",
            }

        from redisearch.storage.raw_store import RawPostStore
        raw_store = RawPostStore(db_path)
        crawler = SubredditCrawler(raw_store=raw_store)
        crawl_result = crawler.crawl_subreddit(sub, max_pages=max_pages)

        with _pipeline_lock:
            _pipeline_status[sub]["crawl_result"] = crawl_result
            _pipeline_status[sub]["stage"] = "preprocess"
            _pipeline_status[sub]["detail"] = "Preprocessing posts..."

        # Stage 2: Preprocess
        service = PreprocessingService(raw_store=raw_store)
        preprocess_result = service.process_unprocessed(limit=10000, subreddit=sub)

        with _pipeline_lock:
            _pipeline_status[sub]["preprocess_result"] = preprocess_result
            _pipeline_status[sub]["stage"] = "index"
            _pipeline_status[sub]["detail"] = "Building BM25 index..."

        # Stage 3: Build index
        builder = BM25IndexBuilder()
        index_summaries = [builder.build_subreddit(sub)]

        with _pipeline_lock:
            _pipeline_status[sub]["index_result"] = index_summaries[0] if index_summaries else {}
            _pipeline_status[sub]["stage"] = "autocomplete"
            _pipeline_status[sub]["detail"] = "Building autocomplete trie..."

        # Stage 4: Build autocomplete
        try:
            ac_builder = AutocompleteBuilder(raw_store=raw_store)
            ac_builder.build(subreddit=sub)
            ac_builder.build()  # rebuild global trie too
        except Exception as e:
            logger.warning("Autocomplete build failed (non-fatal): %s", e)

        elapsed = round(time.time() - start, 2)
        with _pipeline_lock:
            _pipeline_status[sub]["status"] = "completed"
            _pipeline_status[sub]["stage"] = "done"
            _pipeline_status[sub]["detail"] = f"Pipeline completed in {elapsed}s"
            _pipeline_status[sub]["elapsed_seconds"] = elapsed

    except Exception as exc:
        elapsed = round(time.time() - start, 2)
        logger.exception("Pipeline failed for r/%s: %s", sub, exc)
        with _pipeline_lock:
            _pipeline_status[sub]["status"] = "failed"
            _pipeline_status[sub]["detail"] = f"{type(exc).__name__}: {exc}"
            _pipeline_status[sub]["elapsed_seconds"] = elapsed


@router.post("/pipeline/run")
def run_pipeline(request: Request, body: PipelineRequest) -> dict:
    """Trigger the full crawl → preprocess → index pipeline for a subreddit."""
    sub = body.subreddit.strip().lower()
    settings = request.app.state.settings

    # Check if already running
    with _pipeline_lock:
        current = _pipeline_status.get(sub)
        if current and current.get("status") == "running":
            return {"message": f"Pipeline for r/{sub} is already running", "status": current}

    # Launch in background thread
    t = threading.Thread(
        target=_run_pipeline,
        args=(sub, body.max_pages, settings.db_path, settings),
        daemon=True,
    )
    t.start()

    with _pipeline_lock:
        return {"message": f"Pipeline started for r/{sub}", "status": _pipeline_status.get(sub, {})}


@router.get("/pipeline/status/{subreddit}")
def pipeline_status(subreddit: str) -> dict:
    """Check pipeline status for a subreddit."""
    sub = subreddit.strip().lower()
    with _pipeline_lock:
        status = _pipeline_status.get(sub)
    if status is None:
        return {"subreddit": sub, "status": "not_started", "stage": "none", "detail": "No pipeline has been run for this subreddit"}
    return status


@router.get("/pipeline/status")
def all_pipeline_status() -> dict:
    """Get status of all pipelines."""
    with _pipeline_lock:
        return {"pipelines": dict(_pipeline_status)}
