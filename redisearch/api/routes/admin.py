"""Admin API routes — job management and maintenance."""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Request

from redisearch.api.schemas import (
    ErrorResponse,
    JobResponse,
    JobEnqueueRequest,
    JobEnqueueResponse,
    JobListResponse,
)

router = APIRouter(prefix="/admin", tags=["admin"])


@router.post("/jobs", response_model=JobEnqueueResponse)
def enqueue_job(request: Request, body: JobEnqueueRequest) -> JobEnqueueResponse:
    """Enqueue a new background job."""
    rate_limiter = request.app.state.admin_rate_limiter
    client_ip = request.client.host if request.client else "unknown"

    if not rate_limiter.is_allowed(client_ip):
        raise HTTPException(status_code=429, detail="Rate limit exceeded")

    scheduler = request.app.state.scheduler
    job_id = scheduler.job_store.enqueue(
        job_type=body.job_type,
        payload=body.payload or {},
        priority=body.priority,
    )
    return JobEnqueueResponse(job_id=job_id, status="pending")


@router.get("/jobs/{job_id}", response_model=JobResponse)
def get_job(request: Request, job_id: int) -> JobResponse:
    """Get details of a specific job."""
    job_store = request.app.state.scheduler.job_store
    job = job_store.get_by_id(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    return JobResponse(
        id=job.id,
        job_type=job.job_type,
        status=job.status,
        payload=job.payload,
        priority=job.priority,
        created_at=job.created_at,
        started_at=job.started_at,
        completed_at=job.completed_at,
        error=job.error,
        retries=job.retries,
    )


@router.get("/jobs", response_model=JobListResponse)
def list_jobs(
    request: Request,
    status: Optional[str] = Query(None, description="Filter: pending, running, completed, failed"),
) -> JobListResponse:
    """List jobs filtered by status."""
    job_store = request.app.state.scheduler.job_store

    if status == "pending":
        # Use count since we don't have a generic list method
        count = job_store.get_pending_count()
        return JobListResponse(jobs=[], total=count, note="Use a specific status filter for details")
    elif status == "running":
        jobs = job_store.get_running()
    elif status == "failed":
        jobs = job_store.get_failed()
    else:
        # Summary across statuses
        pending = job_store.get_pending_count()
        running = job_store.get_running()
        failed = job_store.get_failed(limit=10)
        all_jobs = running + failed
        return JobListResponse(
            jobs=[
                JobResponse(
                    id=j.id, job_type=j.job_type, status=j.status,
                    payload=j.payload, priority=j.priority,
                    created_at=j.created_at, started_at=j.started_at,
                    completed_at=j.completed_at, error=j.error, retries=j.retries,
                )
                for j in all_jobs
            ],
            total=pending + len(running) + len(failed),
        )

    return JobListResponse(
        jobs=[
            JobResponse(
                id=j.id, job_type=j.job_type, status=j.status,
                payload=j.payload, priority=j.priority,
                created_at=j.created_at, started_at=j.started_at,
                completed_at=j.completed_at, error=j.error, retries=j.retries,
            )
            for j in jobs
        ],
        total=len(jobs),
    )


@router.post("/jobs/{job_id}/retry")
def retry_job(request: Request, job_id: int) -> dict:
    """Retry a failed job."""
    rate_limiter = request.app.state.admin_rate_limiter
    client_ip = request.client.host if request.client else "unknown"
    if not rate_limiter.is_allowed(client_ip):
        raise HTTPException(status_code=429, detail="Rate limit exceeded")

    job_store = request.app.state.scheduler.job_store
    job = job_store.get_by_id(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.status != "failed":
        raise HTTPException(status_code=400, detail="Only failed jobs can be retried")

    job_store.retry(job_id)
    return {"job_id": job_id, "status": "pending"}


@router.post("/maintenance/recover")
def recover_stale(request: Request) -> dict:
    """Recover jobs stuck in running state."""
    rate_limiter = request.app.state.admin_rate_limiter
    client_ip = request.client.host if request.client else "unknown"
    if not rate_limiter.is_allowed(client_ip):
        raise HTTPException(status_code=429, detail="Rate limit exceeded")

    scheduler = request.app.state.scheduler
    count = scheduler.recover_stale()
    return {"recovered": count}


@router.post("/maintenance/cleanup")
def cleanup_jobs(request: Request, keep_last: int = Query(100, ge=0)) -> dict:
    """Remove old completed jobs."""
    rate_limiter = request.app.state.admin_rate_limiter
    client_ip = request.client.host if request.client else "unknown"
    if not rate_limiter.is_allowed(client_ip):
        raise HTTPException(status_code=429, detail="Rate limit exceeded")

    scheduler = request.app.state.scheduler
    count = scheduler.cleanup(keep_last=keep_last)
    return {"deleted": count}
