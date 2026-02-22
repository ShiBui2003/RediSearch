"""Tests for the admin API endpoints."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from redisearch.api.app import create_app
from redisearch.config.settings import Settings


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    s = Settings(project_root=tmp_path)
    s.ensure_dirs()
    return s


@pytest.fixture
def app(settings: Settings):
    application = create_app(settings)
    application.state.searcher = MagicMock()
    application.state.searcher.search.return_value = []
    return application


@pytest.fixture
def client(app) -> TestClient:
    return TestClient(app)


class TestAdminJobs:
    def test_enqueue_job(self, client: TestClient):
        resp = client.post("/admin/jobs", json={
            "job_type": "crawl",
            "payload": {"subreddit": "python"},
            "priority": 5,
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["job_id"] >= 1
        assert data["status"] == "pending"

    def test_get_job(self, client: TestClient):
        # First enqueue
        resp = client.post("/admin/jobs", json={"job_type": "test"})
        job_id = resp.json()["job_id"]

        resp = client.get(f"/admin/jobs/{job_id}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["id"] == job_id
        assert data["job_type"] == "test"
        assert data["status"] == "pending"

    def test_get_missing_job(self, client: TestClient):
        resp = client.get("/admin/jobs/99999")
        assert resp.status_code == 404

    def test_list_jobs(self, client: TestClient):
        client.post("/admin/jobs", json={"job_type": "crawl"})
        resp = client.get("/admin/jobs")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] >= 1

    def test_retry_failed_job(self, app, client: TestClient):
        # Enqueue, claim, then fail the job
        resp = client.post("/admin/jobs", json={"job_type": "test"})
        job_id = resp.json()["job_id"]

        store = app.state.scheduler.job_store
        store.claim_next()
        store.fail(job_id, "test error")

        resp = client.post(f"/admin/jobs/{job_id}/retry")
        assert resp.status_code == 200
        assert resp.json()["status"] == "pending"

    def test_retry_non_failed_job_rejected(self, client: TestClient):
        resp = client.post("/admin/jobs", json={"job_type": "test"})
        job_id = resp.json()["job_id"]

        resp = client.post(f"/admin/jobs/{job_id}/retry")
        assert resp.status_code == 400

    def test_recover_stale(self, client: TestClient):
        resp = client.post("/admin/maintenance/recover")
        assert resp.status_code == 200
        assert "recovered" in resp.json()

    def test_cleanup(self, client: TestClient):
        resp = client.post("/admin/maintenance/cleanup")
        assert resp.status_code == 200
        assert "deleted" in resp.json()
