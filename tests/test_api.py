"""Tests for the FastAPI pipeline trigger API."""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock
import sys
from pathlib import Path

# Ensure the project root is on sys.path
sys.path.insert(0, str(Path(__file__).parent.parent))

from api.main import app

client = TestClient(app)


class TestHealthEndpoint:
    def test_health_returns_ok(self):
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert "health" in data
        assert "timestamp" in data


class TestPipelinesEndpoint:
    def test_list_pipelines(self):
        resp = client.get("/pipelines")
        assert resp.status_code == 200
        data = resp.json()
        assert "pipelines" in data
        assert "count" in data
        assert isinstance(data["pipelines"], list)


class TestRunsEndpoint:
    def test_all_runs_returns_list(self):
        resp = client.get("/runs")
        assert resp.status_code == 200
        data = resp.json()
        assert "runs" in data
        assert isinstance(data["runs"], list)

    def test_all_runs_limit_param(self):
        resp = client.get("/runs?limit=5")
        assert resp.status_code == 200


class TestStatsEndpoint:
    def test_stats_returns_list(self):
        resp = client.get("/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert "stats" in data


class TestTriggerPipeline:
    def test_trigger_unknown_pipeline_404(self):
        resp = client.post("/pipelines/nonexistent_pipeline_xyz/run")
        assert resp.status_code == 404

    def test_pipeline_status_404_no_runs(self):
        resp = client.get("/pipelines/nonexistent_pipeline_xyz/status")
        assert resp.status_code == 404

    def test_pipeline_runs_empty(self):
        resp = client.get("/pipelines/nonexistent_xyz/runs")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 0
        assert data["runs"] == []
