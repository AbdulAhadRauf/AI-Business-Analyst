"""
API endpoint tests using FastAPI TestClient.
"""

import pytest
from fastapi.testclient import TestClient
from app.main import app


client = TestClient(app)


class TestHealthEndpoint:

    def test_health_check(self):
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert "version" in data


class TestCRMEndpoint:

    def test_get_crm_data(self):
        resp = client.get("/data/crm?limit=5")
        assert resp.status_code == 200
        data = resp.json()
        assert "data" in data
        assert "metadata" in data
        assert len(data["data"]) <= 5
        assert data["metadata"]["total_results"] > 0

    def test_filter_active(self):
        resp = client.get("/data/crm?status=active&limit=50")
        assert resp.status_code == 200
        for item in resp.json()["data"]:
            assert item["status"] == "active"

    def test_search(self):
        resp = client.get("/data/crm?search=Customer 1&limit=10")
        assert resp.status_code == 200


class TestSupportEndpoint:

    def test_get_support_data(self):
        resp = client.get("/data/support?limit=5")
        assert resp.status_code == 200
        data = resp.json()
        assert "data" in data
        assert len(data["data"]) <= 5

    def test_filter_high_priority(self):
        resp = client.get("/data/support?priority=high&limit=50")
        assert resp.status_code == 200
        for item in resp.json()["data"]:
            assert item["priority"] == "high"

    def test_filter_open(self):
        resp = client.get("/data/support?status=open&limit=50")
        assert resp.status_code == 200
        for item in resp.json()["data"]:
            assert item["status"] == "open"


class TestAnalyticsEndpoint:

    def test_get_analytics(self):
        resp = client.get("/data/analytics?days=30&limit=10")
        assert resp.status_code == 200
        data = resp.json()
        assert "data" in data

    def test_analytics_summary(self):
        resp = client.get("/data/analytics/summary?days=30")
        assert resp.status_code == 200
        data = resp.json()
        assert "data" in data
        assert "average" in data["data"]


class TestToolSchema:

    def test_tool_schema_returns_tools(self):
        resp = client.get("/tools/schema")
        assert resp.status_code == 200
        data = resp.json()
        assert "tools" in data
        assert len(data["tools"]) > 0
        for tool in data["tools"]:
            assert tool["type"] == "function"
