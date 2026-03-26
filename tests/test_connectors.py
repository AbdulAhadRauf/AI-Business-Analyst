"""
Tests for data connectors – CRM, Support, and Analytics.
"""

import pytest
from app.connectors.crm_connector import CRMConnector
from app.connectors.support_connector import SupportConnector
from app.connectors.analytics_connector import AnalyticsConnector


class TestCRMConnector:

    def setup_method(self):
        self.connector = CRMConnector()

    def test_fetch_returns_items_and_total(self):
        result = self.connector.fetch(limit=5)
        assert "items" in result
        assert "total" in result
        assert len(result["items"]) <= 5
        assert result["total"] > 0

    def test_fetch_pagination(self):
        page1 = self.connector.fetch(limit=3, page=1)
        page2 = self.connector.fetch(limit=3, page=2)
        assert page1["items"] != page2["items"]

    def test_fetch_filter_by_status(self):
        result = self.connector.fetch(limit=50, status="active")
        for item in result["items"]:
            assert item["status"] == "active"

    def test_search(self):
        result = self.connector.search("Customer 1")
        assert result["total"] > 0
        for item in result["items"]:
            assert "customer 1" in item["name"].lower() or "customer 1" in item["email"].lower()

    def test_get_by_id(self):
        customer = self.connector.get_by_id(1)
        assert customer is not None
        assert customer["customer_id"] == 1

    def test_get_by_id_not_found(self):
        customer = self.connector.get_by_id(99999)
        assert customer is None

    def test_tool_definitions(self):
        tools = self.connector.get_tool_definitions()
        assert len(tools) >= 2
        for tool in tools:
            assert tool["type"] == "function"
            assert "name" in tool["function"]
            assert "parameters" in tool["function"]


class TestSupportConnector:

    def setup_method(self):
        self.connector = SupportConnector()

    def test_fetch_returns_items(self):
        result = self.connector.fetch(limit=5)
        assert "items" in result
        assert len(result["items"]) <= 5

    def test_filter_by_priority(self):
        result = self.connector.fetch(limit=50, priority="high")
        for item in result["items"]:
            assert item["priority"] == "high"

    def test_filter_by_status(self):
        result = self.connector.fetch(limit=50, status="open")
        for item in result["items"]:
            assert item["status"] == "open"

    def test_get_by_id(self):
        ticket = self.connector.get_by_id(1)
        assert ticket is not None
        assert ticket["ticket_id"] == 1


class TestAnalyticsConnector:

    def setup_method(self):
        self.connector = AnalyticsConnector()

    def test_fetch_returns_items(self):
        result = self.connector.fetch(limit=5)
        assert "items" in result
        assert len(result["items"]) <= 5

    def test_filter_by_metric(self):
        result = self.connector.fetch(limit=10, metric="daily_active_users")
        for item in result["items"]:
            assert item["metric"] == "daily_active_users"

    def test_get_summary(self):
        summary = self.connector.get_summary(metric="daily_active_users", days=30)
        assert "average" in summary
        assert "trend" in summary
        assert "min" in summary
        assert "max" in summary

    def test_tool_definitions(self):
        tools = self.connector.get_tool_definitions()
        assert len(tools) >= 2
