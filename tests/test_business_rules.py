"""Tests for business rules engine."""

import pytest
from app.services.business_rules import (
    sort_by_priority,
    generate_context_string,
    freshness_label,
)


class TestSortByPriority:

    def test_sorts_high_first(self):
        data = [
            {"priority": "low"},
            {"priority": "high"},
            {"priority": "medium"},
        ]
        sorted_data = sort_by_priority(data)
        assert sorted_data[0]["priority"] == "high"
        assert sorted_data[1]["priority"] == "medium"
        assert sorted_data[2]["priority"] == "low"

    def test_no_priority_field(self):
        data = [{"name": "a"}, {"name": "b"}]
        result = sort_by_priority(data)
        assert result == data


class TestContextString:

    def test_basic_context(self):
        result = generate_context_string("CRM", 50, 10)
        assert "10 of 50" in result

    def test_with_filters(self):
        result = generate_context_string("CRM", 20, 5, filters={"status": "active"})
        assert "status=active" in result


class TestFreshnessLabel:

    def test_returns_string(self):
        label = freshness_label()
        assert "Data as of" in label
