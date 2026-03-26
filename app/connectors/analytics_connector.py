"""Analytics connector — filter by metric name and date range, plus summary aggregation."""

from datetime import datetime, timedelta, timezone
from statistics import mean
from typing import Any, Dict, List, Optional

from app.connectors.base import BaseConnector
from app.models.common import DataType


class AnalyticsConnector(BaseConnector):

    source_name = "Analytics"
    data_type = DataType.TIME_SERIES
    _data_file = "analytics.json"
    _id_field = ""  # no single-record ID
    _search_fields = ("metric",)

    def _apply_filters(self, data, **filters):
        metric = filters.get("metric")
        if metric:
            data = [r for r in data if r["metric"] == metric]
        days = filters.get("days")
        if days:
            cutoff = (datetime.now(timezone.utc) - timedelta(days=int(days))).date().isoformat()
            data = [r for r in data if r["date"] >= cutoff]
        return data

    def get_by_id(self, record_id: int):
        return None  # analytics has no single-record IDs

    def get_summary(self, *, metric: Optional[str] = None, days: int = 7) -> Dict[str, Any]:
        items = self.fetch(limit=999, metric=metric, days=days)["items"]
        if not items:
            return {"summary": "No data available for the requested period."}

        values = [r["value"] for r in items]
        mid = len(values) // 2
        first_avg = mean(values[:mid]) if mid else 0
        second_avg = mean(values[mid:]) if values[mid:] else 0
        trend = "up" if second_avg > first_avg else "down" if second_avg < first_avg else "flat"

        return {
            "metric": metric or "all",
            "period_days": days,
            "data_points": len(values),
            "average": round(mean(values), 1),
            "min": min(values),
            "max": max(values),
            "latest_value": items[0].get("value"),
            "latest_date": items[0].get("date"),
            "trend": trend,
        }

    def get_tool_definitions(self) -> List[Dict[str, Any]]:
        return [
            {
                "type": "function",
                "function": {
                    "name": "get_analytics",
                    "description": "Retrieve analytics metrics. Filter by metric name and time range (days).",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "metric": {"type": "string", "description": "Metric name, e.g. 'daily_active_users'"},
                            "days": {"type": "integer", "description": "Days to look back", "default": 7},
                            "limit": {"type": "integer", "description": "Max data points", "default": 10},
                        },
                        "required": [],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "get_analytics_summary",
                    "description": "Summarized analytics: average, min, max, trend. Best for voice.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "metric": {"type": "string", "description": "Metric name"},
                            "days": {"type": "integer", "description": "Days to look back", "default": 7},
                        },
                        "required": [],
                    },
                },
            },
        ]
