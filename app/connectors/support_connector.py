"""Support ticket connector — filter by priority, status, customer_id."""

from typing import Any, Dict, List

from app.connectors.base import BaseConnector
from app.models.common import DataType


class SupportConnector(BaseConnector):

    source_name = "Support Tickets"
    data_type = DataType.TABULAR
    _data_file = "support_tickets.json"
    _id_field = "ticket_id"
    _search_fields = ("subject",)

    def _apply_filters(self, data, **filters):
        if filters.get("priority"):
            data = [r for r in data if r["priority"] == filters["priority"]]
        if filters.get("status"):
            data = [r for r in data if r["status"] == filters["status"]]
        if filters.get("customer_id"):
            cid = int(filters["customer_id"])
            data = [r for r in data if r["customer_id"] == cid]
        return data

    def get_tool_definitions(self) -> List[Dict[str, Any]]:
        return [
            {
                "type": "function",
                "function": {
                    "name": "get_support_tickets",
                    "description": "Retrieve support tickets. Filter by priority (high/medium/low), status (open/closed), customer_id.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "priority": {"type": "string", "enum": ["high", "medium", "low"], "description": "Filter by priority"},
                            "status": {"type": "string", "enum": ["open", "closed"], "description": "Filter by status"},
                            "customer_id": {"type": "integer", "description": "Filter by customer ID"},
                            "limit": {"type": "integer", "description": "Results per page", "default": 5},
                            "page": {"type": "integer", "description": "Page number", "default": 1},
                        },
                        "required": [],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "get_ticket_by_id",
                    "description": "Get a specific support ticket by ID.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "ticket_id": {"type": "integer", "description": "The ticket ID"},
                        },
                        "required": ["ticket_id"],
                    },
                },
            },
        ]
