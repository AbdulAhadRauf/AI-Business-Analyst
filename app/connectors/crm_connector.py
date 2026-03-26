"""CRM connector — filter by status, search by name/email."""

from typing import Any, Dict, List

from app.connectors.base import BaseConnector
from app.models.common import DataType


class CRMConnector(BaseConnector):

    source_name = "CRM"
    data_type = DataType.TABULAR
    _data_file = "customers.json"
    _id_field = "customer_id"
    _search_fields = ("name", "email")

    def _apply_filters(self, data, **filters):
        status = filters.get("status")
        if status:
            data = [r for r in data if r["status"] == status]
        return data

    def get_tool_definitions(self) -> List[Dict[str, Any]]:
        return [
            {
                "type": "function",
                "function": {
                    "name": "search_customers",
                    "description": "Search CRM customers by name or email.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "query": {"type": "string", "description": "Name or email to search for"},
                            "limit": {"type": "integer", "description": "Max results", "default": 5},
                        },
                        "required": ["query"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "get_customers",
                    "description": "List CRM customers, optionally filtered by status (active/inactive).",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "status": {"type": "string", "enum": ["active", "inactive"], "description": "Filter by status"},
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
                    "name": "get_customer_by_id",
                    "description": "Get a specific customer by ID.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "customer_id": {"type": "integer", "description": "The customer ID"},
                        },
                        "required": ["customer_id"],
                    },
                },
            },
        ]
