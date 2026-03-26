"""
JSON file adapter — backward-compatible adapter that reads from local JSON files.

This wraps the existing JSON-based data access pattern from the original
connectors so everything keeps working while we add the new adapter layer.
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.adapters.base_adapter import BaseDataAdapter, AdapterResult, AdapterSchema

logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


class JSONAdapter(BaseDataAdapter):
    """
    Reads data from a local JSON file. Supports filtering, search,
    pagination, and sorting — all in-memory.

    Good for demos and small datasets. For production, use SQLAdapter
    or build a custom adapter.
    """

    def __init__(
        self,
        source_name: str,
        source: str,
        id_field: str,
        search_fields: List[str],
        display_name: str = "",
        writable: bool = False,
        filter_fields: Optional[List[str]] = None,
    ):
        self.source_name = source_name
        self.display_name = display_name or source_name
        self.id_field = id_field
        self.search_fields = search_fields
        self.writable = writable
        self._source_path = PROJECT_ROOT / source
        self._filter_fields = filter_fields or []
        self._data: Optional[List[Dict[str, Any]]] = None

    def _load(self) -> List[Dict[str, Any]]:
        """Load and cache JSON data."""
        if self._data is None:
            with open(self._source_path, encoding="utf-8") as f:
                self._data = json.load(f)
            logger.info("Loaded %d records from %s", len(self._data), self._source_path.name)
        return self._data

    def _save(self) -> None:
        """Persist changes back to the JSON file (for writable adapters)."""
        if self._data is not None:
            with open(self._source_path, "w", encoding="utf-8") as f:
                json.dump(self._data, f, indent=2, default=str)

    # ── Read Operations ────────────────────────────────────────────

    async def fetch(
        self,
        *,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 10,
        offset: int = 0,
        sort_by: Optional[str] = None,
        sort_order: str = "desc",
    ) -> AdapterResult:
        data = list(self._load())

        # Apply filters
        if filters:
            for key, value in filters.items():
                if value is not None:
                    data = [r for r in data if r.get(key) == value]

        # Sort
        if sort_by and data and sort_by in data[0]:
            data.sort(
                key=lambda r: r.get(sort_by, ""),
                reverse=(sort_order == "desc"),
            )

        total = len(data)
        items = data[offset: offset + limit]
        logger.info(
            "%s fetch: %d total, offset=%d limit=%d (%d items)",
            self.source_name, total, offset, limit, len(items),
        )
        return AdapterResult(items=items, total=total)

    async def get_by_id(self, record_id: Any) -> Optional[Dict[str, Any]]:
        if not self.id_field:
            return None
        # Try int comparison first, then string
        for r in self._load():
            rid = r.get(self.id_field)
            if rid == record_id:
                return r
            try:
                if rid == int(record_id):
                    return r
            except (ValueError, TypeError):
                pass
        return None

    async def search(self, query: str, *, limit: int = 10) -> AdapterResult:
        q = query.lower()
        hits = [
            r for r in self._load()
            if any(q in str(r.get(f, "")).lower() for f in self.search_fields)
        ]
        return AdapterResult(items=hits[:limit], total=len(hits))

    # ── Write Operations ───────────────────────────────────────────

    async def update(
        self, record_id: Any, updates: Dict[str, Any]
    ) -> Dict[str, Any]:
        if not self.writable:
            raise NotImplementedError(
                f"Adapter '{self.source_name}' is read-only. "
                f"Set writable: true in business_config.yaml to enable writes."
            )
        record = await self.get_by_id(record_id)
        if record is None:
            raise ValueError(f"Record {record_id} not found in {self.source_name}")

        record.update(updates)
        self._save()
        logger.info("Updated %s record %s: %s", self.source_name, record_id, updates)
        return record

    async def create(self, data: Dict[str, Any]) -> Dict[str, Any]:
        if not self.writable:
            raise NotImplementedError(
                f"Adapter '{self.source_name}' is read-only."
            )
        records = self._load()
        # Auto-increment ID if id_field is set
        if self.id_field and self.id_field not in data:
            max_id = max((r.get(self.id_field, 0) for r in records), default=0)
            data[self.id_field] = max_id + 1

        records.append(data)
        self._save()
        logger.info("Created %s record: %s", self.source_name, data.get(self.id_field))
        return data

    # ── Schema & Tool Definitions ──────────────────────────────────

    def get_schema(self) -> AdapterSchema:
        records = self._load()
        if not records:
            fields = []
        else:
            sample = records[0]
            fields = [
                {"name": k, "type": type(v).__name__}
                for k, v in sample.items()
            ]
        return AdapterSchema(
            source_name=self.source_name,
            display_name=self.display_name,
            fields=fields,
            id_field=self.id_field,
            search_fields=self.search_fields,
            writable=self.writable,
        )

    def get_tool_definitions(self) -> List[Dict[str, Any]]:
        """Auto-generate LLM tool definitions based on adapter config."""
        tools = []

        # Search tool
        if self.search_fields:
            tools.append({
                "type": "function",
                "function": {
                    "name": f"search_{self.source_name}",
                    "description": (
                        f"Search {self.display_name} by "
                        f"{', '.join(self.search_fields)}."
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "query": {
                                "type": "string",
                                "description": f"Search query for {self.display_name}",
                            },
                            "limit": {
                                "type": "integer",
                                "description": "Max results",
                                "default": 5,
                            },
                        },
                        "required": ["query"],
                    },
                },
            })

        # Fetch/list tool
        tools.append({
            "type": "function",
            "function": {
                "name": f"get_{self.source_name}",
                "description": f"List {self.display_name} records with optional filters.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "limit": {
                            "type": "integer",
                            "description": "Results per page",
                            "default": 5,
                        },
                        "page": {
                            "type": "integer",
                            "description": "Page number",
                            "default": 1,
                        },
                    },
                    "required": [],
                },
            },
        })

        # Get by ID tool
        if self.id_field:
            tools.append({
                "type": "function",
                "function": {
                    "name": f"get_{self.source_name}_by_id",
                    "description": f"Get a specific {self.display_name} record by {self.id_field}.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            self.id_field: {
                                "type": "string",
                                "description": f"The {self.id_field}",
                            },
                        },
                        "required": [self.id_field],
                    },
                },
            })

        # Update tool (only if writable)
        if self.writable and self.id_field:
            tools.append({
                "type": "function",
                "function": {
                    "name": f"update_{self.source_name}",
                    "description": (
                        f"Update a {self.display_name} record. "
                        f"Specify the {self.id_field} and the fields to update."
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            self.id_field: {
                                "type": "string",
                                "description": f"The {self.id_field} of the record to update",
                            },
                            "updates": {
                                "type": "object",
                                "description": "Key-value pairs of fields to update",
                            },
                        },
                        "required": [self.id_field, "updates"],
                    },
                },
            })

        return tools
