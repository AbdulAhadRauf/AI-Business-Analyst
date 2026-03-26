"""
Abstract base class with shared load/sort/paginate logic.
Subclasses only configure: source_name, data_type, _data_file, _id_field,
_search_fields, and override _apply_filters / get_tool_definitions.
"""

import json, logging
from pathlib import Path
from typing import Any, Dict, List, Optional
from abc import ABC, abstractmethod

from app.models.common import DataType

logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


class BaseConnector(ABC):

    source_name: str
    data_type: DataType
    _data_file: str       # e.g. "customers.json"
    _id_field: str         # e.g. "customer_id"
    _search_fields: tuple  # e.g. ("name", "email")

    # ── shared helpers ──────────────────────────────────────────────
    def _load(self) -> List[Dict[str, Any]]:
        with open(PROJECT_ROOT / "data" / self._data_file) as f:
            return json.load(f)

    def _apply_filters(
        self, data: List[Dict[str, Any]], **filters: Any
    ) -> List[Dict[str, Any]]:
        """Override in subclasses if filters go beyond simple equality."""
        return data

    # ── public API with shared sort/paginate ────────────────────────
    def fetch(
        self,
        *,
        limit: int = 10,
        page: int = 1,
        sort_by: Optional[str] = None,
        sort_order: str = "desc",
        **filters: Any,
    ) -> Dict[str, Any]:
        data = self._apply_filters(self._load(), **filters)

        if sort_by and data and sort_by in data[0]:
            data.sort(key=lambda r: r.get(sort_by, ""), reverse=(sort_order == "desc"))

        total = len(data)
        start = (page - 1) * limit
        items = data[start : start + limit]
        logger.info("%s fetch: %d total, page %d (%d items)", self.source_name, total, page, len(items))
        return {"items": items, "total": total}

    def search(self, query: str, *, limit: int = 10) -> Dict[str, Any]:
        q = query.lower()
        hits = [
            r for r in self._load()
            if any(q in str(r.get(f, "")).lower() for f in self._search_fields)
        ]
        return {"items": hits[:limit], "total": len(hits)}

    def get_by_id(self, record_id: int) -> Optional[Dict[str, Any]]:
        for r in self._load():
            if r.get(self._id_field) == record_id:
                return r
        return None

    @abstractmethod
    def get_tool_definitions(self) -> List[Dict[str, Any]]:
        ...
