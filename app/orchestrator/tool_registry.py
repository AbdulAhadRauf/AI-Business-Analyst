"""
Tool Registry — auto-generates LangChain tools from data adapters.

Reads registered adapters from the AdapterRegistry and dynamically
creates @tool-decorated functions that the LangGraph agent can call.
No manual tool writing needed — the adapter's schema drives everything.
"""

import json
import logging
from typing import Any, Dict, List

from langchain_core.tools import tool as langchain_tool

from app.adapters.adapter_registry import AdapterRegistry
from app.adapters.base_adapter import BaseDataAdapter
from app.sessions.session import Session

logger = logging.getLogger(__name__)


def _int(value: str, default: int) -> int:
    """Safe string-to-int conversion."""
    try:
        return int(value) if value and str(value).strip() else default
    except (ValueError, TypeError):
        return default


def build_tools_for_adapter(
    adapter: BaseDataAdapter,
    name: str,
) -> List:
    """
    Auto-generate LangChain @tool functions for a single adapter.

    Creates: search_{name}, get_{name}, get_{name}_by_id, update_{name}
    based on adapter configuration.
    """
    tools = []

    # ── Search Tool ────────────────────────────────────────────────
    if adapter.search_fields:
        @langchain_tool(
            name=f"search_{name}",
            description=f"Search {adapter.display_name} by {', '.join(adapter.search_fields)}.",
        )
        async def search_tool(query: str, limit: str = "5", _adapter=adapter, _name=name) -> str:
            """Search records by text query."""
            result = await _adapter.search(query, limit=_int(limit, 5))
            return json.dumps({"items": result.items, "total": result.total}, default=str)

        tools.append(search_tool)

    # ── Fetch/List Tool ────────────────────────────────────────────
    @langchain_tool(
        name=f"get_{name}",
        description=f"List {adapter.display_name} records. Supports pagination (limit, page) and filters.",
    )
    async def fetch_tool(
        limit: str = "5",
        page: str = "1",
        filters: str = "{}",
        _adapter=adapter,
        _name=name,
    ) -> str:
        """Fetch records with optional filters and pagination."""
        lim = _int(limit, 5)
        pg = _int(page, 1)
        offset = (pg - 1) * lim

        try:
            filter_dict = json.loads(filters) if filters and filters != "{}" else {}
        except json.JSONDecodeError:
            filter_dict = {}

        result = await _adapter.fetch(filters=filter_dict, limit=lim, offset=offset)
        return json.dumps({"items": result.items, "total": result.total}, default=str)

    tools.append(fetch_tool)

    # ── Get By ID Tool ─────────────────────────────────────────────
    if adapter.id_field:
        @langchain_tool(
            name=f"get_{name}_by_id",
            description=f"Get a specific {adapter.display_name} record by {adapter.id_field}.",
        )
        async def get_by_id_tool(record_id: str, _adapter=adapter, _name=name) -> str:
            """Get a single record by ID."""
            # Try as int first, then string
            try:
                rid = int(record_id)
            except (ValueError, TypeError):
                rid = record_id
            record = await _adapter.get_by_id(rid)
            if record is None:
                return json.dumps({"error": f"Record {record_id} not found in {_name}"})
            return json.dumps(record, default=str)

        tools.append(get_by_id_tool)

    # ── Update Tool (writable adapters only) ───────────────────────
    if adapter.writable and adapter.id_field:
        @langchain_tool(
            name=f"update_{name}",
            description=(
                f"Update a {adapter.display_name} record. "
                f"Specify the {adapter.id_field} and the fields to update as a JSON object."
            ),
        )
        async def update_tool(
            record_id: str,
            updates: str,
            _adapter=adapter,
            _name=name,
        ) -> str:
            """Update a record by ID. 'updates' should be a JSON string of field:value pairs."""
            try:
                update_dict = json.loads(updates)
            except json.JSONDecodeError:
                return json.dumps({"error": "Invalid JSON in updates field"})

            try:
                rid = int(record_id)
            except (ValueError, TypeError):
                rid = record_id

            try:
                result = await _adapter.update(rid, update_dict)
                return json.dumps({"success": True, "updated_record": result}, default=str)
            except NotImplementedError as e:
                return json.dumps({"error": str(e)})
            except ValueError as e:
                return json.dumps({"error": str(e)})

        tools.append(update_tool)

    return tools


def build_all_tools(registry: AdapterRegistry) -> List:
    """
    Build LangChain tools for ALL registered adapters.
    Returns a flat list of tool functions.
    """
    all_tools = []
    for name, adapter in registry.get_all_adapters().items():
        tools = build_tools_for_adapter(adapter, name)
        all_tools.extend(tools)
        logger.info("Built %d tools for adapter '%s'", len(tools), name)

    logger.info("Total tools registered: %d", len(all_tools))
    return all_tools
