"""
Snowflake adapter — connects to Snowflake Data Warehouse.

Uses snowflake-connector-python with asyncio run_in_executor for
non-blocking I/O. Follows the BaseDataAdapter interface so the
adapter registry and orchestrator can auto-generate LLM tools.

Config example (business_config.yaml):
    adapters:
      snowflake_orders:
        type: snowflake
        table: ORDERS
        id_field: ORDER_ID
        search_fields: [CUSTOMER_NAME, ORDER_STATUS]
        display_name: "Snowflake Orders"
        writable: false
"""

import asyncio
import logging
from functools import partial
from typing import Any, Dict, List, Optional

from app.adapters.base_adapter import BaseDataAdapter, AdapterResult, AdapterSchema

logger = logging.getLogger(__name__)


class SnowflakeAdapter(BaseDataAdapter):
    """
    Async adapter for Snowflake Data Warehouse.

    Uses snowflake.connector under the hood, wrapped with
    run_in_executor for async compatibility.
    """

    def __init__(
        self,
        source_name: str,
        table: str,
        id_field: str,
        search_fields: List[str],
        account: str = "",
        user: str = "",
        password: str = "",
        warehouse: str = "",
        database: str = "",
        schema: str = "",
        role: str = "",
        display_name: str = "",
        writable: bool = False,
        schema_fields: Optional[List[Dict[str, str]]] = None,
    ):
        self.source_name = source_name
        self.display_name = display_name or source_name
        self.id_field = id_field
        self.search_fields = search_fields
        self.writable = writable
        self._table = table
        self._schema_fields = schema_fields

        # Snowflake connection parameters
        self._conn_params = {
            "account": account,
            "user": user,
            "password": password,
            "warehouse": warehouse,
            "database": database,
            "schema": schema,
        }
        if role:
            self._conn_params["role"] = role

        self._conn = None

    def _get_connection(self):
        """Create or return the cached Snowflake connection (sync)."""
        if self._conn is None or self._conn.is_closed():
            import snowflake.connector
            self._conn = snowflake.connector.connect(**self._conn_params)
            logger.info(
                "Connected to Snowflake: %s.%s.%s",
                self._conn_params.get("database"),
                self._conn_params.get("schema"),
                self._table,
            )
        return self._conn

    def _execute_query(self, query: str, params: tuple = ()) -> List[Dict[str, Any]]:
        """Execute a query synchronously and return rows as dicts."""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(query, params)
            if cursor.description is None:
                return []
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]
        finally:
            cursor.close()

    def _execute_one(self, query: str, params: tuple = ()) -> Optional[Dict[str, Any]]:
        """Execute a query and return a single row."""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(query, params)
            if cursor.description is None:
                return None
            columns = [desc[0] for desc in cursor.description]
            row = cursor.fetchone()
            return dict(zip(columns, row)) if row else None
        finally:
            cursor.close()

    def _execute_dml(self, query: str, params: tuple = ()) -> int:
        """Execute an INSERT/UPDATE and return affected row count."""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(query, params)
            return cursor.rowcount
        finally:
            cursor.close()

    async def _run(self, func, *args, **kwargs):
        """Run a blocking function in the default executor."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, partial(func, *args, **kwargs))

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
        where_clauses = []
        params = []

        if filters:
            for key, value in filters.items():
                if value is not None:
                    where_clauses.append(f"{key} = %s")
                    params.append(value)

        where_sql = f" WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        order_sql = f" ORDER BY {sort_by} {sort_order.upper()}" if sort_by else ""

        # Get total count
        count_query = f"SELECT COUNT(*) AS cnt FROM {self._table}{where_sql}"
        count_result = await self._run(self._execute_one, count_query, tuple(params))
        total = count_result["CNT"] if count_result else 0

        # Get paginated results
        data_query = (
            f"SELECT * FROM {self._table}{where_sql}{order_sql} "
            f"LIMIT %s OFFSET %s"
        )
        items = await self._run(
            self._execute_query, data_query, tuple(params + [limit, offset])
        )

        logger.info(
            "%s Snowflake fetch: %d total, offset=%d limit=%d (%d items)",
            self.source_name, total, offset, limit, len(items),
        )
        return AdapterResult(items=items, total=total)

    async def get_by_id(self, record_id: Any) -> Optional[Dict[str, Any]]:
        query = f"SELECT * FROM {self._table} WHERE {self.id_field} = %s"
        return await self._run(self._execute_one, query, (record_id,))

    async def search(self, query: str, *, limit: int = 10) -> AdapterResult:
        or_clauses = [f"LOWER({field}) LIKE %s" for field in self.search_fields]
        where_sql = " OR ".join(or_clauses)
        search_pattern = f"%{query.lower()}%"
        params = [search_pattern for _ in self.search_fields]

        count_query = f"SELECT COUNT(*) AS cnt FROM {self._table} WHERE {where_sql}"
        count_result = await self._run(self._execute_one, count_query, tuple(params))
        total = count_result["CNT"] if count_result else 0

        data_query = f"SELECT * FROM {self._table} WHERE {where_sql} LIMIT %s"
        items = await self._run(
            self._execute_query, data_query, tuple(params + [limit])
        )

        return AdapterResult(items=items, total=total)

    # ── Write Operations ───────────────────────────────────────────

    async def update(
        self, record_id: Any, updates: Dict[str, Any]
    ) -> Dict[str, Any]:
        if not self.writable:
            raise NotImplementedError(f"Adapter '{self.source_name}' is read-only.")

        set_clause = ", ".join(f"{k} = %s" for k in updates.keys())
        params = list(updates.values()) + [record_id]
        query = f"UPDATE {self._table} SET {set_clause} WHERE {self.id_field} = %s"

        await self._run(self._execute_dml, query, tuple(params))
        updated = await self.get_by_id(record_id)
        logger.info("Updated %s record %s: %s", self.source_name, record_id, updates)
        return updated

    async def create(self, data: Dict[str, Any]) -> Dict[str, Any]:
        if not self.writable:
            raise NotImplementedError(f"Adapter '{self.source_name}' is read-only.")

        columns = ", ".join(data.keys())
        placeholders = ", ".join("%s" for _ in data)
        query = f"INSERT INTO {self._table} ({columns}) VALUES ({placeholders})"

        await self._run(self._execute_dml, query, tuple(data.values()))
        logger.info("Created %s record in Snowflake", self.source_name)
        return data

    # ── Schema & Tool Definitions ──────────────────────────────────

    def get_schema(self) -> AdapterSchema:
        fields = self._schema_fields or []
        return AdapterSchema(
            source_name=self.source_name,
            display_name=self.display_name,
            fields=fields,
            id_field=self.id_field,
            search_fields=self.search_fields,
            writable=self.writable,
        )

    def get_tool_definitions(self) -> List[Dict[str, Any]]:
        """Auto-generate LLM tool definitions for this Snowflake source."""
        tools = []

        # Search tool
        if self.search_fields:
            tools.append({
                "type": "function",
                "function": {
                    "name": f"search_{self.source_name}",
                    "description": (
                        f"Search {self.display_name} in Snowflake by "
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
                "description": f"List {self.display_name} records from Snowflake with optional filters.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "limit": {
                            "type": "integer",
                            "description": "Results per page",
                            "default": 5,
                        },
                        "offset": {
                            "type": "integer",
                            "description": "Offset for pagination",
                            "default": 0,
                        },
                    },
                    "required": [],
                },
            },
        })

        # Get by ID
        if self.id_field:
            tools.append({
                "type": "function",
                "function": {
                    "name": f"get_{self.source_name}_by_id",
                    "description": f"Get a specific {self.display_name} record by {self.id_field} from Snowflake.",
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

        # Update (writable only)
        if self.writable and self.id_field:
            tools.append({
                "type": "function",
                "function": {
                    "name": f"update_{self.source_name}",
                    "description": (
                        f"Update a {self.display_name} record in Snowflake. "
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

    def close(self):
        """Close the Snowflake connection."""
        if self._conn and not self._conn.is_closed():
            self._conn.close()
            logger.info("Closed Snowflake connection for %s", self.source_name)
