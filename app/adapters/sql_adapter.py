"""
SQL adapter — connects to any SQL database (PostgreSQL, MySQL, SQLite).

Uses aiosqlite for SQLite (demo/dev) and can be extended to use
asyncpg (PostgreSQL) or aiomysql (MySQL) for production.

For the pizza company demo, we use SQLite as a lightweight example
of a "real" database.
"""

import logging
from typing import Any, Dict, List, Optional

from app.adapters.base_adapter import BaseDataAdapter, AdapterResult, AdapterSchema

logger = logging.getLogger(__name__)


class SQLAdapter(BaseDataAdapter):
    """
    Generic SQL adapter. Connects to SQLite, PostgreSQL, or MySQL.

    Config example (business_config.yaml):
        adapters:
          orders:
            type: sql
            connection: "sqlite:///data/pizza.db"
            table: orders
            id_field: order_id
            search_fields: [customer_name, delivery_address]
            writable: true
    """

    def __init__(
        self,
        source_name: str,
        connection: str,
        table: str,
        id_field: str,
        search_fields: List[str],
        display_name: str = "",
        writable: bool = False,
        schema_fields: Optional[List[Dict[str, str]]] = None,
    ):
        self.source_name = source_name
        self.display_name = display_name or source_name
        self.id_field = id_field
        self.search_fields = search_fields
        self.writable = writable
        self._connection_string = connection
        self._table = table
        self._schema_fields = schema_fields
        self._db = None

    async def _get_db(self):
        """Lazy-initialize the database connection."""
        if self._db is None:
            if self._connection_string.startswith("sqlite"):
                import aiosqlite
                db_path = self._connection_string.replace("sqlite:///", "")
                self._db = await aiosqlite.connect(db_path)
                self._db.row_factory = aiosqlite.Row
                logger.info("Connected to SQLite: %s", db_path)
            else:
                raise NotImplementedError(
                    f"Connection type not yet supported: {self._connection_string}. "
                    f"Supported: sqlite:///path/to/db.sqlite"
                )
        return self._db

    async def _fetch_all(self, query: str, params: tuple = ()) -> List[Dict[str, Any]]:
        """Execute a query and return all rows as dicts."""
        db = await self._get_db()
        async with db.execute(query, params) as cursor:
            columns = [desc[0] for desc in cursor.description]
            rows = await cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]

    async def _fetch_one(self, query: str, params: tuple = ()) -> Optional[Dict[str, Any]]:
        """Execute a query and return a single row as dict."""
        db = await self._get_db()
        async with db.execute(query, params) as cursor:
            columns = [desc[0] for desc in cursor.description]
            row = await cursor.fetchone()
            return dict(zip(columns, row)) if row else None

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
                    where_clauses.append(f"{key} = ?")
                    params.append(value)

        where_sql = f" WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        order_sql = f" ORDER BY {sort_by} {sort_order.upper()}" if sort_by else ""

        # Get total count
        count_query = f"SELECT COUNT(*) as cnt FROM {self._table}{where_sql}"
        count_result = await self._fetch_one(count_query, tuple(params))
        total = count_result["cnt"] if count_result else 0

        # Get paginated results
        data_query = (
            f"SELECT * FROM {self._table}{where_sql}{order_sql} "
            f"LIMIT ? OFFSET ?"
        )
        items = await self._fetch_all(data_query, tuple(params + [limit, offset]))

        logger.info(
            "%s SQL fetch: %d total, offset=%d limit=%d (%d items)",
            self.source_name, total, offset, limit, len(items),
        )
        return AdapterResult(items=items, total=total)

    async def get_by_id(self, record_id: Any) -> Optional[Dict[str, Any]]:
        query = f"SELECT * FROM {self._table} WHERE {self.id_field} = ?"
        return await self._fetch_one(query, (record_id,))

    async def search(self, query: str, *, limit: int = 10) -> AdapterResult:
        or_clauses = [f"{field} LIKE ?" for field in self.search_fields]
        where_sql = " OR ".join(or_clauses)
        params = [f"%{query}%" for _ in self.search_fields]

        count_query = f"SELECT COUNT(*) as cnt FROM {self._table} WHERE {where_sql}"
        count_result = await self._fetch_one(count_query, tuple(params))
        total = count_result["cnt"] if count_result else 0

        data_query = f"SELECT * FROM {self._table} WHERE {where_sql} LIMIT ?"
        items = await self._fetch_all(data_query, tuple(params + [limit]))

        return AdapterResult(items=items, total=total)

    # ── Write Operations ───────────────────────────────────────────

    async def update(
        self, record_id: Any, updates: Dict[str, Any]
    ) -> Dict[str, Any]:
        if not self.writable:
            raise NotImplementedError(f"Adapter '{self.source_name}' is read-only.")

        set_clause = ", ".join(f"{k} = ?" for k in updates.keys())
        params = list(updates.values()) + [record_id]
        query = f"UPDATE {self._table} SET {set_clause} WHERE {self.id_field} = ?"

        db = await self._get_db()
        await db.execute(query, tuple(params))
        await db.commit()

        updated = await self.get_by_id(record_id)
        logger.info("Updated %s record %s: %s", self.source_name, record_id, updates)
        return updated

    async def create(self, data: Dict[str, Any]) -> Dict[str, Any]:
        if not self.writable:
            raise NotImplementedError(f"Adapter '{self.source_name}' is read-only.")

        columns = ", ".join(data.keys())
        placeholders = ", ".join("?" for _ in data)
        query = f"INSERT INTO {self._table} ({columns}) VALUES ({placeholders})"

        db = await self._get_db()
        cursor = await db.execute(query, tuple(data.values()))
        await db.commit()

        # Return created record
        new_id = cursor.lastrowid
        created = await self.get_by_id(new_id)
        logger.info("Created %s record: %s", self.source_name, new_id)
        return created or data

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

        # Get by ID
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

        # Update (writable only)
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
