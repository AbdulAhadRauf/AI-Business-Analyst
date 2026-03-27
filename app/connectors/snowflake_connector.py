"""
Snowflake connector — sync connector for the data_connector_agent.

Overrides BaseConnector._load() to fetch data from a Snowflake table
instead of a local JSON file. Results are cached in memory with a
configurable TTL to avoid hammering Snowflake on every voice turn.

Usage:
    from app.connectors.snowflake_connector import SnowflakeConnector

    sf = SnowflakeConnector(
        table="ORDERS",
        id_field="ORDER_ID",
        search_fields=("CUSTOMER_NAME", "ORDER_STATUS"),
    )
    result = sf.fetch(limit=5)
"""

import logging
import time
from typing import Any, Dict, List, Optional, Tuple

from app.connectors.base import BaseConnector
from app.models.common import DataType

logger = logging.getLogger(__name__)


class SnowflakeConnector(BaseConnector):
    """
    Sync Snowflake connector for use in data_connector_agent tools.

    Inherits filter/search/paginate logic from BaseConnector.
    Overrides _load() to query Snowflake instead of reading JSON.
    """

    source_name = "Snowflake"
    data_type = DataType.TABULAR
    _data_file = ""          # Not used — overridden by _load()
    _id_field = "id"
    _search_fields: Tuple[str, ...] = ()

    def __init__(
        self,
        table: str,
        id_field: str = "id",
        search_fields: Tuple[str, ...] = (),
        account: str = "",
        user: str = "",
        password: str = "",
        warehouse: str = "",
        database: str = "",
        schema: str = "",
        role: str = "",
        cache_ttl: int = 300,
        source_name: str = "Snowflake",
    ):
        self._table = table
        self._id_field = id_field
        self._search_fields = search_fields
        self.source_name = source_name

        self._conn_params: Dict[str, str] = {
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
        self._cache: Optional[List[Dict[str, Any]]] = None
        self._cache_time: float = 0
        self._cache_ttl = cache_ttl

    def _get_connection(self):
        """Create or return the cached Snowflake connection."""
        if self._conn is None or self._conn.is_closed():
            import snowflake.connector
            self._conn = snowflake.connector.connect(**self._conn_params)
            logger.info(
                "Snowflake connector: connected to %s.%s table %s",
                self._conn_params.get("database"),
                self._conn_params.get("schema"),
                self._table,
            )
        return self._conn

    def _load(self) -> List[Dict[str, Any]]:
        """
        Fetch all rows from the Snowflake table.

        Results are cached for self._cache_ttl seconds to avoid
        repeated Snowflake queries during a voice conversation.
        """
        now = time.time()
        if self._cache is not None and (now - self._cache_time) < self._cache_ttl:
            logger.debug("Snowflake cache hit for %s (age %.1fs)", self._table, now - self._cache_time)
            return self._cache

        logger.info("Snowflake cache miss — querying table %s", self._table)
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(f"SELECT * FROM {self._table}")
            if cursor.description is None:
                self._cache = []
            else:
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                self._cache = [dict(zip(columns, row)) for row in rows]
        finally:
            cursor.close()

        self._cache_time = time.time()
        logger.info(
            "Snowflake loaded %d rows from %s (cached for %ds)",
            len(self._cache), self._table, self._cache_ttl,
        )
        return self._cache

    def _apply_filters(
        self, data: List[Dict[str, Any]], **filters: Any
    ) -> List[Dict[str, Any]]:
        """Apply key=value equality filters."""
        for key, value in filters.items():
            if value is not None:
                data = [r for r in data if r.get(key) == value]
        return data

    def invalidate_cache(self):
        """Force a fresh query on next _load() call."""
        self._cache = None
        self._cache_time = 0
        logger.info("Snowflake cache invalidated for %s", self._table)

    def execute_sql(self, sql: str, params: Optional[Tuple] = None) -> List[Dict[str, Any]]:
        """
        Execute raw SQL directly against Snowflake (used by the intent LLM).
        """
        logger.info("Snowflake executing raw SQL: %s with params %s", sql, params)
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
                
            if cursor.description is None:
                return []
                
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]
        except Exception as e:
            logger.error("Snowflake SQL execution failed: %s", e)
            return [{"error": str(e)}]
        finally:
            cursor.close()

    def execute_multi_table_sql(self, sql: str, params: Optional[Tuple] = None) -> List[Dict[str, Any]]:
        """
        Execute SQL that may reference multiple tables (JOINs).

        Validates that all referenced tables are in the schema registry
        before executing to prevent SQL injection.
        """
        from app.connectors.snowflake_schema_registry import validate_table_names

        if not validate_table_names(sql):
            logger.error("Multi-table SQL rejected — unknown table in: %s", sql)
            return [{"error": "Query references unknown tables. Only ORDERS, CUSTOMERS, ORDER_ITEMS are allowed."}]

        return self.execute_sql(sql, params)

    def get_tool_definitions(self) -> List[Dict[str, Any]]:
        return [
            {
                "type": "function",
                "function": {
                    "name": f"query_{self.source_name.lower()}",
                    "description": (
                        f"Query {self._table} table in Snowflake. "
                        f"Returns rows with optional filters."
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "limit": {
                                "type": "integer",
                                "description": "Max results to return",
                                "default": 10,
                            },
                        },
                        "required": [],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": f"search_{self.source_name.lower()}",
                    "description": (
                        f"Search {self._table} in Snowflake by "
                        f"{', '.join(self._search_fields)}."
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "query": {
                                "type": "string",
                                "description": f"Search text for {self._table}",
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
            },
            {
                "type": "function",
                "function": {
                    "name": f"get_{self.source_name.lower()}_record",
                    "description": f"Get a single record from {self._table} by {self._id_field}.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            self._id_field: {
                                "type": "string",
                                "description": f"The {self._id_field} to look up",
                            },
                        },
                        "required": [self._id_field],
                    },
                },
            },
        ]

    def close(self):
        """Close the Snowflake connection."""
        if self._conn and not self._conn.is_closed():
            self._conn.close()
            logger.info("Closed Snowflake connection for %s", self.source_name)
