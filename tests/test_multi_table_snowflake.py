"""
Tests for Snowflake schema registry and SQL validation.

Tests the single-table ORDERS schema (the only table currently in PIZZA_DB)
and the SQL validation that prevents queries against unknown tables.
"""

import pytest
from app.connectors.snowflake_schema_registry import (
    TABLE_SCHEMAS,
    TABLE_RELATIONSHIPS,
    ALLOWED_TABLES,
    validate_table_names,
    get_schema_prompt,
    get_table_names,
    get_table_columns,
)


# ═══════════════════════════════════════════════════════════════════════
#  Schema Registry Tests
# ═══════════════════════════════════════════════════════════════════════

class TestSchemaRegistry:
    """Test the Snowflake schema registry."""

    def test_orders_table_present(self):
        """Schema registry should have the ORDERS table."""
        names = get_table_names()
        assert "ORDERS" in names

    def test_orders_table_columns(self):
        """ORDERS table should have all 9 expected columns."""
        cols = get_table_columns("ORDERS")
        assert "ORDER_ID" in cols
        assert "CUSTOMER_NAME" in cols
        assert "CUSTOMER_EMAIL" in cols
        assert "PIZZA_NAME" in cols
        assert "QUANTITY" in cols
        assert "PRICE" in cols
        assert "ORDER_STATUS" in cols
        assert "ORDER_DATE" in cols
        assert "PHONE_NUMBERS" in cols
        assert len(cols) == 9

    def test_unknown_table_returns_empty(self):
        """Unknown table should return empty columns list."""
        cols = get_table_columns("NONEXISTENT_TABLE")
        assert cols == []

    def test_schema_has_primary_key(self):
        """ORDERS table should have ORDER_ID as primary key."""
        schema = TABLE_SCHEMAS["ORDERS"]
        assert schema["primary_key"] == "ORDER_ID"

    def test_allowed_tables_match_schemas(self):
        """ALLOWED_TABLES should match TABLE_SCHEMAS keys."""
        assert ALLOWED_TABLES == set(TABLE_SCHEMAS.keys())


# ═══════════════════════════════════════════════════════════════════════
#  Schema Prompt Generation Tests
# ═══════════════════════════════════════════════════════════════════════

class TestSchemaPrompt:
    """Test the LLM prompt generation from schemas."""

    def test_prompt_includes_orders_table(self):
        """Schema prompt should mention the ORDERS table."""
        prompt = get_schema_prompt()
        assert "ORDERS" in prompt

    def test_prompt_includes_all_columns(self):
        """Schema prompt should include all column descriptions."""
        prompt = get_schema_prompt()
        assert "ORDER_ID" in prompt
        assert "CUSTOMER_NAME" in prompt
        assert "CUSTOMER_EMAIL" in prompt
        assert "PIZZA_NAME" in prompt
        assert "QUANTITY" in prompt
        assert "PRICE" in prompt
        assert "ORDER_STATUS" in prompt
        assert "ORDER_DATE" in prompt
        assert "PHONE_NUMBERS" in prompt

    def test_prompt_includes_query_examples(self):
        """Schema prompt should include query examples."""
        prompt = get_schema_prompt()
        assert "SELECT" in prompt
        assert "FROM ORDERS" in prompt

    def test_prompt_mentions_pizza_db(self):
        """Schema prompt should reference PIZZA_DB database."""
        prompt = get_schema_prompt()
        assert "PIZZA_DB" in prompt


# ═══════════════════════════════════════════════════════════════════════
#  SQL Validation Tests
# ═══════════════════════════════════════════════════════════════════════

class TestSQLValidation:
    """Test SQL table name validation."""

    def test_valid_single_table_query(self):
        """Query on ORDERS table should pass."""
        sql = "SELECT * FROM ORDERS WHERE ORDER_ID = 1"
        assert validate_table_names(sql) is True

    def test_valid_filtered_query(self):
        """Filtered query on ORDERS should pass."""
        sql = "SELECT * FROM ORDERS WHERE PHONE_NUMBERS = '1234567890' AND ORDER_STATUS = 'delivered'"
        assert validate_table_names(sql) is True

    def test_valid_aggregation_query(self):
        """Aggregation query on ORDERS should pass."""
        sql = "SELECT PIZZA_NAME, COUNT(*) FROM ORDERS GROUP BY PIZZA_NAME"
        assert validate_table_names(sql) is True

    def test_invalid_table_rejected(self):
        """Query on unknown table should fail."""
        sql = "SELECT * FROM USERS WHERE ID = 1"
        assert validate_table_names(sql) is False

    def test_invalid_join_table_rejected(self):
        """JOIN with unknown table should fail."""
        sql = (
            "SELECT * FROM ORDERS o "
            "LEFT JOIN EVIL_TABLE e ON o.ORDER_ID = e.ID"
        )
        assert validate_table_names(sql) is False

    def test_case_insensitive_table_names(self):
        """Table names should be validated case-insensitively."""
        sql = "SELECT * FROM orders WHERE ORDER_ID = 1"
        assert validate_table_names(sql) is True

    def test_empty_query(self):
        """Query without FROM should pass (no tables to validate)."""
        sql = "SELECT 1"
        assert validate_table_names(sql) is True

    def test_subquery_validation(self):
        """Subqueries referencing ORDERS should pass."""
        sql = "SELECT * FROM ORDERS WHERE PRICE > (SELECT AVG(PRICE) FROM ORDERS)"
        assert validate_table_names(sql) is True


# ═══════════════════════════════════════════════════════════════════════
#  Multi-Table Snowflake Connector Tests (mocked)
# ═══════════════════════════════════════════════════════════════════════

try:
    import snowflake.connector
    HAS_SNOWFLAKE = True
except ImportError:
    HAS_SNOWFLAKE = False


@pytest.mark.skipif(not HAS_SNOWFLAKE, reason="snowflake-connector-python not installed")
class TestMultiTableSnowflakeConnector:
    """Test execute_multi_table_sql with mocked Snowflake."""

    def _make_connector(self):
        from app.connectors.snowflake_connector import SnowflakeConnector
        return SnowflakeConnector(
            table="ORDERS",
            id_field="ORDER_ID",
            search_fields=("CUSTOMER_NAME", "ORDER_STATUS"),
            account="test_account",
            user="test_user",
            password="test_pass",
            warehouse="TEST_WH",
            database="TEST_DB",
            schema="PUBLIC",
            cache_ttl=60,
        )

    def test_multi_table_rejects_unknown_table(self):
        """execute_multi_table_sql should reject queries with unknown tables."""
        conn = self._make_connector()
        result = conn.execute_multi_table_sql(
            "SELECT * FROM EVIL_TABLE WHERE ID = 1"
        )
        assert len(result) == 1
        assert "error" in result[0]

    def test_multi_table_accepts_valid_query(self):
        """execute_multi_table_sql should accept queries on ORDERS table."""
        from unittest.mock import MagicMock, patch

        conn = self._make_connector()

        mock_conn = MagicMock()
        type(mock_conn).is_closed = MagicMock(return_value=False)
        mock_cursor = MagicMock()
        mock_cursor.description = [
            ("ORDER_ID",), ("CUSTOMER_NAME",), ("PIZZA_NAME",),
            ("QUANTITY",), ("PRICE",), ("ORDER_STATUS",),
        ]
        mock_cursor.fetchall.return_value = [
            (1, "Ahad", "Margherita", 2, 15.99, "delivered"),
        ]
        mock_conn.cursor.return_value = mock_cursor

        with patch("snowflake.connector.connect", return_value=mock_conn):
            result = conn.execute_multi_table_sql(
                "SELECT * FROM ORDERS WHERE PHONE_NUMBERS = '1234567890'"
            )
            assert len(result) == 1
            assert result[0]["ORDER_ID"] == 1
            assert result[0]["CUSTOMER_NAME"] == "Ahad"
            assert result[0]["PIZZA_NAME"] == "Margherita"
