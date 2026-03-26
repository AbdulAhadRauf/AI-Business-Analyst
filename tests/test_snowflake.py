"""
Tests for Snowflake connector, adapter, filler messages, and registry integration.

All Snowflake tests use mocked connections — no real Snowflake account needed.
"""

import json
import time
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

# Check if snowflake-connector-python is installed
try:
    import snowflake.connector
    HAS_SNOWFLAKE = True
except ImportError:
    HAS_SNOWFLAKE = False


# ═══════════════════════════════════════════════════════════════════════
# Snowflake Connector Tests
# ═══════════════════════════════════════════════════════════════════════

@pytest.mark.skipif(not HAS_SNOWFLAKE, reason="snowflake-connector-python not installed")
class TestSnowflakeConnector:
    """Test SnowflakeConnector with mocked snowflake.connector."""

    def _make_connector(self):
        from app.connectors.snowflake_connector import SnowflakeConnector
        return SnowflakeConnector(
            table="TEST_TABLE",
            id_field="ID",
            search_fields=("NAME", "STATUS"),
            account="test_account",
            user="test_user",
            password="test_pass",
            warehouse="TEST_WH",
            database="TEST_DB",
            schema="PUBLIC",
            cache_ttl=60,
        )

    def _mock_cursor(self, rows, columns):
        """Create a mock cursor that returns the given rows/columns."""
        cursor = MagicMock()
        cursor.description = [(col,) for col in columns]
        cursor.fetchall.return_value = rows
        cursor.fetchone.return_value = rows[0] if rows else None
        return cursor

    @patch("snowflake.connector.connect")
    def test_fetch_returns_items_and_total(self, mock_connect):
        mock_conn = MagicMock()
        type(mock_conn).is_closed = MagicMock(return_value=False)
        mock_cursor = self._mock_cursor(
            rows=[(1, "Alice", "active"), (2, "Bob", "inactive")],
            columns=["ID", "NAME", "STATUS"],
        )
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        conn = self._make_connector()
        result = conn.fetch(limit=5)

        assert "items" in result
        assert "total" in result
        assert len(result["items"]) <= 5

    @patch("snowflake.connector.connect")
    def test_search_filters_by_fields(self, mock_connect):
        mock_conn = MagicMock()
        type(mock_conn).is_closed = MagicMock(return_value=False)
        mock_cursor = self._mock_cursor(
            rows=[
                (1, "Alice Smith", "active"),
                (2, "Bob Jones", "inactive"),
                (3, "Alice Wonderland", "active"),
            ],
            columns=["ID", "NAME", "STATUS"],
        )
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        conn = self._make_connector()
        result = conn.search("alice", limit=10)

        assert result["total"] > 0
        for item in result["items"]:
            found = any("alice" in str(item.get(f, "")).lower() for f in ("NAME", "STATUS"))
            assert found

    @patch("snowflake.connector.connect")
    def test_get_by_id(self, mock_connect):
        mock_conn = MagicMock()
        type(mock_conn).is_closed = MagicMock(return_value=False)
        mock_cursor = self._mock_cursor(
            rows=[(1, "Alice", "active")],
            columns=["ID", "NAME", "STATUS"],
        )
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        conn = self._make_connector()
        record = conn.get_by_id(1)
        assert record is not None
        assert record["ID"] == 1

    @patch("snowflake.connector.connect")
    def test_get_by_id_not_found(self, mock_connect):
        mock_conn = MagicMock()
        type(mock_conn).is_closed = MagicMock(return_value=False)
        mock_cursor = self._mock_cursor(
            rows=[(1, "Alice", "active")],
            columns=["ID", "NAME", "STATUS"],
        )
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        conn = self._make_connector()
        record = conn.get_by_id(99999)
        assert record is None

    @patch("snowflake.connector.connect")
    def test_cache_ttl(self, mock_connect):
        """Second _load() within TTL should not re-query Snowflake."""
        mock_conn = MagicMock()
        type(mock_conn).is_closed = MagicMock(return_value=False)
        mock_cursor = self._mock_cursor(
            rows=[(1, "Alice", "active")],
            columns=["ID", "NAME", "STATUS"],
        )
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        conn = self._make_connector()
        conn.fetch(limit=5)  # first call — queries Snowflake
        conn.fetch(limit=5)  # second call — should use cache

        # cursor.execute should only be called once (the cache saves the second call)
        assert mock_cursor.execute.call_count == 1

    @patch("snowflake.connector.connect")
    def test_invalidate_cache(self, mock_connect):
        mock_conn = MagicMock()
        type(mock_conn).is_closed = MagicMock(return_value=False)
        mock_cursor = self._mock_cursor(
            rows=[(1, "Alice", "active")],
            columns=["ID", "NAME", "STATUS"],
        )
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        conn = self._make_connector()
        conn.fetch(limit=5)
        conn.invalidate_cache()
        conn.fetch(limit=5)

        # Should have queried twice (cache was invalidated)
        assert mock_cursor.execute.call_count == 2

    def test_tool_definitions(self):
        conn = self._make_connector()
        tools = conn.get_tool_definitions()
        assert len(tools) >= 3
        names = [t["function"]["name"] for t in tools]
        assert any("query" in n for n in names)
        assert any("search" in n for n in names)
        assert any("record" in n for n in names)


# ═══════════════════════════════════════════════════════════════════════
# Snowflake Adapter Tests (async)
# ═══════════════════════════════════════════════════════════════════════

class TestSnowflakeAdapter:
    """Test SnowflakeAdapter with mocked connections."""

    def _make_adapter(self):
        from app.adapters.snowflake_adapter import SnowflakeAdapter
        return SnowflakeAdapter(
            source_name="test_sf",
            table="TEST_TABLE",
            id_field="ID",
            search_fields=["NAME", "STATUS"],
            account="test_account",
            user="test_user",
            password="test_pass",
            warehouse="TEST_WH",
            database="TEST_DB",
            schema="PUBLIC",
            display_name="Test Snowflake",
        )

    def test_get_schema(self):
        adapter = self._make_adapter()
        schema = adapter.get_schema()
        assert schema.source_name == "test_sf"
        assert schema.display_name == "Test Snowflake"
        assert schema.id_field == "ID"

    def test_tool_definitions(self):
        adapter = self._make_adapter()
        tools = adapter.get_tool_definitions()
        assert len(tools) >= 3
        for tool in tools:
            assert tool["type"] == "function"
            assert "name" in tool["function"]
            assert "parameters" in tool["function"]

    def test_tool_definitions_writable(self):
        from app.adapters.snowflake_adapter import SnowflakeAdapter
        adapter = SnowflakeAdapter(
            source_name="writable_sf",
            table="ORDERS",
            id_field="ORDER_ID",
            search_fields=["NAME"],
            writable=True,
        )
        tools = adapter.get_tool_definitions()
        names = [t["function"]["name"] for t in tools]
        assert "update_writable_sf" in names


# ═══════════════════════════════════════════════════════════════════════
# Adapter Registry Snowflake Factory Test
# ═══════════════════════════════════════════════════════════════════════

class TestAdapterRegistrySnowflake:
    """Test that the registry correctly creates SnowflakeAdapter instances."""

    @patch.dict("os.environ", {
        "SNOWFLAKE_ACCOUNT": "test_account",
        "SNOWFLAKE_USER": "test_user",
        "SNOWFLAKE_PASSWORD": "test_pass",
        "SNOWFLAKE_WAREHOUSE": "TEST_WH",
        "SNOWFLAKE_DATABASE": "TEST_DB",
        "SNOWFLAKE_SCHEMA": "PUBLIC",
        "SNOWFLAKE_ROLE": "",
    })
    def test_registry_creates_snowflake_adapter(self, tmp_path):
        """Write a temp config with a Snowflake adapter and load it."""
        config_content = """
adapters:
  sf_orders:
    type: snowflake
    table: ORDERS
    id_field: ORDER_ID
    search_fields: ["CUSTOMER_NAME"]
    display_name: "SF Orders"
"""
        config_file = tmp_path / "test_config.yaml"
        config_file.write_text(config_content)

        from app.adapters.adapter_registry import AdapterRegistry
        registry = AdapterRegistry(config_path=str(config_file))
        registry.load_adapters()

        adapter = registry.get_adapter("sf_orders")
        assert adapter.source_name == "sf_orders"
        assert adapter.display_name == "SF Orders"

        # Verify tool definitions
        tools = adapter.get_tool_definitions()
        assert len(tools) >= 2


# ═══════════════════════════════════════════════════════════════════════
# Filler Message Logic Tests
# ═══════════════════════════════════════════════════════════════════════

class TestFillerMessages:
    """Test the filler message logic without needing Groq or FastRTC."""

    def test_filler_config_loaded(self):
        """Verify filler messages are loaded from business_config.yaml."""
        import yaml
        from pathlib import Path

        config_path = Path(__file__).resolve().parent.parent / "business_config.yaml"
        if config_path.exists():
            with open(config_path, encoding="utf-8") as f:
                config = yaml.safe_load(f)
            perf = config.get("performance", {})
            fillers = perf.get("filler_messages", [])
            assert len(fillers) >= 1, "Should have at least one filler message"
            timeout = perf.get("filler_timeout_seconds", 1.5)
            assert timeout > 0, "Timeout should be positive"

    def test_filler_timeout_triggers_correctly(self):
        """
        Simulate the filler logic: if agent takes > timeout,
        a filler message should be used.
        """
        import time
        from concurrent.futures import ThreadPoolExecutor

        def slow_agent(text):
            time.sleep(0.5)  # simulate slow response
            return "Here are the results"

        executor = ThreadPoolExecutor(max_workers=1)
        future = executor.submit(slow_agent, "test")

        filler_used = False
        try:
            result = future.result(timeout=0.1)  # very short timeout
        except Exception:
            filler_used = True
            result = future.result()  # wait for real result

        assert filler_used, "Filler should have been triggered"
        assert result == "Here are the results"
        executor.shutdown(wait=False)

    def test_no_filler_when_agent_is_fast(self):
        """If agent responds quickly, no filler should be used."""
        import time
        from concurrent.futures import ThreadPoolExecutor

        def fast_agent(text):
            return "Quick response"

        executor = ThreadPoolExecutor(max_workers=1)
        future = executor.submit(fast_agent, "test")

        filler_used = False
        try:
            result = future.result(timeout=2.0)  # generous timeout
        except Exception:
            filler_used = True
            result = future.result()

        assert not filler_used, "Filler should NOT have been triggered"
        assert result == "Quick response"
        executor.shutdown(wait=False)
