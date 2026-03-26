"""
Adapter Registry — loads and manages data adapters from business_config.yaml.

Reads the 'adapters' section of the config and instantiates the correct
adapter class for each data source. The orchestrator uses this registry
to discover available data sources and auto-generate LLM tools.
"""

import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from app.adapters.base_adapter import BaseDataAdapter

logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


def _expand_env(value: str) -> str:
    """Expand ${ENV_VAR} references in config values."""
    if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
        env_key = value[2:-1]
        return os.environ.get(env_key, value)
    return value


class AdapterRegistry:
    """
    Central registry of all data adapters.

    Reads business_config.yaml → adapters section and creates
    the appropriate adapter instance for each data source.
    """

    def __init__(self, config_path: Optional[str] = None):
        self._adapters: Dict[str, BaseDataAdapter] = {}
        self._config: Dict[str, Any] = {}
        self._full_config: Dict[str, Any] = {}

        config_file = Path(config_path) if config_path else PROJECT_ROOT / "business_config.yaml"
        if config_file.exists():
            with open(config_file, encoding="utf-8") as f:
                self._full_config = yaml.safe_load(f) or {}
            self._config = self._full_config.get("adapters", {})
            logger.info("Loaded config from %s (%d adapters)", config_file, len(self._config))
        else:
            logger.warning("No business_config.yaml found at %s", config_file)

    def load_adapters(self) -> None:
        """Instantiate all adapters from config."""
        for name, cfg in self._config.items():
            adapter_type = cfg.get("type", "json")
            try:
                adapter = self._create_adapter(name, adapter_type, cfg)
                self._adapters[name] = adapter
                logger.info(
                    "Registered adapter: %s (type=%s, writable=%s)",
                    name, adapter_type, cfg.get("writable", False),
                )
            except Exception as e:
                logger.error("Failed to create adapter '%s': %s", name, e)
                raise

    def _create_adapter(
        self, name: str, adapter_type: str, cfg: Dict[str, Any]
    ) -> BaseDataAdapter:
        """Factory method — creates the right adapter based on type."""

        if adapter_type == "json":
            from app.adapters.json_adapter import JSONAdapter
            return JSONAdapter(
                source_name=name,
                source=cfg["source"],
                id_field=cfg.get("id_field", ""),
                search_fields=cfg.get("search_fields", []),
                display_name=cfg.get("display_name", name),
                writable=cfg.get("writable", False),
            )

        elif adapter_type == "sql":
            from app.adapters.sql_adapter import SQLAdapter
            return SQLAdapter(
                source_name=name,
                connection=_expand_env(cfg["connection"]),
                table=cfg["table"],
                id_field=cfg.get("id_field", "id"),
                search_fields=cfg.get("search_fields", []),
                display_name=cfg.get("display_name", name),
                writable=cfg.get("writable", False),
            )

        elif adapter_type == "snowflake":
            from app.adapters.snowflake_adapter import SnowflakeAdapter
            return SnowflakeAdapter(
                source_name=name,
                table=cfg["table"],
                id_field=cfg.get("id_field", "id"),
                search_fields=cfg.get("search_fields", []),
                display_name=cfg.get("display_name", name),
                writable=cfg.get("writable", False),
                account=_expand_env(cfg.get("account", "${SNOWFLAKE_ACCOUNT}")),
                user=_expand_env(cfg.get("user", "${SNOWFLAKE_USER}")),
                password=_expand_env(cfg.get("password", "${SNOWFLAKE_PASSWORD}")),
                warehouse=_expand_env(cfg.get("warehouse", "${SNOWFLAKE_WAREHOUSE}")),
                database=_expand_env(cfg.get("database", "${SNOWFLAKE_DATABASE}")),
                schema=_expand_env(cfg.get("schema", "${SNOWFLAKE_SCHEMA}")),
                role=_expand_env(cfg.get("role", "${SNOWFLAKE_ROLE}")),
            )

        else:
            raise ValueError(
                f"Unknown adapter type: '{adapter_type}'. "
                f"Supported types: json, sql, snowflake"
            )

    # ── Public API ─────────────────────────────────────────────────

    def get_adapter(self, name: str) -> BaseDataAdapter:
        """Get a specific adapter by name."""
        if name not in self._adapters:
            raise KeyError(
                f"Adapter '{name}' not found. "
                f"Available: {list(self._adapters.keys())}"
            )
        return self._adapters[name]

    def get_all_adapters(self) -> Dict[str, BaseDataAdapter]:
        """Get all registered adapters."""
        return dict(self._adapters)

    def get_adapter_names(self) -> List[str]:
        """List all registered adapter names."""
        return list(self._adapters.keys())

    def get_all_tool_definitions(self) -> List[Dict[str, Any]]:
        """Collect tool definitions from all adapters (for LLM)."""
        tools = []
        for adapter in self._adapters.values():
            tools.extend(adapter.get_tool_definitions())
        return tools

    def get_all_schemas(self) -> List[Dict[str, Any]]:
        """Collect schemas from all adapters (for prompt building)."""
        schemas = []
        for adapter in self._adapters.values():
            schema = adapter.get_schema()
            schemas.append({
                "source_name": schema.source_name,
                "display_name": schema.display_name,
                "fields": schema.fields,
                "id_field": schema.id_field,
                "search_fields": schema.search_fields,
                "writable": schema.writable,
            })
        return schemas

    def get_full_config(self) -> Dict[str, Any]:
        """Return the complete business_config.yaml as a dict."""
        return self._full_config


# ── Singleton ───────────────────────────────────────────────────────

_registry: Optional[AdapterRegistry] = None


def get_registry(config_path: Optional[str] = None) -> AdapterRegistry:
    """Get or create the global adapter registry."""
    global _registry
    if _registry is None:
        _registry = AdapterRegistry(config_path)
        _registry.load_adapters()
    return _registry
