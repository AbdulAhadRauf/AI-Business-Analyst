"""
Abstract base adapter — the interface every data source must implement.

Businesses connect their databases (SQL, MongoDB, REST APIs, JSON files, etc.)
by subclassing BaseDataAdapter and implementing these async methods.

Comparable to:
  - OpenAI Assistants → Function definitions
  - Salesforce Agentforce → Actions/Skills
  - Intercom Fin → Data Connectors
  - Zendesk AI → Action Builder integrations
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class AdapterResult:
    """Standard result returned by all adapter read operations."""
    items: List[Dict[str, Any]] = field(default_factory=list)
    total: int = 0


@dataclass
class AdapterSchema:
    """Describes the shape of data an adapter provides — used by the LLM."""
    source_name: str
    display_name: str
    fields: List[Dict[str, str]]  # [{"name": "order_id", "type": "integer"}, ...]
    id_field: str
    search_fields: List[str]
    writable: bool = False


class BaseDataAdapter(ABC):
    """
    Abstract interface for all data adapters.

    Every data source the business wants the agent to access must
    implement this interface. The adapter registry loads these from
    business_config.yaml and the orchestrator auto-generates LLM tools.
    """

    source_name: str       # Machine key, e.g. "orders"
    display_name: str      # Human label, e.g. "Customer Orders"
    id_field: str           # Primary key field name
    search_fields: List[str]
    writable: bool = False

    # ── Read Operations ────────────────────────────────────────────

    @abstractmethod
    async def fetch(
        self,
        *,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 10,
        offset: int = 0,
        sort_by: Optional[str] = None,
        sort_order: str = "desc",
    ) -> AdapterResult:
        """
        Fetch records with optional filters and pagination.
        Returns AdapterResult(items=[...], total=N).
        """
        ...

    @abstractmethod
    async def get_by_id(self, record_id: Any) -> Optional[Dict[str, Any]]:
        """Get a single record by its primary key."""
        ...

    @abstractmethod
    async def search(self, query: str, *, limit: int = 10) -> AdapterResult:
        """Full-text / fuzzy search across search_fields."""
        ...

    # ── Write Operations ───────────────────────────────────────────

    @abstractmethod
    async def update(
        self, record_id: Any, updates: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Update a record. Returns the updated record.
        Raises NotImplementedError if adapter is read-only.
        """
        ...

    @abstractmethod
    async def create(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a new record. Returns the created record.
        Raises NotImplementedError if adapter is read-only.
        """
        ...

    # ── Schema & Tool Definitions ──────────────────────────────────

    @abstractmethod
    def get_schema(self) -> AdapterSchema:
        """
        Return schema metadata so the LLM knows what fields exist.
        Used by prompt_builder to inform the agent about data shapes.
        """
        ...

    @abstractmethod
    def get_tool_definitions(self) -> List[Dict[str, Any]]:
        """
        Return OpenAI-compatible function/tool definitions.
        The tool_registry uses these to build LangChain tools.
        """
        ...
