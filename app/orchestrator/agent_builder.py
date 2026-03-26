"""
Agent Builder — dynamically constructs a LangGraph ReAct agent from config.

This is the central orchestration point. It reads business_config.yaml and
assembles:
  1. The right LLM (Groq, OpenAI, Anthropic)
  2. Tools from all registered adapters (auto-generated)
  3. Auth tools (if authentication is required)
  4. System prompt (company personality + rules + data schemas)
  5. Session-aware memory (per-caller conversation history)

Comparable to:
  - OpenAI Assistants → create_assistant()
  - Salesforce Agentforce → Orchestration Layer
  - Intercom Fin → Fin AI Engine
"""

import json
import logging
from typing import Any, Dict, List, Optional

from langchain_groq import ChatGroq
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.prebuilt import create_react_agent
from langchain_core.tools import tool as langchain_tool

from app.adapters.adapter_registry import AdapterRegistry, get_registry
from app.orchestrator.tool_registry import build_all_tools
from app.orchestrator.prompt_builder import PromptBuilder
from app.orchestrator.action_executor import ActionExecutor
from app.rules.rule_engine import RuleEngine
from app.auth.auth_manager import AuthManager
from app.sessions.session import Session
from app.sessions.session_store import SessionStore, get_session_store

logger = logging.getLogger(__name__)


class AgentBuilder:
    """
    Builds and manages the AI agent from business_config.yaml.

    Usage:
        builder = AgentBuilder()
        builder.initialize()

        # For each conversation:
        session = builder.create_session("voice")
        response = await builder.invoke(session, "I want to change my address")
    """

    def __init__(self, config_path: Optional[str] = None):
        self._config_path = config_path
        self._registry: Optional[AdapterRegistry] = None
        self._rule_engine: Optional[RuleEngine] = None
        self._auth_manager: Optional[AuthManager] = None
        self._action_executor: Optional[ActionExecutor] = None
        self._prompt_builder: Optional[PromptBuilder] = None
        self._session_store: Optional[SessionStore] = None
        self._checkpointer = InMemorySaver()
        self._agent = None
        self._tools: List = []
        self._config: Dict[str, Any] = {}
        self._initialized = False

    def initialize(self) -> None:
        """
        Initialize all components from config. Call once at startup.
        """
        if self._initialized:
            return

        # Load adapter registry
        self._registry = get_registry(self._config_path)
        self._config = self._registry.get_full_config()

        # Initialize rule engine
        rules_config = self._config.get("business_rules", {})
        self._rule_engine = RuleEngine(rules_config)

        # Initialize auth manager
        auth_config = self._config.get("auth", {})
        self._auth_manager = AuthManager(
            auth_config, self._registry.get_all_adapters()
        )

        # Initialize action executor
        agent_cfg = self._config.get("agent", {})
        self._action_executor = ActionExecutor(
            rule_engine=self._rule_engine,
            confirm_before_write=agent_cfg.get("confirm_before_write", True),
        )

        # Initialize prompt builder
        self._prompt_builder = PromptBuilder(
            config=self._config,
            registry=self._registry,
            rule_engine=self._rule_engine,
        )

        # Session store
        self._session_store = get_session_store()

        # Build tools from adapters
        self._tools = build_all_tools(self._registry)

        # Add auth tool if auth is required
        if self._auth_manager.is_required:
            self._tools.append(self._build_auth_tool())

        # Build the LangGraph agent
        self._agent = self._build_agent()

        self._initialized = True
        logger.info(
            "AgentBuilder initialized: %d adapters, %d tools, auth=%s",
            len(self._registry.get_adapter_names()),
            len(self._tools),
            self._auth_manager.is_required,
        )

    def _build_agent(self):
        """Build the LangGraph ReAct agent."""
        agent_cfg = self._config.get("agent", {})
        llm_model = agent_cfg.get("llm_model", "meta-llama/llama-4-scout-17b-16e-instruct")

        llm = ChatGroq(model=llm_model, max_tokens=512)

        # Build initial system prompt (without session context)
        system_prompt = self._prompt_builder.build_system_prompt()

        return create_react_agent(
            model=llm,
            tools=self._tools,
            checkpointer=self._checkpointer,
            prompt=system_prompt,
        )

    def _build_auth_tool(self):
        """Build the authenticate_caller tool."""
        auth_mgr = self._auth_manager
        session_store = self._session_store

        @langchain_tool(
            name="authenticate_caller",
            description=(
                "Verify the caller's identity. Call this when you need to authenticate "
                "a user. Pass their response to the verification challenge."
            ),
        )
        async def authenticate_caller(
            session_id: str,
            response: str,
        ) -> str:
            """Process an authentication response from the caller."""
            session = session_store.get_session(session_id)
            if session is None:
                return json.dumps({"error": "Session not found"})

            result = await auth_mgr.process_auth_response(session, response)
            return json.dumps({
                "success": result.success,
                "message": result.message,
                "customer_id": result.customer_id,
                "challenges_remaining": result.challenges_remaining,
            })

        return authenticate_caller

    # ── Public API ──────────────────────────────────────────────

    def create_session(self, channel: str = "voice") -> Session:
        """Create a new session for a caller."""
        if not self._initialized:
            self.initialize()
        return self._session_store.create_session(channel=channel)

    def get_session(self, session_id: str) -> Optional[Session]:
        """Get an existing session."""
        return self._session_store.get_session(session_id)

    async def invoke(self, session: Session, user_message: str) -> str:
        """
        Send a user message to the agent and get the response.

        Handles:
        - Auth flow injection
        - Business rule evaluation
        - Agent invocation
        - Response extraction
        """
        if not self._initialized:
            self.initialize()

        # Track the message
        session.add_message("user", user_message)

        # Check if auth is needed and not yet started
        if self._auth_manager.should_authenticate(session):
            if session.auth_attempts == 0:
                # First interaction — get initial challenge
                challenge = await self._auth_manager.get_initial_challenge(session)
                if challenge:
                    greeting = self._config.get("agent", {}).get("greeting", "")
                    response_text = f"{greeting} {challenge}" if greeting else challenge
                    session.add_message("assistant", response_text)
                    return response_text

        # Invoke the agent
        agent_response = self._agent.invoke(
            {"messages": [{"role": "user", "content": user_message}]},
            config={"configurable": {"thread_id": session.id}},
        )

        response_text = agent_response["messages"][-1].content

        # Check for greeting rules (after auth)
        if session.authenticated and session.customer_profile:
            from app.rules.rule_models import RuleEvalContext
            context = RuleEvalContext(
                customer=session.customer_profile,
                session=session.to_dict(),
            )
            greetings = self._rule_engine.get_greeting_messages(context)
            if greetings and len(session.messages) <= 4:
                # Prepend greeting to first response after auth
                greeting_text = " ".join(greetings)
                response_text = f"{greeting_text} {response_text}"

        session.add_message("assistant", response_text)
        return response_text

    @property
    def config(self) -> Dict[str, Any]:
        """Access the full business config."""
        return self._config

    @property
    def auth_manager(self) -> Optional[AuthManager]:
        return self._auth_manager

    @property
    def rule_engine(self) -> Optional[RuleEngine]:
        return self._rule_engine

    @property
    def action_executor(self) -> Optional[ActionExecutor]:
        return self._action_executor

    @property
    def registry(self) -> Optional[AdapterRegistry]:
        return self._registry


# ── Singleton ───────────────────────────────────────────────────

_builder: Optional[AgentBuilder] = None


def get_agent_builder(config_path: Optional[str] = None) -> AgentBuilder:
    """Get or create the global agent builder."""
    global _builder
    if _builder is None:
        _builder = AgentBuilder(config_path)
        _builder.initialize()
    return _builder
