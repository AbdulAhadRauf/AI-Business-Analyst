"""
Prompt Builder — constructs the LLM system prompt dynamically from config.

Combines company personality, data source schemas, business rules,
auth state, and session context into a comprehensive system prompt.

This is what makes the agent aware of:
- What company it works for
- What data it has access to
- What rules it must follow
- Whether the caller is authenticated
"""

import logging
from typing import Any, Dict, List, Optional

from app.adapters.adapter_registry import AdapterRegistry
from app.rules.rule_engine import RuleEngine
from app.sessions.session import Session

logger = logging.getLogger(__name__)


class PromptBuilder:
    """
    Builds the system prompt for the LangGraph agent dynamically.

    The prompt is personalized per-tenant (from business_config.yaml)
    and per-session (auth state, customer context).
    """

    def __init__(
        self,
        config: Dict[str, Any],
        registry: AdapterRegistry,
        rule_engine: Optional[RuleEngine] = None,
    ):
        self.config = config
        self.registry = registry
        self.rule_engine = rule_engine
        self.company = config.get("company", {})
        self.agent_config = config.get("agent", {})
        self.auth_config = config.get("auth", {})

    def build_system_prompt(self, session: Optional[Session] = None) -> str:
        """
        Build the full system prompt.

        Sections:
        1. Identity & Personality
        2. Available Data Sources
        3. Business Rules
        4. Authentication State
        5. Voice Guidelines
        """
        sections = []

        # ── 1. Identity ──────────────────────────────────────────
        company_name = self.company.get("name", "our company")
        personality = self.agent_config.get("personality", "helpful AI assistant")

        sections.append(
            f"You are {company_name}'s AI assistant. "
            f"Your personality: {personality}.\n"
        )

        # Greeting
        greeting = self.agent_config.get("greeting", "")
        if greeting:
            sections.append(
                f"When a customer first connects, greet them with: \"{greeting}\"\n"
            )

        # ── 2. Available Data Sources ────────────────────────────
        schemas = self.registry.get_all_schemas()
        if schemas:
            sections.append("## DATA SOURCES AVAILABLE TO YOU:\n")
            for schema in schemas:
                field_desc = ", ".join(
                    f"{f['name']} ({f['type']})" for f in schema.get("fields", [])
                )
                writable = "READ-WRITE" if schema.get("writable") else "READ-ONLY"
                sections.append(
                    f"### {schema['display_name']} ({writable})\n"
                    f"- Key: {schema['source_name']}\n"
                    f"- ID field: {schema['id_field']}\n"
                    f"- Searchable by: {', '.join(schema.get('search_fields', []))}\n"
                    f"- Fields: {field_desc}\n"
                )

        # ── 3. Business Rules ────────────────────────────────────
        if self.rule_engine:
            rules_text = self.rule_engine.render_rules_for_prompt()
            if rules_text and rules_text != "No specific business rules configured.":
                sections.append(
                    f"## BUSINESS RULES — YOU MUST FOLLOW THESE:\n{rules_text}\n"
                )

        # ── 4. Authentication State ──────────────────────────────
        auth_required = self.auth_config.get("required", False)
        if auth_required and session:
            if session.authenticated:
                sections.append(
                    f"## AUTHENTICATION: ✅ VERIFIED\n"
                    f"- Customer ID: {session.customer_id}\n"
                )
                if session.customer_profile:
                    safe_profile = {
                        k: v for k, v in session.customer_profile.items()
                        if k not in ("password", "card_number", "ssn", "secret")
                    }
                    sections.append(
                        f"- Customer profile: {safe_profile}\n"
                        f"All queries should be scoped to this customer.\n"
                    )
            else:
                sections.append(
                    "## AUTHENTICATION: ❌ NOT YET VERIFIED\n"
                    "You MUST authenticate the caller before giving them access "
                    "to any personal data. Use the authenticate_caller tool.\n"
                )
        elif auth_required:
            sections.append(
                "## AUTHENTICATION: Required but no session context available.\n"
                "You must verify the caller's identity before proceeding.\n"
            )

        # ── 5. Voice & Response Guidelines ───────────────────────
        max_results = self.agent_config.get("max_voice_results", 5)
        confirm_writes = self.agent_config.get("confirm_before_write", True)

        sections.append(
            "## RESPONSE GUIDELINES:\n"
            "• Keep responses SHORT and conversational — the user is listening via voice.\n"
            "• Summarize data rather than reading out raw records.\n"
            f"• Show at most {max_results} results at a time.\n"
            "• When presenting numbers, round them and say 'about' or 'around'.\n"
            "• If there are many results, highlight the most important ones.\n"
            "• Always mention the data source you used.\n"
            "• If a query is ambiguous, ask a brief clarifying question.\n"
            "• Use natural spoken language — avoid markdown formatting.\n"
        )

        if confirm_writes:
            sections.append(
                "• Before making ANY changes (updates, creates, deletes), "
                "ALWAYS confirm with the customer first. Read back the changes "
                "and ask them to confirm.\n"
            )

        return "\n".join(sections)
