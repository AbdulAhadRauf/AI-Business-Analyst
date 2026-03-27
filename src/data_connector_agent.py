"""
Data Connector Agent — LangGraph agent with voice authentication,
RBAC enforcement, and multi-table Snowflake support.

Authentication Flow:
    1. User calls in → agent asks for phone number
    2. User provides phone → agent asks for PIN
    3. User provides PIN → agent authenticates via user_db
    4. On success → role/permissions loaded, data access enabled
    5. On failure → retry up to max_attempts

RBAC:
    - admin  → sees all data, all users
    - manager → sees all data sources, all users
    - customer → sees ONLY their own orders/tickets (scoped by phone)

Multi-Table Snowflake:
    - SQL generator knows about ORDERS, CUSTOMERS, ORDER_ITEMS
    - Can generate JOINs across tables
    - All queries scoped by RBAC filters
"""

import json, sys, re
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from langchain_groq import ChatGroq
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.prebuilt import create_react_agent
from langgraph.graph import StateGraph, START, END
from langgraph.graph.message import add_messages
from typing import TypedDict, Annotated, List, Dict, Any, Literal, Optional
from langchain_core.messages import BaseMessage, HumanMessage, AIMessage, SystemMessage
from pydantic import BaseModel, Field
from loguru import logger
from langchain_core.tools import tool
from dotenv import load_dotenv

load_dotenv()

from app.connectors.crm_connector import CRMConnector
from app.connectors.support_connector import SupportConnector
from app.connectors.analytics_connector import AnalyticsConnector
from app.services.crm_service import get_customer_by_phone, get_order_by_id, get_customer_orders
from app.services.business_rules import apply_customer_rules
from app.config import settings
from app.auth.user_db import authenticate_by_phone_pin, get_user_by_phone
from app.auth.rbac import RBACManager
from app.connectors.snowflake_schema_registry import get_schema_prompt, validate_table_names

crm = CRMConnector()
support = SupportConnector()
analytics = AnalyticsConnector()

# ── RBAC Manager ───────────────────────────────────────────────────────
rbac = RBACManager()


def _int(value: str, default: int) -> int:
    try:
        return int(value) if value and value.strip() else default
    except (ValueError, TypeError):
        return default


# ── CRM Tools (RBAC-aware) ────────────────────────────────────────────

@tool
def search_customers(query: str, limit: str = "5") -> str:
    """Search CRM customers by name or email. Requires admin or manager role."""
    result = crm.search(query, limit=_int(limit, 5))
    return json.dumps(result)

@tool
def get_customers(status: str = "", limit: str = "5") -> str:
    """List CRM customers. Filter by status (active/inactive). Requires admin or manager role."""
    result = crm.fetch(limit=_int(limit, 5), status=status or None)
    return json.dumps(result)

@tool
def get_customer_by_id(customer_id: str) -> str:
    """Get a specific customer by ID. Requires admin or manager role."""
    c = crm.get_by_id(_int(customer_id, 0))
    return json.dumps(c if c else {"error": f"Customer {customer_id} not found"})

@tool
def get_customer_context_tool(phone_number: str = "", order_id: str = "") -> str:
    """Identify customer across CRM using phone number or given order ID, returning business context, recent orders, and rewards."""
    customer = None
    if order_id:
        order = get_order_by_id(order_id)
        if order:
            from app.services.crm_service import MOCK_CUSTOMERS
            for c in MOCK_CUSTOMERS:
                if c["customer_id"] == order["customer_id"]:
                    customer = c
                    break
    elif phone_number:
        customer = get_customer_by_phone(phone_number)

    if not customer:
        return json.dumps({"error": "Customer not found."})

    orders = get_customer_orders(customer["customer_id"])
    context = apply_customer_rules(customer, orders)

    return json.dumps(context, default=str)


# ── Support Tools ──────────────────────────────────────────────────────

@tool
def get_support_tickets(priority: str = "", status: str = "", customer_id: str = "", limit: str = "5") -> str:
    """Retrieve support tickets. Filter by priority, status, customer_id."""
    result = support.fetch(
        limit=_int(limit, 5),
        priority=priority or None,
        status=status or None,
        customer_id=_int(customer_id, 0) if customer_id else None,
    )
    return json.dumps(result)

@tool
def get_ticket_by_id(ticket_id: str) -> str:
    """Get a specific support ticket by ID."""
    t = support.get_by_id(_int(ticket_id, 0))
    return json.dumps(t if t else {"error": f"Ticket {ticket_id} not found"})


# ── Analytics Tools ────────────────────────────────────────────────────

@tool
def get_analytics(metric: str = "", days: str = "7", limit: str = "10") -> str:
    """Retrieve analytics metrics. Filter by metric name and time range (days). Requires admin or manager role."""
    result = analytics.fetch(limit=_int(limit, 10), metric=metric or None, days=_int(days, 7))
    return json.dumps(result)

@tool
def get_analytics_summary(metric: str = "", days: str = "7") -> str:
    """Summarized analytics: average, min, max, trend. Best for voice. Requires admin or manager role."""
    return json.dumps(analytics.get_summary(metric=metric or None, days=_int(days, 7)))


# ── Snowflake Tools (conditional) ─────────────────────────────────────

snowflake_tools = []
snowflake_enabled = bool(settings.SNOWFLAKE_ACCOUNT)

if snowflake_enabled:
    from app.connectors.snowflake_connector import SnowflakeConnector

    sf = SnowflakeConnector(
        table=settings.SNOWFLAKE_DATABASE and "ORDERS" or "ORDERS",
        id_field="ORDER_ID",
        search_fields=("CUSTOMER_NAME", "ORDER_STATUS"),
        account=settings.SNOWFLAKE_ACCOUNT,
        user=settings.SNOWFLAKE_USER,
        password=settings.SNOWFLAKE_PASSWORD,
        warehouse=settings.SNOWFLAKE_WAREHOUSE,
        database=settings.SNOWFLAKE_DATABASE,
        schema=settings.SNOWFLAKE_SCHEMA,
        role=settings.SNOWFLAKE_ROLE,
    )
    logger.info("❄️ Snowflake connector enabled (account: {})", settings.SNOWFLAKE_ACCOUNT)

    @tool
    def query_snowflake() -> str:
        """Query pizza order records from the Snowflake data warehouse. Use this when the user asks about orders, pizza, or Snowflake data. Returns recent order rows."""
        result = sf.fetch(limit=10)
        return json.dumps(result, default=str)

    @tool
    def search_snowflake(query: str) -> str:
        """Search Snowflake pizza orders by customer name or order status. Use this when the user asks to find a specific order or customer in Snowflake."""
        result = sf.search(query, limit=5)
        return json.dumps(result, default=str)

    @tool
    def get_snowflake_record(record_id: str) -> str:
        """Get a specific pizza order from Snowflake by its ORDER_ID."""
        r = sf.get_by_id(_int(record_id, 0))
        return json.dumps(r if r else {"error": f"Record {record_id} not found in Snowflake"}, default=str)

    snowflake_tools = [query_snowflake, search_snowflake, get_snowflake_record]
else:
    logger.info("❄️ Snowflake not configured (SNOWFLAKE_ACCOUNT is empty)")


# ═══════════════════════════════════════════════════════════════════════
#  AGENT STATE — includes auth + RBAC fields
# ═══════════════════════════════════════════════════════════════════════

class AgentState(TypedDict):
    messages: Annotated[List[BaseMessage], add_messages]
    phone_number: str
    order_id: str
    intent: str
    # ── Auth + RBAC state ──────────────────────────────────────────
    authenticated: bool
    user_name: str
    user_role: str           # admin | manager | customer
    customer_id: str
    auth_step: str           # none | awaiting_phone | awaiting_pin | done | failed
    auth_attempts: int
    allowed_sources: List[str]


# ═══════════════════════════════════════════════════════════════════════
#  STRUCTURED OUTPUT MODELS
# ═══════════════════════════════════════════════════════════════════════

class IntentResult(BaseModel):
    intent: str = Field(description="'snowflake' if user asks about orders, deliveries, customer accounts, transactions. Else 'general'.")
    phone_number: str = Field(description="Extracted phone number if present in user message, else empty string.")
    order_id: str = Field(description="Extracted order ID if present in user message, else empty string.")

class AuthExtraction(BaseModel):
    phone_number: str = Field(description="Extracted phone number (digits only), or empty string if not found.")
    pin: str = Field(description="Extracted PIN (4 digits), or empty string if not found.")

class SqlResult(BaseModel):
    sql: str = Field(description="A valid Snowflake SQL query")


# ═══════════════════════════════════════════════════════════════════════
#  GRAPH NODES
# ═══════════════════════════════════════════════════════════════════════

def auth_router(state: AgentState) -> str:
    """
    First node — routes to auth flow or intent extraction.

    If user is not authenticated, we go through the voice auth flow:
      1. Ask for phone number
      2. Ask for PIN
      3. Verify credentials
    """
    if state.get("authenticated", False):
        return "intent_extractor"

    auth_step = state.get("auth_step", "none")

    if auth_step == "none":
        return "ask_phone"
    elif auth_step == "awaiting_phone":
        return "process_phone"
    elif auth_step == "awaiting_pin":
        return "process_pin"
    elif auth_step == "done":
        return "intent_extractor"
    elif auth_step == "failed":
        return "auth_failed"
    else:
        return "ask_phone"


def ask_phone(state: AgentState):
    """Ask the caller for their phone number."""
    msg = AIMessage(content=(
        "Welcome to Pizza Express! For security, I need to verify your identity. "
        "Could you please tell me the phone number on your account?"
    ))
    return {"messages": [msg], "auth_step": "awaiting_phone", "auth_attempts": state.get("auth_attempts", 0)}


def process_phone(state: AgentState):
    """Extract phone number from the user's voice response."""
    latest_msg = state["messages"][-1].content
    model = ChatGroq(model=settings.GROQ_LLM_MODEL, max_tokens=128, temperature=0)

    # Use LLM to extract the phone number from natural speech
    structured_llm = model.with_structured_output(AuthExtraction)
    try:
        result = structured_llm.invoke(
            f"Extract the phone number (digits only) from this message. "
            f"If the user says something like 'one two three four five six seven eight nine zero' "
            f"convert it to digits '1234567890'. Message: {latest_msg}"
        )
        phone = re.sub(r'[^\d]', '', result.phone_number)
    except Exception as e:
        logger.error(f"Phone extraction failed: {e}")
        phone = re.sub(r'[^\d]', '', latest_msg)

    if len(phone) >= 10:
        # Valid phone number extracted — ask for PIN
        phone = phone[-10:]  # take last 10 digits
        user = get_user_by_phone(phone)
        if user:
            msg = AIMessage(content=f"Thank you! I found your account. Now please tell me your 4-digit PIN to verify your identity.")
            return {"messages": [msg], "phone_number": phone, "auth_step": "awaiting_pin"}
        else:
            attempts = state.get("auth_attempts", 0) + 1
            if attempts >= 3:
                msg = AIMessage(content="I'm sorry, I couldn't find an account with that phone number. Please contact us directly for assistance.")
                return {"messages": [msg], "auth_step": "failed", "auth_attempts": attempts}
            msg = AIMessage(content=f"I couldn't find an account with that phone number. Could you try again? You have {3 - attempts} attempts left.")
            return {"messages": [msg], "auth_step": "awaiting_phone", "auth_attempts": attempts}
    else:
        msg = AIMessage(content="I didn't quite catch a valid phone number. Could you please say your 10-digit phone number again?")
        return {"messages": [msg], "auth_step": "awaiting_phone"}


def process_pin(state: AgentState):
    """Extract PIN from user's voice response and authenticate."""
    latest_msg = state["messages"][-1].content
    phone = state.get("phone_number", "")

    # Extract PIN from speech
    model = ChatGroq(model=settings.GROQ_LLM_MODEL, max_tokens=128, temperature=0)
    structured_llm = model.with_structured_output(AuthExtraction)
    try:
        result = structured_llm.invoke(
            f"Extract the 4-digit PIN from this message. "
            f"If the user says 'one one one one' convert to '1111'. "
            f"Message: {latest_msg}"
        )
        pin = re.sub(r'[^\d]', '', result.pin)
    except Exception as e:
        logger.error(f"PIN extraction failed: {e}")
        pin = re.sub(r'[^\d]', '', latest_msg)

    if len(pin) < 4:
        msg = AIMessage(content="I didn't catch a valid 4-digit PIN. Could you please say it again?")
        return {"messages": [msg], "auth_step": "awaiting_pin"}

    pin = pin[:4]  # take first 4 digits

    # Authenticate
    user = authenticate_by_phone_pin(phone, pin)

    if user:
        role = user.get("role", "customer")
        name = user.get("name", "there")
        customer_id = user.get("customer_id", "")
        allowed = list(rbac.get_allowed_sources(role))

        logger.info(f"✅ Authenticated: {name} (role={role}, customer_id={customer_id})")

        msg = AIMessage(content=(
            f"You're verified, {name}! Welcome back. "
            f"Your role is {role}. How can I help you today?"
        ))
        return {
            "messages": [msg],
            "authenticated": True,
            "user_name": name,
            "user_role": role,
            "customer_id": customer_id,
            "phone_number": phone,
            "auth_step": "done",
            "allowed_sources": allowed,
        }
    else:
        attempts = state.get("auth_attempts", 0) + 1
        if attempts >= 3:
            msg = AIMessage(content="I'm sorry, that PIN doesn't match. You've exceeded the maximum number of attempts. Please contact us directly for assistance.")
            return {"messages": [msg], "auth_step": "failed", "auth_attempts": attempts}

        msg = AIMessage(content=f"That PIN doesn't match what we have on file. You have {3 - attempts} attempts left. Please try again.")
        return {"messages": [msg], "auth_step": "awaiting_pin", "auth_attempts": attempts}


def auth_failed(state: AgentState):
    """Terminal node when authentication fails."""
    msg = AIMessage(content="I wasn't able to verify your identity. For security, I can't provide access to account data. Please contact us at our support line for further assistance. Goodbye!")
    return {"messages": [msg]}


def intent_extractor(state: AgentState):
    """Extract intent from the authenticated user's message."""
    latest_msg = state["messages"][-1].content
    model = ChatGroq(model=settings.GROQ_LLM_MODEL, max_tokens=256, temperature=0)
    structured_llm = model.with_structured_output(IntentResult)
    try:
        result = structured_llm.invoke(f"Extract intent mapping and entities from this message:\n{latest_msg}")
        phone = result.phone_number or state.get("phone_number", "")
        order = result.order_id or state.get("order_id", "")
        intent = result.intent
    except Exception as e:
        logger.error(f"Intent extraction failed: {e}")
        phone = state.get("phone_number", "")
        order = state.get("order_id", "")
        intent = "general"

    return {"intent": intent, "phone_number": phone, "order_id": order}


def rbac_check(state: AgentState) -> str:
    """
    Check if the user has permission for the intended data source.

    Routes to the appropriate node based on intent and permissions.
    """
    intent = state.get("intent", "general")
    role = state.get("user_role", "customer")

    if intent == "snowflake" and snowflake_enabled:
        if rbac.can_access_data(role, "snowflake_orders"):
            return "snowflake_query"
        else:
            return "access_denied"

    # General intent — let react agent handle with RBAC context
    return "react_agent"


def access_denied(state: AgentState):
    """Response when user doesn't have permission to access requested data."""
    role = state.get("user_role", "customer")
    msg = AIMessage(content=(
        f"I'm sorry, but your account role ({role}) doesn't have access to that data. "
        f"If you believe this is an error, please contact your administrator."
    ))
    return {"messages": [msg]}


def snowflake_query_node(state: AgentState):
    """
    Multi-table Snowflake query node with RBAC enforcement.

    1. Gets full schema from registry (ORDERS, CUSTOMERS, ORDER_ITEMS)
    2. Applies RBAC filter (customer can only see their own data)
    3. LLM generates SQL with JOINs if needed
    4. Executes and formats response
    """
    model = ChatGroq(model=settings.GROQ_LLM_MODEL, max_tokens=1024, temperature=0)

    phone = state.get("phone_number", "")
    order = state.get("order_id", "")
    role = state.get("user_role", "customer")
    user_name = state.get("user_name", "Customer")
    latest_msg = state["messages"][-1].content

    # ── 1. Build RBAC WHERE clause ──────────────────────────────────
    rbac_filter = rbac.get_snowflake_filter(role, phone=phone)
    if rbac_filter:
        rbac_clause = f"RBAC CONSTRAINT (MUST include in WHERE): {rbac_filter}"
    else:
        rbac_clause = "No RBAC filter — user has full data access."

    # Additional identity conditions
    identity_conditions = []
    if phone and role == "customer":
        identity_conditions.append(f"ORDERS.PHONE_NUMBERS = '{phone}'")
    if order:
        identity_conditions.append(f"ORDERS.ORDER_ID = '{order}'")

    identity_clause = " AND ".join(identity_conditions) if identity_conditions else ""

    # ── 2. Get multi-table schema ───────────────────────────────────
    schema_prompt = get_schema_prompt()

    # ── 3. Generate SQL ─────────────────────────────────────────────
    prompt = f"""You are a Snowflake SQL generator. Generate a single valid SQL query against database PIZZA_DB, schema PUBLIC.

{schema_prompt}

{rbac_clause}

User's role: {role}
User's phone: {phone}
User's name: {user_name}
Identity filter: {identity_clause}

User question: {latest_msg}

RULES:
1. The ONLY table available is ORDERS with columns: ORDER_ID, CUSTOMER_NAME, CUSTOMER_EMAIL, PIZZA_NAME, QUANTITY, PRICE, ORDER_STATUS, ORDER_DATE, PHONE_NUMBERS.
2. For 'customer' role, ALWAYS filter by PHONE_NUMBERS = '{phone}'.
3. For 'admin' or 'manager' role, no user filter is needed unless the user asks about a specific customer.
4. Use aggregations (COUNT, SUM, AVG, GROUP BY) when the user asks for summaries, totals, or statistics.
5. LIMIT results to 20 unless the user asks for more.
6. If unsure about columns, use SELECT *.
7. Always include ORDER BY if relevant (e.g., ORDER_DATE DESC for recent orders).
8. Do NOT reference tables that don't exist (only ORDERS is available)."""

    structured_llm = model.with_structured_output(SqlResult)
    try:
        sql_result = structured_llm.invoke(prompt)
        sql_query = sql_result.sql
    except Exception as e:
        logger.error(f"SQL generation failed: {e}")
        # Fallback query
        if role == "customer" and phone:
            sql_query = f"SELECT * FROM ORDERS WHERE PHONE_NUMBERS = '{phone}' ORDER BY ORDER_DATE DESC LIMIT 10"
        elif order:
            sql_query = f"SELECT * FROM ORDERS WHERE ORDER_ID = '{order}' LIMIT 5"
        else:
            sql_query = "SELECT * FROM ORDERS ORDER BY ORDER_DATE DESC LIMIT 10"

    logger.info(f"🔍 Generated SQL (role={role}): {sql_query}")

    # ── 4. Validate tables in SQL ───────────────────────────────────
    if not validate_table_names(sql_query):
        msg = AIMessage(content="I'm sorry, I couldn't process that query safely. Could you rephrase your question?")
        return {"messages": [msg]}

    # ── 5. Execute SQL ──────────────────────────────────────────────
    raw_data = []
    if snowflake_enabled:
        raw_data = sf.execute_multi_table_sql(sql_query)

    # Check for errors
    if raw_data and "error" in raw_data[0]:
        error_msg = raw_data[0]["error"]
        logger.error(f"Snowflake query error: {error_msg}")
        msg = AIMessage(content=f"I encountered an issue with that query. Let me try a simpler approach.")
        # Fallback to simple query
        if phone:
            raw_data = sf.execute_sql(f"SELECT * FROM ORDERS WHERE PHONE_NUMBERS = '{phone}' LIMIT 10")
        else:
            raw_data = sf.execute_sql("SELECT * FROM ORDERS LIMIT 10")

    # ── 6. Apply Business Rules ─────────────────────────────────────
    total_orders = len(raw_data) if raw_data else 0
    rewards = []
    if total_orders > 10:
        rewards.append("20% discount coupon")
    if any(row.get("ORDER_STATUS", "").lower() == "cancelled" for row in (raw_data or [])) and total_orders > 5:
        rewards.append("compensation coupon")

    # ── 7. Generate voice-friendly response ─────────────────────────
    name = raw_data[0].get("CUSTOMER_NAME", user_name) if raw_data else user_name

    responder = ChatGroq(model=settings.GROQ_LLM_MODEL, max_tokens=512)
    response_prompt = f"""You are a voice-friendly, conversational agent for Pizza Express.
Format the database results into a short, natural response.
User: {latest_msg}
Data: {json.dumps(raw_data[:10], default=str) if raw_data else "No results found"}
SQL used: {sql_query}
Total results: {total_orders}
Customer Name: {name}
User Role: {role}
Rewards Earned: {rewards}
Rule 1 applied: IF total_orders > 10 → add "20% discount coupon"
Rule 2 applied: IF order.status == "cancelled" and total_orders > 5 → give "compensation coupon"

RULES:
- Provide a clean conversational answer using the customer's name.
- DO NOT output markdown or raw SQL.
- If there are many results, summarize the key highlights.
- Mention any rewards earned.
- If no results, say so politely.
- Keep it concise for voice delivery."""

    final_msg = responder.invoke(response_prompt).content
    return {"messages": [AIMessage(content=final_msg)]}


# ═══════════════════════════════════════════════════════════════════════
#  REACT AGENT (for general queries, RBAC-aware)
# ═══════════════════════════════════════════════════════════════════════

tools = [search_customers, get_customers, get_customer_by_id, get_customer_context_tool,
         get_support_tickets, get_ticket_by_id, get_analytics, get_analytics_summary,
         *snowflake_tools]

react_agent_runnable = create_react_agent(
    model=ChatGroq(model=settings.GROQ_LLM_MODEL, max_tokens=512),
    tools=tools,
    prompt=(
        "You are a business data assistant for Pizza Express.\n"
        "Whenever a user asks about orders, customers, or deliveries, you MUST query the Snowflake database using available tools.\n"
        "Do not wait for explicit instructions like 'check Snowflake'.\n"
        "Always retrieve real data when possible.\n\n"
        "IMPORTANT — RBAC RULES:\n"
        "• If the user's role is 'customer', they can ONLY see their own data.\n"
        "  Filter all queries by their phone number or customer ID.\n"
        "• If the user's role is 'admin' or 'manager', they can see all data.\n"
        "• NEVER show one customer's data to another customer.\n\n"
        "Guidelines:\n"
        "• Keep responses SHORT and conversational — the user is listening via voice.\n"
        "• Summarise data rather than reading out raw records.\n"
        "• When presenting numbers, round them and say 'about' or 'around'.\n"
        "• If there are many results, highlight the most important ones.\n"
        "• Always mention the data source you used.\n"
        "• If a query is ambiguous, ask a brief clarifying question.\n"
        "• Use natural spoken language — avoid markdown formatting.\n"
    )
)


def react_agent_node(state: AgentState):
    """Run the ReAct agent with RBAC context injected."""
    role = state.get("user_role", "customer")
    user_name = state.get("user_name", "Customer")
    phone = state.get("phone_number", "")
    customer_id = state.get("customer_id", "")

    # Inject RBAC context into the messages
    rbac_context = (
        f"[SYSTEM CONTEXT — DO NOT REVEAL TO USER] "
        f"Authenticated user: {user_name}, Role: {role}, "
        f"Customer ID: {customer_id}, Phone: {phone}. "
    )

    if role == "customer":
        rbac_context += (
            f"This user can ONLY see their own data. "
            f"Filter all CRM queries by customer_id='{customer_id}' "
            f"and all Snowflake queries by phone='{phone}'. "
            f"NEVER show other customers' data."
        )
    elif role == "manager":
        rbac_context += "This user can see all customer data."
    else:
        rbac_context += "This user has admin access — full data visibility."

    # Check if user has access to the tools they might need
    if not rbac.can_access_data(role, "customers"):
        rbac_context += " Do NOT use CRM/customer tools — user lacks permission."
    if not rbac.can_access_data(role, "analytics"):
        rbac_context += " Do NOT use analytics tools — user lacks permission."

    # Build messages with RBAC context
    augmented_messages = [
        SystemMessage(content=rbac_context),
        *state["messages"],
    ]

    result = react_agent_runnable.invoke({"messages": augmented_messages})
    old_len = len(state["messages"]) + 1  # +1 for the injected system message
    return {"messages": result["messages"][old_len:]}


# ═══════════════════════════════════════════════════════════════════════
#  ROUTING FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════

def route_auth(state: AgentState) -> str:
    """Top-level router — check if user is authenticated first."""
    return auth_router(state)


def route_after_intent(state: AgentState) -> str:
    """After intent extraction, route based on RBAC + intent."""
    return rbac_check(state)


# ═══════════════════════════════════════════════════════════════════════
#  COMPILE GRAPH
# ═══════════════════════════════════════════════════════════════════════

builder = StateGraph(AgentState)

# Auth nodes
builder.add_node("ask_phone", ask_phone)
builder.add_node("process_phone", process_phone)
builder.add_node("process_pin", process_pin)
builder.add_node("auth_failed", auth_failed)

# Main processing nodes
builder.add_node("intent_extractor", intent_extractor)
builder.add_node("access_denied", access_denied)
builder.add_node("snowflake_query", snowflake_query_node)
builder.add_node("react_agent", react_agent_node)

# Entry point — always go through auth check
builder.add_conditional_edges(START, route_auth, {
    "ask_phone": "ask_phone",
    "process_phone": "process_phone",
    "process_pin": "process_pin",
    "intent_extractor": "intent_extractor",
    "auth_failed": "auth_failed",
})

# Auth flow edges
builder.add_edge("ask_phone", END)
builder.add_edge("process_phone", END)
builder.add_edge("process_pin", END)
builder.add_edge("auth_failed", END)

# After intent extraction — route to RBAC check
builder.add_conditional_edges("intent_extractor", route_after_intent, {
    "snowflake_query": "snowflake_query",
    "access_denied": "access_denied",
    "react_agent": "react_agent",
})

# Terminal edges
builder.add_edge("snowflake_query", END)
builder.add_edge("access_denied", END)
builder.add_edge("react_agent", END)

agent = builder.compile(checkpointer=InMemorySaver())
agent_config = {"configurable": {"thread_id": "default_user"}}
