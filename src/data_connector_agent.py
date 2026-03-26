"""
Data Connector Agent — LangGraph agent with business data tools.
All data access is delegated to app/connectors/.

Snowflake tools are conditionally registered only when
SNOWFLAKE_ACCOUNT is set in the environment.
"""

import json, sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from langchain_groq import ChatGroq
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.prebuilt import create_react_agent
from langgraph.graph import StateGraph, START, END
from langgraph.graph.message import add_messages
from typing import TypedDict, Annotated, List, Dict, Any, Literal
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

crm = CRMConnector()
support = SupportConnector()
analytics = AnalyticsConnector()


def _int(value: str, default: int) -> int:
    try:
        return int(value) if value and value.strip() else default
    except (ValueError, TypeError):
        return default


# ── CRM Tools ──────────────────────────────────────────────────────────

@tool
def search_customers(query: str, limit: str = "5") -> str:
    """Search CRM customers by name or email."""
    result = crm.search(query, limit=_int(limit, 5))
    return json.dumps(result)

@tool
def get_customers(status: str = "", limit: str = "5") -> str:
    """List CRM customers. Filter by status (active/inactive)."""
    result = crm.fetch(limit=_int(limit, 5), status=status or None)
    return json.dumps(result)

@tool
def get_customer_by_id(customer_id: str) -> str:
    """Get a specific customer by ID."""
    c = crm.get_by_id(_int(customer_id, 0))
    return json.dumps(c if c else {"error": f"Customer {customer_id} not found"})

@tool
def get_customer_context_tool(phone_number: str = "", order_id: str = "") -> str:
    """Identify customer across CRM using phone number or given order ID, returning business context, recent orders, and rewards."""
    customer = None
    if order_id:
        # User gave order ID, look it up in mock CRM orders
        order = get_order_by_id(order_id)
        if order:
            from app.services.crm_service import MOCK_CUSTOMERS
            for c in MOCK_CUSTOMERS:
                if c["customer_id"] == order["customer_id"]:
                    customer = c
                    break
    elif phone_number:
        # User gave phone number
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
    """Retrieve analytics metrics. Filter by metric name and time range (days)."""
    result = analytics.fetch(limit=_int(limit, 10), metric=metric or None, days=_int(days, 7))
    return json.dumps(result)

@tool
def get_analytics_summary(metric: str = "", days: str = "7") -> str:
    """Summarized analytics: average, min, max, trend. Best for voice."""
    return json.dumps(analytics.get_summary(metric=metric or None, days=_int(days, 7)))


# ── Snowflake Tools (conditional) ─────────────────────────────────────

snowflake_tools = []
snowflake_enabled = bool(settings.SNOWFLAKE_ACCOUNT)

if snowflake_enabled:
    from app.connectors.snowflake_connector import SnowflakeConnector

    sf = SnowflakeConnector(
        table=settings.SNOWFLAKE_DATABASE and "ORDERS" or "ORDERS",  # default table
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


# ── Supervisor Graph Definitions ───────────────────────────────────────

class AgentState(TypedDict):
    messages: Annotated[List[BaseMessage], add_messages]
    phone_number: str
    order_id: str
    intent: str
    
class IntentResult(BaseModel):
    intent: str = Field(description="'snowflake' if user asks about orders, deliveries, customer accounts, transactions. Else 'general'.")
    phone_number: str = Field(description="Extracted phone number if present in user message, else empty string.")
    order_id: str = Field(description="Extracted order ID if present in user message, else empty string.")

class SqlResult(BaseModel):
    sql: str = Field(description="A valid Snowflake SQL query")

def intent_extractor(state: AgentState):
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

def ask_identity(state: AgentState):
    msg = AIMessage(content="Can you please provide your phone number or order ID so I can look up your details?")
    return {"messages": [msg]}

def snowflake_query_node(state: AgentState):
    # 1. Translate natural language -> SQL
    model = ChatGroq(model=settings.GROQ_LLM_MODEL, max_tokens=1024, temperature=0)
    
    phone = state.get("phone_number", "")
    order = state.get("order_id", "")
    latest_msg = state["messages"][-1].content
    
    # Construct conditions
    conditions = []
    
    if phone:
        conditions.append(f"PHONE_NUMBERS = '{phone}'")
        
    if order:
        conditions.append(f"ORDER_ID = '{order}'")
        
    where_clause = " OR ".join(conditions) if conditions else "1=1"
    
    prompt = f"""You are a Snowflake SQL generator.
Table name: ORDERS
Fields: ORDER_ID, CUSTOMER_NAME, CUSTOMER_EMAIL, PIZZA_NAME, QUANTITY, PRICE, ORDER_STATUS, ORDER_DATE, PHONE_NUMBERS
User query: {latest_msg}
Condition constraints (Identity): WHERE {where_clause}

Output valid SQL query matching the user intent over these conditions. Use SELECT * if you are unsure about columns."""

    structured_llm = model.with_structured_output(SqlResult)
    try:
        sql_result = structured_llm.invoke(prompt)
        sql_query = sql_result.sql
    except Exception:
        sql_query = f"SELECT * FROM ORDERS WHERE {where_clause} LIMIT 5"
        
    # 2. Execute SQL
    raw_data = []
    if snowflake_enabled:
        raw_data = sf.execute_sql(sql_query)
        
    # 3. Apply Rules
    total_orders = len(raw_data)
    rewards = []
    if total_orders > 10:
        rewards.append("20% discount coupon")
    if any(row.get("ORDER_STATUS", "").lower() == "cancelled" for row in raw_data) and total_orders > 5:
        rewards.append("compensation coupon")
        
    # 4. Generate response
    name = raw_data[0].get("CUSTOMER_NAME", "Customer") if raw_data else "Customer"
    
    responder = ChatGroq(model=settings.GROQ_LLM_MODEL, max_tokens=512)
    response_prompt = f"""You are a voice-friendly, conversational agent.
Format the database results into a short, natural response.
User: {latest_msg}
Data: {raw_data}
Customer Name: {name}
Rewards Earned: {rewards}
Rule 1 applied: IF total_orders > 10 -> add "20% discount coupon"
Rule 2 applied: IF order.status == "cancelled" and total_orders > 5 -> give "compensation coupon"

Important: Provide a clean conversational answer using the customer's name and directly mentioning any rewards.
DO NOT output markdown."""

    final_msg = responder.invoke(response_prompt).content
    return {"messages": [AIMessage(content=final_msg)]}

# Build fallback ReAct agent
tools = [search_customers, get_customers, get_customer_by_id, get_customer_context_tool,
         get_support_tickets, get_ticket_by_id, get_analytics, get_analytics_summary,
         *snowflake_tools]

react_agent_runnable = create_react_agent(
    model=ChatGroq(model=settings.GROQ_LLM_MODEL, max_tokens=512),
    tools=tools,
    prompt=(
        "You are a business data assistant.\n"
        "Whenever a user asks about orders, customers, or deliveries, you MUST query the Snowflake database using available tools.\n"
        "Do not wait for explicit instructions like 'check Snowflake'.\n"
        "Always retrieve real data when possible.\n\n"
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
    result = react_agent_runnable.invoke({"messages": state["messages"]})
    old_len = len(state["messages"])
    # return only new messages appended by react agent
    return {"messages": result["messages"][old_len:]}

def route_intent(state: AgentState) -> str:
    if state["intent"] == "snowflake" and snowflake_enabled:
        if not state.get("phone_number") and not state.get("order_id"):
            return "ask_identity"
        return "snowflake_query"
    return "react_agent"

# Compile Graph
builder = StateGraph(AgentState)
builder.add_node("intent_extractor", intent_extractor)
builder.add_node("ask_identity", ask_identity)
builder.add_node("snowflake_query", snowflake_query_node)
builder.add_node("react_agent", react_agent_node)

builder.add_edge(START, "intent_extractor")
builder.add_conditional_edges("intent_extractor", route_intent, {
    "ask_identity": "ask_identity",
    "snowflake_query": "snowflake_query",
    "react_agent": "react_agent"
})

builder.add_edge("ask_identity", END)
builder.add_edge("snowflake_query", END)
builder.add_edge("react_agent", END)

agent = builder.compile(checkpointer=InMemorySaver())
agent_config = {"configurable": {"thread_id": "default_user"}}
