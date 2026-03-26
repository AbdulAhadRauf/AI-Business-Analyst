"""
FastAPI application entry point.
Mounts routers, sets up CORS and logging.

This is the REST API server. For the voice interface,
run src/fastrtc_data_stream.py instead.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import json

from app.routers import health, data
from app.utils.logging import configure_logging
from app.config import settings
from app.services.crm_service import MOCK_CUSTOMERS, get_customer_by_phone, get_customer_orders
from app.services.business_rules import apply_customer_rules

configure_logging()

app = FastAPI(
    title=settings.APP_NAME,
    description=(
        "A production-quality Universal Data Connector providing a unified "
        "interface for an LLM to access CRM, Support Ticket, and Analytics "
        "data through function calling. Includes a real-time voice conversation "
        "interface powered by FastRTC and Groq."
    ),
    version="1.0.0",
)

# ── CORS ────────────────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Routers ─────────────────────────────────────────────────────────────
app.include_router(health.router)
app.include_router(data.router)

# ── Customer Query Endpoint ─────────────────────────────────────────────

class CustomerQueryRequest(BaseModel):
    customer_name: str | None = None
    phone_number: str | None = None
    order_id: str | None = None
    query: str

@app.post("/customer/query", summary="Query using isolated customer context")
async def customer_query(req: CustomerQueryRequest):
    # 1. Identify customer
    customer = None
    if req.phone_number:
        customer = get_customer_by_phone(req.phone_number)
    elif req.customer_name:
        for c in MOCK_CUSTOMERS:
            if c["name"].lower() == req.customer_name.lower():
                customer = c
                break
                
    # 2. Apply rules and get context
    context_str = ""
    if customer:
        orders = get_customer_orders(customer["customer_id"])
        context = apply_customer_rules(customer, orders)
        context_str = f"SYSTEM INJECTED CONTEXT: The user is identified as {context['name']} (VIP Status: {context['status']}). They have {context['total_orders']} orders. Rewards available: {', '.join(context['rewards']) if context['rewards'] else 'None'}. Latest order: {context['last_order']}."
    else:
        context_str = "SYSTEM INJECTED CONTEXT: Caller is unidentified."
        
    # 3. Import agent from our standalone src agent (which is built for tools)
    from src.data_connector_agent import agent
    from langchain_core.messages import HumanMessage, SystemMessage
    
    # 4. Invoke agent with injected context
    human_content = f"{context_str}\n\nUSER QUERY:\n{req.query}"
    
    messages = [
        HumanMessage(content=human_content)
    ]
    
    import uuid
    thread_id = str(uuid.uuid4())
    
    result = await agent.ainvoke(
        {"messages": messages},
        config={"configurable": {"thread_id": thread_id}}
    )
    
    agent_response = result["messages"][-1].content
    return {"response": agent_response}
