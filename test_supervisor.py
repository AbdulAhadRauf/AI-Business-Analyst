import asyncio
import json
from src.data_connector_agent import agent, agent_config

async def test_supervisor_agent():
    print("=== Testing Supervisor Agent Routing ===")

    # Test 1: Order intent without Identity
    print("\n[Turn 1] User asks about orders with NO identity.")
    res1 = await agent.ainvoke({"messages": [("user", "What did I order?")]}, config=agent_config)
    print("Agent Response:", res1["messages"][-1].content)

    # Test 2: Provide Identity
    print("\n[Turn 2] User provides phone number.")
    res2 = await agent.ainvoke({"messages": [("user", "My phone number is 1234567890")]}, config=agent_config)
    print("Agent Response:", res2["messages"][-1].content)

    # Test 3: Ask again (identity should be persisted)
    print("\n[Turn 3] User asks about orders again.")
    res3 = await agent.ainvoke({"messages": [("user", "Where is my order?")]}, config=agent_config)
    print("Agent Response:", res3["messages"][-1].content)

    # Test 4: General query -> React Agent Fallback
    print("\n[Turn 4] User asks a general question (CRM/Analytics).")
    res4 = await agent.ainvoke({"messages": [("user", "Tell me about analytics for support.")]}, config=agent_config)
    print("Agent Response:", res4["messages"][-1].content)

if __name__ == "__main__":
    asyncio.run(test_supervisor_agent())
