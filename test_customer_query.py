import asyncio
import json
from app.main import customer_query, CustomerQueryRequest
from app.services.crm_service import MOCK_CUSTOMERS, MOCK_ORDERS

async def test():
    print("Testing customer query endpoint locally without full FastAPI server...")
    
    # Test 1: Ahad with > 10 orders (Reward Eligibility rule)
    req1 = CustomerQueryRequest(
        phone_number="1234567890",
        query="Where is my order?"
    )
    res1 = await customer_query(req1)
    print("\nTest 1 (Ahad - >10 orders):")
    print(res1["response"])

    # Test 2: Rahul with cancelled order and > 5 orders (Cancellation Benefit)
    req2 = CustomerQueryRequest(
        customer_name="Rahul",
        query="What is my status?"
    )
    res2 = await customer_query(req2)
    print("\nTest 2 (Rahul - Cancelled):")
    print(res2["response"])

if __name__ == "__main__":
    asyncio.run(test())
