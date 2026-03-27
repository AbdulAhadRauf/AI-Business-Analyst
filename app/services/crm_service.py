"""
Mock CRM Service for Customer Identity and Order lookup.
"""

from typing import Optional, List, Dict, Any

# Mock databases
MOCK_CUSTOMERS = [
    {"customer_id": "C001", "name": "Ahad", "phone": "1234567890", "email": "ahad@pizzaexpress.com", "role": "admin"},
    {"customer_id": "C002", "name": "John", "phone": "0987654321", "email": "john@pizzaexpress.com", "role": "manager"},
    {"customer_id": "C004", "name": "Sarah", "phone": "4444444444", "email": "sarah@example.com", "role": "customer"},
    {"customer_id": "C005", "name": "Mike", "phone": "6666666666", "email": "mike@example.com", "role": "customer"},
    {"customer_id": "C001", "name": "Rahul Sharma", "phone": "123", "email": "rahul@example.com", "role": "customer"},
    {"customer_id": "C002", "name": "Priya Verma", "phone": "234", "email": "priya@example.com", "role": "customer"},
    {"customer_id": "C003", "name": "Amit Singh", "phone": "345", "email": "amit@example.com", "role": "customer"},
    {"customer_id": "C004", "name": "Sneha Gupta", "phone": "456", "email": "sneha@example.com", "role": "customer"},
    {"customer_id": "C005", "name": "Arjun Mehta", "phone": "567", "email": "arjun@example.com", "role": "customer"}
]

MOCK_ORDERS = [
    {"order_id": "ORD-100", "customer_id": "C001", "status": "Delivered", "item": "Margherita"},
    {"order_id": "ORD-101", "customer_id": "C002", "status": "Pending", "item": "Farmhouse"},
    {"order_id": "ORD-102", "customer_id": "C003", "status": "Cancelled", "item": "Pepperoni"},
    {"order_id": "ORD-103", "customer_id": "C004", "status": "Delivered", "item": "Veggie Delight"},
    {"order_id": "ORD-104", "customer_id": "C005", "status": "Pending", "item": "BBQ Chicken"}
]

# Adding more orders to reach thresholds for rules (e.g. C001 has 11 orders to trigger reward)
for i in range(105, 115):
    MOCK_ORDERS.append({"order_id": f"ORD-{i}", "customer_id": "C001", "status": "delivered", "item": "Pizza"})

for i in range(115, 122):
    MOCK_ORDERS.append({"order_id": f"ORD-{i}", "customer_id": "C002", "status": "delivered", "item": "Pasta"})


def get_customer_by_phone(phone_number: str) -> Optional[Dict[str, Any]]:
    for c in MOCK_CUSTOMERS:
        if c["phone"] == phone_number:
            return c
    return None

def get_order_by_id(order_id: str) -> Optional[Dict[str, Any]]:
    for o in MOCK_ORDERS:
        if o["order_id"] == order_id:
            return o
    return None

def get_customer_orders(customer_id: str) -> List[Dict[str, Any]]:
    return [o for o in MOCK_ORDERS if o["customer_id"] == customer_id]
