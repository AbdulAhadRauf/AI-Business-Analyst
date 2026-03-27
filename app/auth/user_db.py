"""
Dummy User Database — in-memory user store for voice authentication.

Each user has a phone number, PIN, role, and customer_id.
The agent asks the caller for their phone number and PIN via voice,
then calls authenticate_by_phone_pin() to verify identity.

Roles:
    admin    — full access to all data, all users' records
    manager  — access to CRM, support, analytics, Snowflake for all customers
    customer — access ONLY to their own orders and support tickets
"""

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


# ── Dummy Users ────────────────────────────────────────────────────────
# In production, this would be a real database (Postgres, DynamoDB, etc.)

USERS_DB: List[Dict[str, Any]] = [
    {
        "user_id": "U001",
        "name": "Ahad",
        "phone": "1234567890",
        "pin": "1111",
        "role": "admin",
        "customer_id": "C001",
        "email": "ahad@pizzaexpress.com",
    },
    {
        "user_id": "U002",
        "name": "Rahul",
        "phone": "0987654321",
        "pin": "2222",
        "role": "manager",
        "customer_id": "C002",
        "email": "rahul@pizzaexpress.com",
    },
    {
        "user_id": "U003",
        "name": "John",
        "phone": "5555555555",
        "pin": "3333",
        "role": "customer",
        "customer_id": "C003",
        "email": "john@example.com",
    },
    {
        "user_id": "U004",
        "name": "Sarah",
        "phone": "4444444444",
        "pin": "4444",
        "role": "customer",
        "customer_id": "C004",
        "email": "sarah@example.com",
    },
    {
        "user_id": "U005",
        "name": "Mike",
        "phone": "6666666666",
        "pin": "5555",
        "role": "customer",
        "customer_id": "C005",
        "email": "mike@example.com",
    },
]


def _normalize_phone(phone: str) -> str:
    """Strip formatting from phone numbers for comparison."""
    import re
    return re.sub(r'[\s\-\(\)\.\+]', '', phone.strip())


def authenticate_by_phone_pin(
    phone: str, pin: str
) -> Optional[Dict[str, Any]]:
    """
    Authenticate a user by phone number and PIN.

    Returns the user record (without the PIN) if credentials match,
    or None if authentication fails.
    """
    normalized_phone = _normalize_phone(phone)
    normalized_pin = pin.strip()

    for user in USERS_DB:
        if _normalize_phone(user["phone"]) == normalized_phone:
            if user["pin"] == normalized_pin:
                # Return user record WITHOUT the PIN
                safe_user = {k: v for k, v in user.items() if k != "pin"}
                logger.info(
                    "Auth success: user=%s role=%s customer_id=%s",
                    safe_user["name"], safe_user["role"], safe_user["customer_id"],
                )
                return safe_user
            else:
                logger.warning(
                    "Auth failed: phone=%s (wrong PIN)", normalized_phone
                )
                return None

    logger.warning("Auth failed: phone=%s (not found)", normalized_phone)
    return None


def get_user_by_phone(phone: str) -> Optional[Dict[str, Any]]:
    """Look up a user by phone number (without PIN check)."""
    normalized = _normalize_phone(phone)
    for user in USERS_DB:
        if _normalize_phone(user["phone"]) == normalized:
            return {k: v for k, v in user.items() if k != "pin"}
    return None


def get_user_by_customer_id(customer_id: str) -> Optional[Dict[str, Any]]:
    """Look up a user by their customer_id."""
    for user in USERS_DB:
        if user["customer_id"] == customer_id:
            return {k: v for k, v in user.items() if k != "pin"}
    return None


def get_all_users() -> List[Dict[str, Any]]:
    """Return all users (without PINs) — for admin use."""
    return [{k: v for k, v in u.items() if k != "pin"} for u in USERS_DB]
