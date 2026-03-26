"""
Business rules — priority sorting, context strings, freshness labels.
"""

import logging
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

PRIORITY_ORDER = {"high": 0, "medium": 1, "low": 2}


def sort_by_priority(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Sort records with a 'priority' field: high > medium > low."""
    if not data or "priority" not in data[0]:
        return data
    return sorted(data, key=lambda r: PRIORITY_ORDER.get(r.get("priority", ""), 99))


def generate_context_string(
    source: str,
    total: int,
    returned: int,
    *,
    filters: Optional[Dict[str, Any]] = None,
) -> str:
    parts = [f"Showing {returned} of {total} {source} records"]
    if filters:
        active = {k: v for k, v in filters.items() if v is not None}
        if active:
            parts.append(f"(filtered by {', '.join(f'{k}={v}' for k, v in active.items())})")
    return " ".join(parts)


def freshness_label() -> str:
    return f"Data as of {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"


# ── Custom Business Rules Engine ───────────────────────────────────────

def apply_customer_rules(customer_data: Dict[str, Any], orders_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Evaluates customer data against business rules and returns an enriched context.
    """
    total_orders = len(orders_data)
    
    # Base context
    enriched_context = {
        "customer_id": customer_data.get("customer_id"),
        "name": customer_data.get("name"),
        "phone": customer_data.get("phone"),
        "total_orders": total_orders,
        "orders": orders_data,
        "last_order": orders_data[-1] if orders_data else None,
        "status": "Regular",
        "rewards": []
    }
    
    rewards = []
    
    # Rule 1: Reward Eligibility
    if total_orders > 10:
        rewards.append("20% discount coupon")
        
    # Rule 2: Cancellation Benefit
    if any(o.get("status") == "cancelled" for o in orders_data) and total_orders > 5:
        rewards.append("compensation coupon")
        
    enriched_context["rewards"] = rewards
    
    # Rule 3: Priority Customer
    if total_orders > 20:
        enriched_context["status"] = "VIP"
            
    return enriched_context
