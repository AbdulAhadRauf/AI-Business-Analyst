"""
Role-Based Access Control (RBAC) — controls who can access what data.

Roles:
    admin    — full access to everything, can see all users' data
    manager  — access to CRM, support, analytics, Snowflake; can see all users
    customer — access only to their own orders and support tickets

Usage:
    from app.auth.rbac import RBACManager

    rbac = RBACManager()
    if rbac.can_access_data("customer", "snowflake_orders"):
        # allowed
    filter_clause = rbac.get_snowflake_filter("customer", phone="1234567890")
    # → "PHONE_NUMBERS = '1234567890'"
"""

import logging
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


# ── Role Definitions ───────────────────────────────────────────────────

ROLE_PERMISSIONS: Dict[str, Dict[str, Any]] = {
    "admin": {
        "data_sources": {"*"},  # all data sources
        "can_see_all_users": True,
        "description": "Full access to all data and all users' records",
    },
    "manager": {
        "data_sources": {
            "customers", "support_tickets", "analytics", "snowflake_orders",
        },
        "can_see_all_users": True,
        "description": "Access to CRM, support, analytics, and Snowflake for all customers",
    },
    "customer": {
        "data_sources": {
            "snowflake_orders", "support_tickets",
        },
        "can_see_all_users": False,
        "description": "Access only to own orders and support tickets",
    },
}


class RBACManager:
    """
    Manages role-based access control for data sources.

    Determines what data a user can access based on their role,
    and generates SQL filter clauses for row-level security.
    """

    def __init__(self, role_config: Optional[Dict[str, Any]] = None):
        """
        Initialize with optional config override from business_config.yaml.
        Falls back to built-in ROLE_PERMISSIONS.
        """
        if role_config and role_config.get("roles"):
            self._roles = {}
            for role_name, role_def in role_config["roles"].items():
                ds = role_def.get("data_sources", [])
                self._roles[role_name] = {
                    "data_sources": set(ds) if isinstance(ds, list) else {ds},
                    "can_see_all_users": role_def.get("can_see_all_users", False),
                }
        else:
            self._roles = ROLE_PERMISSIONS

    def get_allowed_sources(self, role: str) -> Set[str]:
        """Get the set of data source names this role can access."""
        perm = self._roles.get(role, self._roles.get("customer", {}))
        sources = perm.get("data_sources", set())
        return sources

    def can_access_data(self, role: str, data_source: str) -> bool:
        """Check if a role can access a specific data source."""
        allowed = self.get_allowed_sources(role)
        if "*" in allowed:
            return True
        return bool(data_source in allowed)

    def can_see_all_users(self, role: str) -> bool:
        """Check if a role can view other users' data."""
        perm = self._roles.get(role, self._roles.get("customer", {}))
        return perm.get("can_see_all_users", False)

    def get_snowflake_filter(
        self,
        role: str,
        *,
        phone: str = "",
        customer_id: str = "",
        customer_name: str = "",
    ) -> str:
        """
        Generate a SQL WHERE clause for row-level security in Snowflake.

        Admin/Manager: no filter (sees all rows)
        Customer: filter to only their own records by phone/customer_id
        """
        if self.can_see_all_users(role):
            return ""  # no filter — sees all data

        # Customer-level scoping
        conditions = []
        if phone:
            conditions.append(f"PHONE_NUMBERS = '{phone}'")
        if customer_name:
            conditions.append(f"CUSTOMER_NAME = '{customer_name}'")

        if conditions:
            return " OR ".join(conditions)

        # Fallback: if no identifying info, block access
        logger.warning(
            "RBAC: role=%s has no identifier for scoping — returning impossible filter",
            role,
        )
        return "1=0"  # returns no rows

    def get_crm_filter(
        self,
        role: str,
        customer_id: str = "",
    ) -> Optional[str]:
        """
        Generate a filter for CRM / support ticket queries.

        Admin/Manager: None (no filter)
        Customer: filter to their customer_id
        """
        if self.can_see_all_users(role):
            return None

        if customer_id:
            return customer_id

        return None

    def describe_role(self, role: str) -> str:
        """Human-readable description of what a role can do."""
        perm = self._roles.get(role)
        if perm is None:
            return f"Unknown role: {role}"

        desc = perm.get("description", "")
        if desc:
            return desc

        sources = perm.get("data_sources", set())
        all_users = perm.get("can_see_all_users", False)
        return str(
            f"Role '{role}': access to {sources}, "
            f"{'can' if all_users else 'cannot'} see other users' data"
        )


# ── Singleton ──────────────────────────────────────────────────────────

_rbac: Optional[RBACManager] = None


def get_rbac_manager(config: Optional[Dict[str, Any]] = None) -> RBACManager:
    """Get or create the global RBAC manager."""
    global _rbac
    if _rbac is None:
        _rbac = RBACManager(config)
    return _rbac
