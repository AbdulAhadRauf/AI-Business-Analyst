"""
Tests for voice authentication and RBAC.
"""

import pytest
from app.auth.user_db import (
    authenticate_by_phone_pin,
    get_user_by_phone,
    get_user_by_customer_id,
    get_all_users,
)
from app.auth.rbac import RBACManager


# ═══════════════════════════════════════════════════════════════════════
#  User DB Authentication Tests
# ═══════════════════════════════════════════════════════════════════════

class TestUserDB:
    """Test the dummy user database authentication."""

    def test_authenticate_valid_admin(self):
        """Ahad (admin) authenticates with correct phone and PIN."""
        user = authenticate_by_phone_pin("1234567890", "1111")
        assert user is not None
        assert user["name"] == "Ahad"
        assert user["role"] == "admin"
        assert user["customer_id"] == "C001"
        # PIN should NOT be in the returned dict
        assert "pin" not in user

    def test_authenticate_valid_manager(self):
        """Rahul (manager) authenticates with correct phone and PIN."""
        user = authenticate_by_phone_pin("0987654321", "2222")
        assert user is not None
        assert user["name"] == "Rahul"
        assert user["role"] == "manager"

    def test_authenticate_valid_customer(self):
        """John (customer) authenticates with correct phone and PIN."""
        user = authenticate_by_phone_pin("5555555555", "3333")
        assert user is not None
        assert user["name"] == "John"
        assert user["role"] == "customer"
        assert user["customer_id"] == "C003"

    def test_authenticate_wrong_pin(self):
        """Correct phone but wrong PIN should fail."""
        user = authenticate_by_phone_pin("1234567890", "9999")
        assert user is None

    def test_authenticate_unknown_phone(self):
        """Unknown phone number should fail."""
        user = authenticate_by_phone_pin("0000000000", "1111")
        assert user is None

    def test_authenticate_with_formatted_phone(self):
        """Phone numbers with formatting should still work."""
        user = authenticate_by_phone_pin("123-456-7890", "1111")
        assert user is not None
        assert user["name"] == "Ahad"

    def test_authenticate_with_parentheses(self):
        """Phone numbers with parentheses should still work."""
        user = authenticate_by_phone_pin("(123) 456-7890", "1111")
        assert user is not None
        assert user["name"] == "Ahad"

    def test_get_user_by_phone_found(self):
        """Look up user by phone (no PIN needed)."""
        user = get_user_by_phone("1234567890")
        assert user is not None
        assert user["name"] == "Ahad"
        assert "pin" not in user

    def test_get_user_by_phone_not_found(self):
        """Look up non-existent phone."""
        user = get_user_by_phone("0000000000")
        assert user is None

    def test_get_user_by_customer_id(self):
        """Look up user by customer_id."""
        user = get_user_by_customer_id("C002")
        assert user is not None
        assert user["name"] == "Rahul"

    def test_get_all_users(self):
        """All users returned without PINs."""
        users = get_all_users()
        assert len(users) >= 5
        for u in users:
            assert "pin" not in u
            assert "name" in u
            assert "role" in u


# ═══════════════════════════════════════════════════════════════════════
#  RBAC Tests
# ═══════════════════════════════════════════════════════════════════════

class TestRBAC:
    """Test role-based access control."""

    def setup_method(self):
        self.rbac = RBACManager()

    def test_admin_can_access_all(self):
        """Admin has wildcard access."""
        assert self.rbac.can_access_data("admin", "customers") is True
        assert self.rbac.can_access_data("admin", "snowflake_orders") is True
        assert self.rbac.can_access_data("admin", "analytics") is True
        assert self.rbac.can_access_data("admin", "support_tickets") is True
        assert self.rbac.can_access_data("admin", "anything_else") is True

    def test_manager_access(self):
        """Manager can access CRM, support, analytics, snowflake."""
        assert self.rbac.can_access_data("manager", "customers") is True
        assert self.rbac.can_access_data("manager", "support_tickets") is True
        assert self.rbac.can_access_data("manager", "analytics") is True
        assert self.rbac.can_access_data("manager", "snowflake_orders") is True

    def test_customer_limited_access(self):
        """Customer can only access snowflake_orders and support_tickets."""
        assert self.rbac.can_access_data("customer", "snowflake_orders") is True
        assert self.rbac.can_access_data("customer", "support_tickets") is True
        assert self.rbac.can_access_data("customer", "customers") is False
        assert self.rbac.can_access_data("customer", "analytics") is False

    def test_admin_can_see_all_users(self):
        """Admin can see all users' data."""
        assert self.rbac.can_see_all_users("admin") is True

    def test_manager_can_see_all_users(self):
        """Manager can see all users' data."""
        assert self.rbac.can_see_all_users("manager") is True

    def test_customer_cannot_see_all_users(self):
        """Customer CANNOT see other users' data."""
        assert self.rbac.can_see_all_users("customer") is False

    def test_snowflake_filter_admin(self):
        """Admin gets no filter (sees all data)."""
        f = self.rbac.get_snowflake_filter("admin", phone="1234567890")
        assert f == ""

    def test_snowflake_filter_customer(self):
        """Customer gets a WHERE clause scoped to their phone."""
        f = self.rbac.get_snowflake_filter("customer", phone="5555555555")
        assert "PHONE_NUMBERS = '5555555555'" in f

    def test_snowflake_filter_customer_no_identity(self):
        """Customer with no identifier gets '1=0' (no access)."""
        f = self.rbac.get_snowflake_filter("customer")
        assert f == "1=0"

    def test_crm_filter_admin(self):
        """Admin gets no CRM filter."""
        f = self.rbac.get_crm_filter("admin", customer_id="C001")
        assert f is None

    def test_crm_filter_customer(self):
        """Customer gets their customer_id as CRM filter."""
        f = self.rbac.get_crm_filter("customer", customer_id="C003")
        assert f == "C003"

    def test_unknown_role_defaults_to_customer(self):
        """Unknown role should default to customer permissions."""
        assert self.rbac.can_access_data("unknown_role", "snowflake_orders") is True
        assert self.rbac.can_access_data("unknown_role", "analytics") is False

    def test_describe_role(self):
        """Role description should return a string."""
        desc = self.rbac.describe_role("admin")
        assert isinstance(desc, str)
        assert len(desc) > 0

    def test_describe_unknown_role(self):
        desc = self.rbac.describe_role("nonexistent")
        assert "Unknown role" in desc


# ═══════════════════════════════════════════════════════════════════════
#  RBAC with Config Override Tests
# ═══════════════════════════════════════════════════════════════════════

class TestRBACWithConfig:
    """Test RBAC with custom config (simulating business_config.yaml)."""

    def test_custom_roles(self):
        """RBAC should accept custom role definitions from config."""
        config = {
            "roles": {
                "viewer": {
                    "data_sources": ["analytics"],
                    "can_see_all_users": False,
                },
                "super_admin": {
                    "data_sources": ["*"],
                    "can_see_all_users": True,
                },
            }
        }
        rbac = RBACManager(config)
        assert rbac.can_access_data("viewer", "analytics") is True
        assert rbac.can_access_data("viewer", "customers") is False
        assert rbac.can_access_data("super_admin", "anything") is True
