"""
Tests for the Advanced Business Rules Engine.

Tests all 9 rule categories:
    1. Customer Segmentation
    2. Pricing & Discounts
    3. Delivery & Operations
    4. Cancellation & Refund
    5. Personalization
    6. Risk & Fraud
    7. Support Automation
    8. Loyalty & Gamification
    9. Context-Aware AI
"""

import pytest
from app.services.business_rules import (
    RulesEngine,
    RulesResult,
    apply_customer_rules,
    sort_by_priority,
    generate_context_string,
    freshness_label,
)


@pytest.fixture
def engine():
    return RulesEngine()


@pytest.fixture
def base_customer():
    return {"customer_id": "C001", "name": "Ahad", "phone": "1234567890"}


def make_orders(count, status="delivered", item="Pizza", price=500):
    """Helper to generate mock order lists."""
    return [
        {"order_id": f"ORD-{i}", "status": status, "item": item, "price": price}
        for i in range(count)
    ]


# ═══════════════════════════════════════════════════════════════════════
#  1. Customer Segmentation Tests
# ═══════════════════════════════════════════════════════════════════════

class TestCustomerSegmentation:
    def test_vip_customer(self, engine, base_customer):
        """VIP: total_orders > 20 AND total_spent > 10000."""
        orders = make_orders(25, price=500)
        result = engine.evaluate(base_customer, orders)
        assert result.customer_tier == "VIP"
        assert "priority_support" in result.benefits
        assert "vip_customer" in result.triggered_rules

    def test_frequent_customer(self, engine, base_customer):
        """Frequent: orders_last_30_days >= 5 (no dates → check fallback)."""
        orders = make_orders(3)
        result = engine.evaluate(base_customer, orders)
        # Without dates, orders_last_30_days = 0, so not triggered
        assert "frequent_customer" not in result.triggered_rules

    def test_loyal_customer(self, engine, base_customer):
        """Loyal: total_orders > 10 but under VIP threshold."""
        orders = make_orders(15, price=100)  # 1500 total, under 10000
        result = engine.evaluate(base_customer, orders)
        assert result.customer_tier == "Loyal"
        assert "loyal_customer" in result.triggered_rules

    def test_regular_customer(self, engine, base_customer):
        """Regular: no special segmentation rules triggered."""
        orders = make_orders(3)
        result = engine.evaluate(base_customer, orders)
        assert result.customer_tier == "Regular"


# ═══════════════════════════════════════════════════════════════════════
#  2. Pricing & Discounts Tests
# ═══════════════════════════════════════════════════════════════════════

class TestPricingDiscounts:
    def test_high_value_order_discount(self, engine, base_customer):
        """10% discount for orders > 1000 value."""
        orders = [{"order_id": "ORD-1", "status": "delivered", "item": "Pizza", "price": 1500}]
        result = engine.evaluate(base_customer, orders)
        assert any("10%" in d for d in result.discounts)
        assert "high_value_order_discount" in result.triggered_rules

    def test_first_order_offer(self, engine, base_customer):
        """20% welcome coupon for first order."""
        orders = make_orders(1)
        result = engine.evaluate(base_customer, orders)
        assert any("welcome" in d.lower() for d in result.discounts)
        assert "first_order_offer" in result.triggered_rules

    def test_bulk_order_loyalty(self, engine, base_customer):
        """Loyalty discount for > 10 orders."""
        orders = make_orders(12)
        result = engine.evaluate(base_customer, orders)
        assert any("loyalty" in d.lower() for d in result.discounts)
        assert "bulk_order_discount" in result.triggered_rules

    def test_no_discount_for_regular(self, engine, base_customer):
        """No high-value discount for normal orders."""
        orders = make_orders(3, price=50)
        result = engine.evaluate(base_customer, orders)
        assert "high_value_order_discount" not in result.triggered_rules


# ═══════════════════════════════════════════════════════════════════════
#  3. Delivery & Operations Tests
# ═══════════════════════════════════════════════════════════════════════

class TestDeliveryOperations:
    def test_delayed_order_escalation(self, engine, base_customer):
        """Delayed > 20 min → escalation + free delivery."""
        orders = [{"order_id": "ORD-1", "status": "delayed", "delay_minutes": 25, "price": 100}]
        result = engine.evaluate(base_customer, orders)
        assert result.escalate_to_support is True
        assert "delayed_order_compensation" in result.triggered_rules

    def test_no_escalation_for_short_delay(self, engine, base_customer):
        """Delay < 20 min → no escalation."""
        orders = [{"order_id": "ORD-1", "status": "delayed", "delay_minutes": 10, "price": 100}]
        result = engine.evaluate(base_customer, orders)
        assert result.escalate_to_support is False


# ═══════════════════════════════════════════════════════════════════════
#  4. Cancellation & Refund Tests
# ═══════════════════════════════════════════════════════════════════════

class TestCancellationRefund:
    def test_early_cancellation_full_refund(self, engine, base_customer):
        """Placed order → 100% refund."""
        orders = make_orders(3)
        result = engine.evaluate(base_customer, orders, context={"cancel_order_status": "placed"})
        assert result.refund_percentage == 100
        assert "early_cancellation" in result.triggered_rules

    def test_late_cancellation_partial_refund(self, engine, base_customer):
        """Preparing order → 80% refund."""
        orders = make_orders(3)
        result = engine.evaluate(base_customer, orders, context={"cancel_order_status": "preparing"})
        assert result.refund_percentage == 80
        assert "late_cancellation" in result.triggered_rules

    def test_no_cancellation_for_delivered(self, engine, base_customer):
        """Delivered order → 0% refund."""
        orders = make_orders(3)
        result = engine.evaluate(base_customer, orders, context={"cancel_order_status": "delivered"})
        assert result.refund_percentage == 0
        assert "no_cancellation" in result.triggered_rules

    def test_cancellation_compensation(self, engine, base_customer):
        """Past cancellations + many orders → compensation coupon."""
        orders = make_orders(5) + make_orders(2, status="cancelled")
        result = engine.evaluate(base_customer, orders)
        assert "cancellation_compensation" in result.triggered_rules


# ═══════════════════════════════════════════════════════════════════════
#  5. Personalization Tests
# ═══════════════════════════════════════════════════════════════════════

class TestPersonalization:
    def test_favorite_item_suggestion(self, engine, base_customer):
        """Should suggest the most-ordered item."""
        orders = make_orders(5, item="Margherita") + make_orders(2, item="Pepperoni")
        result = engine.evaluate(base_customer, orders)
        assert any("Margherita" in s for s in result.suggestions)
        assert "favorite_product_suggestion" in result.triggered_rules

    def test_no_suggestion_without_items(self, engine, base_customer):
        """No suggestion if orders have no item field."""
        orders = [{"order_id": "ORD-1", "status": "delivered", "price": 100}]
        result = engine.evaluate(base_customer, orders)
        assert "favorite_product_suggestion" not in result.triggered_rules


# ═══════════════════════════════════════════════════════════════════════
#  6. Risk & Fraud Tests
# ═══════════════════════════════════════════════════════════════════════

class TestRiskFraud:
    def test_high_cancellation_rate(self, engine, base_customer):
        """> 50% cancellation rate → restrict offers."""
        orders = make_orders(5, status="cancelled") + make_orders(4, status="delivered")  # 10 orders, 5 cancelled
        result = engine.evaluate(base_customer, orders)
        # 5/9 > 0.5, but need 10+ orders
        assert "high_refund_frequency" not in result.triggered_rules  # under 10

        # With 10 orders, 6 cancelled
        orders = make_orders(6, status="cancelled") + make_orders(5, status="delivered")
        result = engine.evaluate(base_customer, orders)
        assert result.restrict_offers is True
        assert "high_refund_frequency" in result.triggered_rules


# ═══════════════════════════════════════════════════════════════════════
#  7. Support Automation Tests
# ═══════════════════════════════════════════════════════════════════════

class TestSupportAutomation:
    def test_auto_resolve_order_status(self, engine, base_customer):
        """Order status query → auto respond."""
        orders = make_orders(1)
        result = engine.evaluate(base_customer, orders, context={"query_type": "order_status"})
        assert result.auto_respond is True
        assert "auto_resolve_order_status" in result.triggered_rules

    def test_escalate_negative_sentiment(self, engine, base_customer):
        """Negative sentiment → escalate to human."""
        orders = make_orders(1)
        result = engine.evaluate(base_customer, orders, context={"sentiment": "negative"})
        assert result.escalate_to_human is True
        assert "escalate_angry_customer" in result.triggered_rules


# ═══════════════════════════════════════════════════════════════════════
#  8. Loyalty & Gamification Tests
# ═══════════════════════════════════════════════════════════════════════

class TestLoyaltyGamification:
    def test_milestone_10_orders(self, engine, base_customer):
        """10th order → free item reward."""
        orders = make_orders(10)
        result = engine.evaluate(base_customer, orders)
        assert result.milestone_reached == "10_orders"
        assert "milestone_reward_10" in result.triggered_rules

    def test_milestone_25_orders(self, engine, base_customer):
        """25th order → premium pizza."""
        orders = make_orders(25, price=100)  # Under VIP threshold
        result = engine.evaluate(base_customer, orders)
        assert result.milestone_reached == "25_orders"
        assert "milestone_reward_25" in result.triggered_rules

    def test_birthday_offer(self, engine):
        """Birthday → free dessert."""
        customer = {"customer_id": "C001", "name": "Ahad", "phone": "1234", "is_birthday": True}
        orders = make_orders(3)
        result = engine.evaluate(customer, orders)
        assert any("birthday" in r.lower() for r in result.rewards)
        assert "birthday_offer" in result.triggered_rules


# ═══════════════════════════════════════════════════════════════════════
#  9. Context-Aware AI Tests
# ═══════════════════════════════════════════════════════════════════════

class TestContextAwareAI:
    def test_missing_info_prompt(self, engine, base_customer):
        """No phone or order_id → ask user."""
        orders = make_orders(1)
        result = engine.evaluate(base_customer, orders, context={})
        assert result.ask_user is not None
        assert "missing_info" in result.triggered_rules

    def test_smart_tool_trigger(self, engine, base_customer):
        """Order status intent → use Snowflake."""
        orders = make_orders(1)
        result = engine.evaluate(base_customer, orders, context={
            "intent": "order_status", "phone_number": "1234567890",
        })
        assert result.use_snowflake is True
        assert "smart_tool_trigger" in result.triggered_rules


# ═══════════════════════════════════════════════════════════════════════
#  RulesResult Utilities
# ═══════════════════════════════════════════════════════════════════════

class TestRulesResult:
    def test_to_dict_empty(self):
        """Empty result should have minimal keys."""
        r = RulesResult()
        d = r.to_dict()
        assert d["customer_tier"] == "Regular"
        assert d["triggered_rules"] == []

    def test_to_prompt_context(self, engine, base_customer):
        """Prompt context should be a readable string."""
        orders = make_orders(12)
        result = engine.evaluate(base_customer, orders)
        ctx = result.to_prompt_context()
        assert isinstance(ctx, str)
        assert len(ctx) > 0


# ═══════════════════════════════════════════════════════════════════════
#  Legacy Backward Compatibility
# ═══════════════════════════════════════════════════════════════════════

class TestLegacyAPI:
    def test_apply_customer_rules_returns_enriched(self):
        """Legacy API should still return enriched context dict."""
        customer = {"customer_id": "C001", "name": "Ahad", "phone": "1234"}
        orders = make_orders(12)
        result = apply_customer_rules(customer, orders)
        assert "status" in result
        assert "rewards" in result
        assert "triggered_rules" in result
        assert "rules_context" in result

    def test_apply_customer_rules_has_suggestions(self):
        """Legacy API should include suggestions."""
        customer = {"customer_id": "C001", "name": "Ahad", "phone": "1234"}
        orders = make_orders(5, item="Margherita")
        result = apply_customer_rules(customer, orders)
        assert "suggestions" in result


# ═══════════════════════════════════════════════════════════════════════
#  Utility Function Tests
# ═══════════════════════════════════════════════════════════════════════

class TestUtilities:
    def test_sort_by_priority(self):
        data = [
            {"priority": "low", "id": 1},
            {"priority": "high", "id": 2},
            {"priority": "medium", "id": 3},
        ]
        sorted_data = sort_by_priority(data)
        assert sorted_data[0]["priority"] == "high"
        assert sorted_data[1]["priority"] == "medium"
        assert sorted_data[2]["priority"] == "low"

    def test_generate_context_string(self):
        ctx = generate_context_string("orders", 100, 10, filters={"status": "delivered"})
        assert "10 of 100" in ctx
        assert "status=delivered" in ctx

    def test_freshness_label(self):
        label = freshness_label()
        assert "Data as of" in label
