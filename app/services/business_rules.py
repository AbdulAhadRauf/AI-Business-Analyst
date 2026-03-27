"""
Advanced Business Rules Engine — evaluates customer/order data against
configurable rules across 9 categories.

Categories:
    1. Customer Segmentation  (VIP, frequent buyer)
    2. Pricing & Discounts    (high-value, first-order, comeback)
    3. Delivery & Operations  (delayed compensation, escalation)
    4. Cancellation & Refund  (early vs late cancellation)
    5. Personalization        (favorite item, time-based suggestions)
    6. Risk & Fraud           (suspicious activity, refund abuse)
    7. Support Automation     (auto-resolve, sentiment escalation)
    8. Loyalty & Gamification (milestones, birthday offers)
    9. Context-Aware AI       (missing info, smart tool triggers)

Usage:
    from app.services.business_rules import RulesEngine

    engine = RulesEngine()
    result = engine.evaluate(customer, orders, context)
    # result -> RulesResult with all triggered rules
"""

import logging
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

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


# ═══════════════════════════════════════════════════════════════════════
#  Rules Result
# ═══════════════════════════════════════════════════════════════════════

@dataclass
class RulesResult:
    """Aggregated result of all triggered business rules."""
    # Customer Segmentation
    customer_tier: str = "Regular"
    customer_tags: List[str] = field(default_factory=list)
    benefits: List[str] = field(default_factory=list)

    # Pricing & Discounts
    discounts: List[str] = field(default_factory=list)
    offers: List[str] = field(default_factory=list)

    # Delivery & Operations
    delivery_actions: List[str] = field(default_factory=list)
    escalate_to_support: bool = False

    # Cancellation & Refund
    refund_percentage: Optional[int] = None
    cancellation_note: str = ""

    # Personalization
    suggestions: List[str] = field(default_factory=list)

    # Risk & Fraud
    fraud_flags: List[str] = field(default_factory=list)
    restrict_offers: bool = False

    # Support Automation
    auto_respond: bool = False
    escalate_to_human: bool = False

    # Loyalty & Gamification
    rewards: List[str] = field(default_factory=list)
    milestone_reached: Optional[str] = None

    # Context-Aware AI
    ask_user: Optional[str] = None
    use_snowflake: bool = False

    # Metadata
    triggered_rules: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict, omitting empty/None fields."""
        d = {}
        d["customer_tier"] = self.customer_tier
        if self.customer_tags:
            d["customer_tags"] = self.customer_tags
        if self.benefits:
            d["benefits"] = self.benefits
        if self.discounts:
            d["discounts"] = self.discounts
        if self.offers:
            d["offers"] = self.offers
        if self.delivery_actions:
            d["delivery_actions"] = self.delivery_actions
        if self.escalate_to_support:
            d["escalate_to_support"] = True
        if self.refund_percentage is not None:
            d["refund_percentage"] = self.refund_percentage
            d["cancellation_note"] = self.cancellation_note
        if self.suggestions:
            d["suggestions"] = self.suggestions
        if self.fraud_flags:
            d["fraud_flags"] = self.fraud_flags
        if self.restrict_offers:
            d["restrict_offers"] = True
        if self.auto_respond:
            d["auto_respond"] = True
        if self.escalate_to_human:
            d["escalate_to_human"] = True
        if self.rewards:
            d["rewards"] = self.rewards
        if self.milestone_reached:
            d["milestone_reached"] = self.milestone_reached
        if self.ask_user:
            d["ask_user"] = self.ask_user
        if self.use_snowflake:
            d["use_snowflake"] = True
        d["triggered_rules"] = self.triggered_rules
        return d

    def to_prompt_context(self) -> str:
        """Generate a text summary for injecting into LLM prompts."""
        lines = []
        lines.append(f"Customer Tier: {self.customer_tier}")
        if self.customer_tags:
            lines.append(f"Tags: {', '.join(self.customer_tags)}")
        if self.benefits:
            lines.append(f"Benefits: {', '.join(self.benefits)}")
        if self.discounts:
            lines.append(f"Discounts to mention: {', '.join(self.discounts)}")
        if self.offers:
            lines.append(f"Offers to mention: {', '.join(self.offers)}")
        if self.rewards:
            lines.append(f"Rewards earned: {', '.join(self.rewards)}")
        if self.suggestions:
            lines.append(f"Suggest to customer: {', '.join(self.suggestions)}")
        if self.milestone_reached:
            lines.append(f"Milestone: {self.milestone_reached}")
        if self.delivery_actions:
            lines.append(f"Delivery actions: {', '.join(self.delivery_actions)}")
        if self.refund_percentage is not None:
            lines.append(f"Refund: {self.refund_percentage}% — {self.cancellation_note}")
        if self.fraud_flags:
            lines.append(f"⚠️ FRAUD FLAGS: {', '.join(self.fraud_flags)}")
        if self.restrict_offers:
            lines.append("⚠️ Offers restricted due to abuse")
        if self.escalate_to_support:
            lines.append("⚠️ Escalate to support team")
        if self.escalate_to_human:
            lines.append("⚠️ Escalate to human agent")
        if self.ask_user:
            lines.append(f"Ask user: {self.ask_user}")
        return "\n".join(lines) if lines else "No special rules triggered."


# ═══════════════════════════════════════════════════════════════════════
#  Rules Engine
# ═══════════════════════════════════════════════════════════════════════

class RulesEngine:
    """
    Evaluates customer and order data against comprehensive business rules.

    All rules are evaluated independently. The result is an aggregated
    RulesResult object that can be injected into LLM prompts or returned
    via API.
    """

    def evaluate(
        self,
        customer: Dict[str, Any],
        orders: List[Dict[str, Any]],
        context: Optional[Dict[str, Any]] = None,
    ) -> RulesResult:
        """
        Evaluate all business rules.

        Args:
            customer: Customer profile dict (name, phone, customer_id, etc.)
            orders: List of order dicts (order_id, status, item, price, etc.)
            context: Optional runtime context (intent, sentiment, query_type, etc.)

        Returns:
            RulesResult with all triggered rules and their actions.
        """
        ctx = context or {}
        result = RulesResult()

        # Pre-compute order statistics
        stats = self._compute_stats(orders, customer)

        # Evaluate each category
        self._eval_customer_segmentation(stats, result)
        self._eval_pricing_and_discounts(stats, result)
        self._eval_delivery_and_operations(stats, orders, result)
        self._eval_cancellation_and_refund(stats, orders, ctx, result)
        self._eval_personalization(stats, orders, ctx, result)
        self._eval_risk_and_fraud(stats, orders, result)
        self._eval_support_automation(ctx, result)
        self._eval_loyalty_and_gamification(stats, result)
        self._eval_context_aware_ai(ctx, result)

        logger.info(
            "Rules evaluated: %d triggered for customer=%s",
            len(result.triggered_rules),
            customer.get("name", "unknown"),
        )

        return result

    # ── Statistics Pre-computation ─────────────────────────────────

    def _compute_stats(
        self, orders: List[Dict[str, Any]], customer: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Compute aggregate stats from order data."""
        now = datetime.now(timezone.utc)
        total_orders = len(orders)

        # Total spent
        total_spent = sum(
            float(o.get("price", o.get("PRICE", 0)) or 0) for o in orders
        )

        # Orders in last 30 days
        orders_last_30 = 0
        last_order_date = None
        for o in orders:
            order_date_raw = o.get("order_date", o.get("ORDER_DATE"))
            if order_date_raw:
                try:
                    if isinstance(order_date_raw, str):
                        od = datetime.fromisoformat(order_date_raw.replace("Z", "+00:00"))
                    elif isinstance(order_date_raw, datetime):
                        od = order_date_raw if order_date_raw.tzinfo else order_date_raw.replace(tzinfo=timezone.utc)
                    else:
                        continue
                    if (now - od).days <= 30:
                        orders_last_30 += 1
                    if last_order_date is None or od > last_order_date:
                        last_order_date = od
                except (ValueError, TypeError):
                    pass

        # Days since last order
        days_since_last = 0
        if last_order_date:
            days_since_last = (now - last_order_date).days

        # Cancelled and refunded orders
        cancelled = [o for o in orders if (o.get("status") or o.get("ORDER_STATUS", "")).lower() == "cancelled"]
        delivered = [o for o in orders if (o.get("status") or o.get("ORDER_STATUS", "")).lower() == "delivered"]

        # Most ordered item
        items = [o.get("item", o.get("PIZZA_NAME", "")) for o in orders if o.get("item") or o.get("PIZZA_NAME")]
        item_counts = Counter(items)
        most_ordered = item_counts.most_common(1)[0][0] if item_counts else None

        # Highest single order value
        max_order_value = max(
            (float(o.get("price", o.get("PRICE", 0)) or 0) for o in orders),
            default=0.0,
        )

        return {
            "total_orders": total_orders,
            "total_spent": total_spent,
            "orders_last_30_days": orders_last_30,
            "days_since_last_order": days_since_last,
            "cancelled_count": len(cancelled),
            "delivered_count": len(delivered),
            "most_ordered_item": most_ordered,
            "max_order_value": max_order_value,
            "customer_name": customer.get("name", ""),
            "has_birthday_today": customer.get("is_birthday", False),
        }

    # ── 1. Customer Segmentation ───────────────────────────────────

    def _eval_customer_segmentation(
        self, stats: Dict[str, Any], result: RulesResult
    ) -> None:
        # VIP Customer
        if stats["total_orders"] > 20 and stats["total_spent"] > 10000:
            result.customer_tier = "VIP"
            result.benefits.extend(["priority_support", "faster_delivery", "exclusive_discounts"])
            result.triggered_rules.append("vip_customer")

        # Frequent Customer
        elif stats["orders_last_30_days"] >= 5:
            result.customer_tags.append("Frequent Buyer")
            if result.customer_tier == "Regular":
                result.customer_tier = "Frequent"
            result.triggered_rules.append("frequent_customer")

        # Loyal Customer (not quite VIP but consistent)
        elif stats["total_orders"] > 10:
            result.customer_tags.append("Loyal Customer")
            if result.customer_tier == "Regular":
                result.customer_tier = "Loyal"
            result.triggered_rules.append("loyal_customer")

    # ── 2. Pricing & Discounts ─────────────────────────────────────

    def _eval_pricing_and_discounts(
        self, stats: Dict[str, Any], result: RulesResult
    ) -> None:
        # High-value order discount
        if stats["max_order_value"] > 1000:
            result.discounts.append("10% high-value order discount")
            result.triggered_rules.append("high_value_order_discount")

        # First order welcome offer
        if stats["total_orders"] == 1:
            result.discounts.append("20% welcome coupon for first order")
            result.triggered_rules.append("first_order_offer")

        # Inactive customer reactivation
        if stats["days_since_last_order"] > 30 and stats["total_orders"] > 0:
            result.offers.append("25% comeback discount — we miss you!")
            result.triggered_rules.append("inactive_customer_reactivation")

        # Bulk order discount
        if stats["total_orders"] > 10:
            result.discounts.append("20% loyalty discount coupon")
            result.triggered_rules.append("bulk_order_discount")

    # ── 3. Delivery & Operations ───────────────────────────────────

    def _eval_delivery_and_operations(
        self, stats: Dict[str, Any], orders: List[Dict[str, Any]], result: RulesResult
    ) -> None:
        for o in orders:
            status = (o.get("status") or o.get("ORDER_STATUS", "")).lower()

            # Delayed order compensation
            if status == "delayed" or status == "out_for_delivery":
                delay_minutes = int(o.get("delay_minutes", 0))
                if delay_minutes > 20:
                    result.delivery_actions.append(
                        f"Order {o.get('order_id', o.get('ORDER_ID', '?'))}: "
                        f"free delivery on next order (delayed {delay_minutes} min)"
                    )
                    result.escalate_to_support = True
                    result.triggered_rules.append("delayed_order_compensation")
                    result.triggered_rules.append("real_time_escalation")

    # ── 4. Cancellation & Refund ───────────────────────────────────

    def _eval_cancellation_and_refund(
        self,
        stats: Dict[str, Any],
        orders: List[Dict[str, Any]],
        ctx: Dict[str, Any],
        result: RulesResult,
    ) -> None:
        # Check if context has a specific order being cancelled
        cancel_order_status = ctx.get("cancel_order_status", "")

        if cancel_order_status == "preparing":
            result.refund_percentage = 80
            result.cancellation_note = "Late cancellation — order already being prepared (80% refund)"
            result.triggered_rules.append("late_cancellation")
        elif cancel_order_status == "placed":
            result.refund_percentage = 100
            result.cancellation_note = "Early cancellation — full refund"
            result.triggered_rules.append("early_cancellation")
        elif cancel_order_status in ("out_for_delivery", "delivered"):
            result.refund_percentage = 0
            result.cancellation_note = "Cannot cancel — order already on the way or delivered"
            result.triggered_rules.append("no_cancellation")

        # Compensation for past cancellations
        if stats["cancelled_count"] > 0 and stats["total_orders"] > 5:
            result.offers.append("compensation coupon for past cancellation")
            result.triggered_rules.append("cancellation_compensation")

    # ── 5. Personalization ─────────────────────────────────────────

    def _eval_personalization(
        self,
        stats: Dict[str, Any],
        orders: List[Dict[str, Any]],
        ctx: Dict[str, Any],
        result: RulesResult,
    ) -> None:
        # Favorite product suggestion
        if stats["most_ordered_item"]:
            result.suggestions.append(
                f"Your favorite: {stats['most_ordered_item']} — would you like to order it again?"
            )
            result.triggered_rules.append("favorite_product_suggestion")

        # Time-based suggestion
        current_hour = datetime.now().hour
        if 17 <= current_hour <= 22:  # Evening
            result.suggestions.append("It's the perfect time for pizza! Check out our evening specials.")
            result.triggered_rules.append("time_based_suggestion_evening")
        elif 11 <= current_hour <= 14:  # Lunch
            result.suggestions.append("Lunch combo deals are available right now!")
            result.triggered_rules.append("time_based_suggestion_lunch")

    # ── 6. Risk & Fraud ────────────────────────────────────────────

    def _eval_risk_and_fraud(
        self, stats: Dict[str, Any], orders: List[Dict[str, Any]], result: RulesResult
    ) -> None:
        # Multiple orders in short time span
        if stats["orders_last_30_days"] > 15:
            result.fraud_flags.append("High order volume — possible fraud or bulk abuse")
            result.triggered_rules.append("suspicious_activity")

        # High refund/cancellation frequency
        if stats["total_orders"] >= 10:
            cancel_ratio = stats["cancelled_count"] / stats["total_orders"]
            if cancel_ratio > 0.5:
                result.fraud_flags.append("High cancellation rate")
                result.restrict_offers = True
                result.triggered_rules.append("high_refund_frequency")

    # ── 7. Support Automation ──────────────────────────────────────

    def _eval_support_automation(
        self, ctx: Dict[str, Any], result: RulesResult
    ) -> None:
        query_type = ctx.get("query_type", "")
        sentiment = ctx.get("sentiment", "")

        # Auto-resolve order status queries
        if query_type == "order_status":
            result.auto_respond = True
            result.triggered_rules.append("auto_resolve_order_status")

        # Escalate angry customers
        if sentiment == "negative":
            result.escalate_to_human = True
            result.triggered_rules.append("escalate_angry_customer")

    # ── 8. Loyalty & Gamification ──────────────────────────────────

    def _eval_loyalty_and_gamification(
        self, stats: Dict[str, Any], result: RulesResult
    ) -> None:
        total = stats["total_orders"]

        # Milestone rewards
        if total == 10:
            result.rewards.append("🎉 10th order milestone — free item!")
            result.milestone_reached = "10_orders"
            result.triggered_rules.append("milestone_reward_10")
        elif total == 25:
            result.rewards.append("🎉 25th order milestone — free premium pizza!")
            result.milestone_reached = "25_orders"
            result.triggered_rules.append("milestone_reward_25")
        elif total == 50:
            result.rewards.append("🏆 50th order milestone — VIP status unlocked + free meal!")
            result.milestone_reached = "50_orders"
            result.triggered_rules.append("milestone_reward_50")

        # Birthday offer
        if stats.get("has_birthday_today"):
            result.rewards.append("🎂 Happy Birthday! Free dessert with your next order!")
            result.triggered_rules.append("birthday_offer")

    # ── 9. Context-Aware AI ────────────────────────────────────────

    def _eval_context_aware_ai(
        self, ctx: Dict[str, Any], result: RulesResult
    ) -> None:
        phone = ctx.get("phone_number", "")
        order_id = ctx.get("order_id", "")
        intent = ctx.get("intent", "")

        # Missing identifying info
        if not phone and not order_id:
            result.ask_user = "Please provide your phone number or order ID so I can look up your information."
            result.triggered_rules.append("missing_info")

        # Smart tool trigger
        if intent in ("order_status", "customer_query", "snowflake"):
            result.use_snowflake = True
            result.triggered_rules.append("smart_tool_trigger")


# ═══════════════════════════════════════════════════════════════════════
#  Legacy API — backward-compatible wrapper
# ═══════════════════════════════════════════════════════════════════════

_engine = RulesEngine()


def apply_customer_rules(
    customer_data: Dict[str, Any],
    orders_data: List[Dict[str, Any]],
    context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Backward-compatible wrapper — evaluates all business rules and returns
    an enriched context dict.
    """
    result = _engine.evaluate(customer_data, orders_data, context)

    enriched = {
        "customer_id": customer_data.get("customer_id"),
        "name": customer_data.get("name"),
        "phone": customer_data.get("phone"),
        "total_orders": len(orders_data),
        "orders": orders_data,
        "last_order": orders_data[-1] if orders_data else None,
        "status": result.customer_tier,
        "rewards": result.rewards + result.discounts,
        "offers": result.offers,
        "suggestions": result.suggestions,
        "benefits": result.benefits,
        "tags": result.customer_tags,
        "delivery_actions": result.delivery_actions,
        "fraud_flags": result.fraud_flags,
        "triggered_rules": result.triggered_rules,
        "rules_context": result.to_prompt_context(),
    }

    if result.refund_percentage is not None:
        enriched["refund_percentage"] = result.refund_percentage
        enriched["cancellation_note"] = result.cancellation_note

    if result.escalate_to_human:
        enriched["escalate_to_human"] = True
    if result.escalate_to_support:
        enriched["escalate_to_support"] = True
    if result.ask_user:
        enriched["ask_user"] = result.ask_user

    return enriched
