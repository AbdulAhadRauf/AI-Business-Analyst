"""
Rule Engine — evaluates YAML-defined business rules against runtime context.

Businesses define rules in business_config.yaml and the engine evaluates
them automatically during conversations. Rules can:
- Inject greeting messages
- Block certain actions (e.g., block update if order is in transit)
- Require confirmation before writes
- Trigger escalation to a human

The rules are also rendered into the agent's system prompt so it
naturally follows them in conversation.
"""

import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from app.rules.rule_models import RuleAction, RuleEvalContext

logger = logging.getLogger(__name__)


class RuleEngine:
    """
    Evaluates business rules from YAML config against runtime context.

    Rule conditions are simple expressions like:
      "customer.days_since_last_order > 14"
      "order.status == 'in_transit'"
      "session.auth_attempts >= 3"
    """

    def __init__(self, rules_config: Dict[str, Any]):
        self.rules_config = rules_config or {}

    def evaluate_category(
        self, category: str, context: RuleEvalContext
    ) -> List[RuleAction]:
        """
        Evaluate all rules in a category against the context.
        Returns all matching RuleActions.
        """
        rules = self.rules_config.get(category, [])
        matched = []

        for rule in rules:
            condition = rule.get("condition", "")
            if self._check_condition(condition, context):
                action = RuleAction(
                    action=rule.get("action", "inject_message"),
                    message=rule.get("message", ""),
                    reason=rule.get("reason", ""),
                    blocks=rule.get("blocks", ""),
                )
                matched.append(action)
                logger.info(
                    "Rule matched: [%s] condition='%s' → action='%s'",
                    category, condition, action.action,
                )

        return matched

    def check_guard(
        self, action_name: str, context: RuleEvalContext
    ) -> Optional[RuleAction]:
        """
        Check if an action is blocked by any guard rule.
        Returns the blocking RuleAction if blocked, None if allowed.
        """
        guards = self.rules_config.get("guards", [])
        for rule in guards:
            blocks = rule.get("blocks", "")
            if blocks and blocks == action_name:
                condition = rule.get("condition", "")
                if self._check_condition(condition, context):
                    return RuleAction(
                        action="block",
                        message=rule.get("message", f"Action '{action_name}' is blocked."),
                        blocks=blocks,
                    )
        return None

    def get_greeting_messages(self, context: RuleEvalContext) -> List[str]:
        """Evaluate greeting rules and return matching messages."""
        actions = self.evaluate_category("greeting", context)
        return [a.message for a in actions if a.message]

    def should_escalate(self, context: RuleEvalContext) -> Optional[RuleAction]:
        """Check if escalation rules are triggered."""
        actions = self.evaluate_category("escalation", context)
        return actions[0] if actions else None

    def render_rules_for_prompt(self) -> str:
        """
        Render all business rules as natural language for the agent's
        system prompt, so it knows the rules and follows them.
        """
        lines = []
        for category, rules in self.rules_config.items():
            if not rules:
                continue
            lines.append(f"\n### {category.replace('_', ' ').title()} Rules:")
            for rule in rules:
                condition = rule.get("condition", "unknown")
                message = rule.get("message", "")
                blocks = rule.get("blocks", "")
                action = rule.get("action", "")

                if blocks:
                    lines.append(f"- If {condition}: BLOCK {blocks}. Say: \"{message}\"")
                elif action == "escalate":
                    reason = rule.get("reason", "")
                    lines.append(f"- If {condition}: ESCALATE to human agent. Reason: {reason}")
                elif message:
                    lines.append(f"- If {condition}: Say to customer: \"{message}\"")

        return "\n".join(lines) if lines else "No specific business rules configured."

    def _check_condition(self, condition: str, context: RuleEvalContext) -> bool:
        """
        Evaluate a simple condition string against the context.

        Supports:
          customer.field == 'value'
          customer.field > 14
          order.status == 'in_transit'
          session.auth_attempts >= 3
        """
        if not condition:
            return False

        try:
            # Build a safe evaluation namespace from context
            namespace = {}

            if context.customer:
                namespace["customer"] = _DotDict(context.customer)
            else:
                namespace["customer"] = _DotDict({})

            if context.order:
                namespace["order"] = _DotDict(context.order)
            else:
                namespace["order"] = _DotDict({})

            if context.session:
                namespace["session"] = _DotDict(context.session)
            else:
                namespace["session"] = _DotDict({})

            # Evaluate safely — only allow comparisons, no imports or calls
            result = _safe_eval(condition, namespace)
            return bool(result)

        except Exception as e:
            logger.warning("Rule condition eval failed: '%s' → %s", condition, e)
            return False


class _DotDict:
    """Dict wrapper that supports dot-notation access for rule conditions."""

    def __init__(self, data: Dict[str, Any]):
        self._data = data

    def __getattr__(self, key: str) -> Any:
        value = self._data.get(key)
        if value is None:
            return _NullValue()
        if isinstance(value, dict):
            return _DotDict(value)
        return value

    def __repr__(self):
        return repr(self._data)


class _NullValue:
    """Represents a missing value — comparisons always return False."""

    def __gt__(self, other): return False
    def __lt__(self, other): return False
    def __ge__(self, other): return False
    def __le__(self, other): return False
    def __eq__(self, other): return False
    def __ne__(self, other): return True
    def __bool__(self): return False
    def __repr__(self): return "NullValue"


def _safe_eval(expression: str, namespace: Dict[str, Any]) -> bool:
    """
    Safely evaluate a simple comparison expression.
    Only allows basic comparisons, no function calls or imports.
    """
    # Block dangerous patterns
    dangerous = ["import", "__", "exec", "eval", "open", "os.", "sys.", "subprocess"]
    for d in dangerous:
        if d in expression:
            raise ValueError(f"Unsafe expression: {expression}")

    # Use restricted eval
    return eval(expression, {"__builtins__": {}}, namespace)
