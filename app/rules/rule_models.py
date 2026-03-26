"""
Rule models — data structures for the business rules engine.
"""

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class RuleAction:
    """Action to take when a rule matches."""
    action: str          # inject_message | block | escalate | require_confirmation
    message: str = ""    # Message to inject or error message
    reason: str = ""     # For escalation logging
    blocks: str = ""     # Which tool/action to block


@dataclass
class BusinessRule:
    """A single business rule with a condition and action."""
    condition: str       # e.g. "customer.days_since_last_order > 14"
    action: str          # e.g. "inject_message"
    message: str = ""
    reason: str = ""
    blocks: str = ""


@dataclass
class RuleEvalContext:
    """Context passed to the rule engine for evaluation."""
    customer: Optional[Dict[str, Any]] = None
    order: Optional[Dict[str, Any]] = None
    session: Optional[Dict[str, Any]] = None
    metadata: Optional[Dict[str, Any]] = None
