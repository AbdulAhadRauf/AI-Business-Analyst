"""
Action Executor — handles write operations with safety checks.

Before any data modification, it:
1. Checks authentication state
2. Evaluates business rules / guard conditions
3. Optionally asks for confirmation
4. Executes the write via the adapter
5. Logs the action for audit
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from app.adapters.base_adapter import BaseDataAdapter
from app.rules.rule_engine import RuleEngine
from app.rules.rule_models import RuleEvalContext
from app.sessions.session import Session

logger = logging.getLogger(__name__)


class ActionResult:
    """Result of an action execution."""

    def __init__(
        self,
        success: bool,
        message: str,
        data: Optional[Dict[str, Any]] = None,
        blocked: bool = False,
        needs_confirmation: bool = False,
    ):
        self.success = success
        self.message = message
        self.data = data
        self.blocked = blocked
        self.needs_confirmation = needs_confirmation

    def to_json(self) -> str:
        return json.dumps({
            "success": self.success,
            "message": self.message,
            "data": self.data,
            "blocked": self.blocked,
            "needs_confirmation": self.needs_confirmation,
        }, default=str)


class ActionExecutor:
    """
    Safe execution layer for data mutations.

    Enforces auth, business rules, and confirmation before writes.
    All actions are logged for audit trails.
    """

    def __init__(
        self,
        rule_engine: Optional[RuleEngine] = None,
        confirm_before_write: bool = True,
    ):
        self.rule_engine = rule_engine
        self.confirm_before_write = confirm_before_write
        self._action_log: List[Dict[str, Any]] = []
        self._pending_confirmations: Dict[str, Dict[str, Any]] = {}

    async def execute_update(
        self,
        adapter: BaseDataAdapter,
        record_id: Any,
        updates: Dict[str, Any],
        session: Optional[Session] = None,
        confirmed: bool = False,
    ) -> ActionResult:
        """
        Execute an update with full safety checks.

        1. Check auth
        2. Check guard rules
        3. Check confirmation
        4. Execute write
        5. Log action
        """
        action_name = f"update_{adapter.source_name}"

        # ── 1. Auth Check ────────────────────────────────────────
        if session and not session.authenticated:
            return ActionResult(
                success=False,
                message="You need to verify your identity before I can make changes.",
                blocked=True,
            )

        # ── 2. Guard Rules Check ─────────────────────────────────
        if self.rule_engine:
            # Get the current record to evaluate rules against
            current = await adapter.get_by_id(record_id)
            if current is None:
                return ActionResult(
                    success=False,
                    message=f"Record {record_id} not found.",
                )

            context = RuleEvalContext(
                order=current,
                customer=session.customer_profile if session else None,
                session=session.to_dict() if session else None,
            )
            guard = self.rule_engine.check_guard(action_name, context)
            if guard:
                return ActionResult(
                    success=False,
                    message=guard.message,
                    blocked=True,
                )

        # ── 3. Confirmation Check ────────────────────────────────
        if self.confirm_before_write and not confirmed:
            # Store pending action for confirmation
            pending_key = f"{session.id}_{action_name}_{record_id}" if session else f"anon_{record_id}"
            self._pending_confirmations[pending_key] = {
                "adapter_name": adapter.source_name,
                "record_id": record_id,
                "updates": updates,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            return ActionResult(
                success=False,
                needs_confirmation=True,
                message=(
                    f"I'll update record {record_id} with: "
                    f"{', '.join(f'{k} → {v}' for k, v in updates.items())}. "
                    f"Can you confirm?"
                ),
            )

        # ── 4. Execute ───────────────────────────────────────────
        try:
            result = await adapter.update(record_id, updates)

            # ── 5. Log ───────────────────────────────────────────
            self._log_action(
                action=action_name,
                record_id=record_id,
                updates=updates,
                session_id=session.id if session else None,
                customer_id=session.customer_id if session else None,
                success=True,
            )

            return ActionResult(
                success=True,
                message=f"Done! Record {record_id} has been updated.",
                data=result,
            )

        except Exception as e:
            logger.error("Action failed: %s record %s: %s", action_name, record_id, e)
            self._log_action(
                action=action_name,
                record_id=record_id,
                updates=updates,
                session_id=session.id if session else None,
                success=False,
                error=str(e),
            )
            return ActionResult(
                success=False,
                message=f"Sorry, I couldn't update that record: {e}",
            )

    def confirm_pending(self, session_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a pending confirmation for a session.
        Returns the pending action details, or None.
        """
        for key, action in self._pending_confirmations.items():
            if key.startswith(session_id):
                return self._pending_confirmations.pop(key)
        return None

    def _log_action(self, **kwargs) -> None:
        """Log an action for audit trail."""
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            **kwargs,
        }
        self._action_log.append(entry)
        logger.info("Action log: %s", entry)

    def get_action_log(self) -> List[Dict[str, Any]]:
        """Get the full action audit log."""
        return list(self._action_log)
