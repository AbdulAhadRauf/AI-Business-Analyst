"""
Knowledge-based authenticator — verifies callers by asking questions
they should know the answers to (phone number, order ID, last 4 of card, etc.).

Configured via business_config.yaml:

    auth:
      strategy: knowledge
      challenges:
        - prompt: "What's the phone number on your account?"
          match_adapter: customers
          match_field: phone
        - prompt: "Last 4 digits of your payment card?"
          match_adapter: customers
          match_field: card_last_4
"""

import logging
import re
from typing import Any, Dict, List, Optional

from app.auth.base_authenticator import BaseAuthenticator, AuthChallenge, AuthResult
from app.adapters.base_adapter import BaseDataAdapter

logger = logging.getLogger(__name__)


class KnowledgeAuthenticator(BaseAuthenticator):
    """
    Verifies callers by asking knowledge-based questions and matching
    responses against data adapter records.

    Flow:
    1. Present first challenge ("What's your phone number?")
    2. Search the adapter for a matching record
    3. If found, present next challenge ("Last 4 of card?")
    4. Verify remaining challenges against the found record
    5. Return AuthResult with customer profile
    """

    def __init__(
        self,
        challenges: List[Dict[str, str]],
        adapters: Dict[str, BaseDataAdapter],
        max_attempts: int = 3,
        on_failure_message: str = "I wasn't able to verify your identity.",
    ):
        self.challenges = challenges
        self.adapters = adapters
        self.max_attempts = max_attempts
        self.on_failure_message = on_failure_message

        # Per-session state: {session_id: {...}}
        self._session_state: Dict[str, Dict[str, Any]] = {}

    def _get_state(self, session_id: str) -> Dict[str, Any]:
        """Get or create auth state for a session."""
        if session_id not in self._session_state:
            self._session_state[session_id] = {
                "challenge_index": 0,
                "attempts": 0,
                "matched_record": None,
                "verified_fields": {},
            }
        return self._session_state[session_id]

    async def get_next_challenge(self, session_id: str) -> Optional[AuthChallenge]:
        state = self._get_state(session_id)
        idx = state["challenge_index"]

        if idx >= len(self.challenges):
            return None  # All challenges complete

        challenge_config = self.challenges[idx]
        return AuthChallenge(
            prompt=challenge_config["prompt"],
            field_name=challenge_config.get("match_field", f"challenge_{idx}"),
            is_final=(idx == len(self.challenges) - 1),
        )

    async def verify_response(
        self, session_id: str, field_name: str, response: str
    ) -> AuthResult:
        state = self._get_state(session_id)
        state["attempts"] += 1

        if state["attempts"] > self.max_attempts:
            self._cleanup(session_id)
            return AuthResult(
                success=False,
                message=self.on_failure_message,
            )

        idx = state["challenge_index"]
        if idx >= len(self.challenges):
            return AuthResult(success=False, message="No more challenges to verify.")

        challenge_config = self.challenges[idx]
        adapter_name = challenge_config.get("match_adapter", "")
        match_field = challenge_config.get("match_field", "")
        normalized_response = self._normalize(response)

        # First challenge: search for the customer
        if state["matched_record"] is None and adapter_name in self.adapters:
            adapter = self.adapters[adapter_name]
            result = await adapter.search(normalized_response, limit=5)

            # Find exact match on the specified field
            matched = None
            for record in result.items:
                record_value = self._normalize(str(record.get(match_field, "")))
                if record_value == normalized_response:
                    matched = record
                    break

            if matched is None:
                remaining = self.max_attempts - state["attempts"]
                return AuthResult(
                    success=False,
                    message=(
                        f"I couldn't find an account matching that {match_field}. "
                        f"You have {remaining} {'attempt' if remaining == 1 else 'attempts'} left."
                    ),
                    challenges_remaining=len(self.challenges) - idx,
                )

            state["matched_record"] = matched
            state["verified_fields"][match_field] = True
            state["challenge_index"] += 1

        # Subsequent challenges: verify against the matched record
        elif state["matched_record"] is not None:
            record = state["matched_record"]
            record_value = self._normalize(str(record.get(match_field, "")))

            if record_value != normalized_response:
                remaining = self.max_attempts - state["attempts"]
                if remaining <= 0:
                    self._cleanup(session_id)
                    return AuthResult(
                        success=False,
                        message=self.on_failure_message,
                    )
                return AuthResult(
                    success=False,
                    message=(
                        f"That doesn't match what we have on file. "
                        f"You have {remaining} {'attempt' if remaining == 1 else 'attempts'} left."
                    ),
                    challenges_remaining=len(self.challenges) - idx,
                )

            state["verified_fields"][match_field] = True
            state["challenge_index"] += 1

        # Check if all challenges are done
        if state["challenge_index"] >= len(self.challenges):
            record = state["matched_record"]
            customer_id = None
            if record:
                # Find the ID field from the adapter
                adapter = self.adapters.get(challenge_config.get("match_adapter", ""))
                if adapter and hasattr(adapter, 'id_field'):
                    customer_id = str(record.get(adapter.id_field, ""))

            self._cleanup(session_id)
            return AuthResult(
                success=True,
                customer_id=customer_id,
                customer_profile=record,
                message="You're verified! How can I help you today?",
                challenges_remaining=0,
            )

        # More challenges remain
        next_challenge = await self.get_next_challenge(session_id)
        return AuthResult(
            success=False,
            message=next_challenge.prompt if next_challenge else "",
            challenges_remaining=len(self.challenges) - state["challenge_index"],
        )

    def _normalize(self, value: str) -> str:
        """Normalize a response for comparison (strip, lowercase, remove formatting)."""
        value = value.strip().lower()
        # Remove common formatting (dashes, parentheses, spaces in phone numbers)
        value = re.sub(r'[\s\-\(\)\.\+]', '', value)
        return value

    def _cleanup(self, session_id: str) -> None:
        """Remove session auth state."""
        self._session_state.pop(session_id, None)
