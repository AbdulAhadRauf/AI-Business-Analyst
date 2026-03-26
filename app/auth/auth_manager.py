"""
Auth Manager — orchestrates the authentication flow based on config.

Sits between the agent and the authenticator. The agent calls
authenticate_caller as a tool, and the auth manager handles the
challenge/response flow, updating the session state.
"""

import logging
from typing import Any, Dict, Optional

from app.auth.base_authenticator import BaseAuthenticator, AuthChallenge, AuthResult
from app.auth.knowledge_auth import KnowledgeAuthenticator
from app.adapters.base_adapter import BaseDataAdapter
from app.sessions.session import Session

logger = logging.getLogger(__name__)


class AuthManager:
    """
    Manages the authentication lifecycle for a session.

    Configured from business_config.yaml → auth section.
    Creates the appropriate authenticator and drives the flow.
    """

    def __init__(
        self,
        auth_config: Dict[str, Any],
        adapters: Dict[str, BaseDataAdapter],
    ):
        self.config = auth_config
        self.required = auth_config.get("required", False)
        self.strategy = auth_config.get("strategy", "none")
        self.max_attempts = auth_config.get("max_attempts", 3)
        self.on_failure = auth_config.get(
            "on_failure",
            "I wasn't able to verify your identity. Please contact us directly.",
        )
        self._authenticator = self._create_authenticator(adapters)

    def _create_authenticator(
        self, adapters: Dict[str, BaseDataAdapter]
    ) -> Optional[BaseAuthenticator]:
        """Create the authenticator based on strategy."""
        if self.strategy == "none" or not self.required:
            return None

        if self.strategy == "knowledge":
            return KnowledgeAuthenticator(
                challenges=self.config.get("challenges", []),
                adapters=adapters,
                max_attempts=self.max_attempts,
                on_failure_message=self.on_failure,
            )

        logger.warning("Unknown auth strategy: %s, falling back to none", self.strategy)
        return None

    @property
    def is_required(self) -> bool:
        """Whether authentication is required for this tenant."""
        return self.required and self._authenticator is not None

    async def get_initial_challenge(self, session: Session) -> Optional[str]:
        """
        Get the first auth challenge to present to the caller.
        Returns the prompt string, or None if auth is not required.
        """
        if not self.is_required or session.authenticated:
            return None

        challenge = await self._authenticator.get_next_challenge(session.id)
        if challenge:
            session.pending_auth_field = challenge.field_name
            return challenge.prompt
        return None

    async def process_auth_response(
        self, session: Session, response: str
    ) -> AuthResult:
        """
        Process a verification response from the caller.
        Updates session state based on the result.
        """
        if not self._authenticator:
            return AuthResult(
                success=True,
                message="Authentication not required.",
            )

        field_name = session.pending_auth_field or "unknown"
        result = await self._authenticator.verify_response(
            session.id, field_name, response
        )

        session.auth_attempts += 1

        if result.success:
            session.authenticated = True
            session.customer_id = result.customer_id
            session.customer_profile = result.customer_profile
            logger.info(
                "Session %s authenticated (customer_id=%s, attempts=%d)",
                session.id, result.customer_id, session.auth_attempts,
            )
        else:
            # Queue next challenge if there is one
            if result.challenges_remaining > 0 and result.message:
                next_challenge = await self._authenticator.get_next_challenge(session.id)
                if next_challenge:
                    session.pending_auth_field = next_challenge.field_name

            if session.auth_attempts >= self.max_attempts:
                logger.warning(
                    "Session %s failed auth after %d attempts",
                    session.id, session.auth_attempts,
                )

        return result

    def should_authenticate(self, session: Session) -> bool:
        """Check if this session still needs authentication."""
        return self.is_required and not session.authenticated
