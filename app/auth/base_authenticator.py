"""
Base authenticator — abstract interface for caller verification strategies.

Businesses configure which auth strategy to use in business_config.yaml.
Available strategies: knowledge, otp, caller_id, none.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional


@dataclass
class AuthChallenge:
    """What to ask the caller next."""
    prompt: str                      # "What's the phone number on your account?"
    field_name: str                  # "phone" (internal key)
    is_final: bool = False           # True if this is the last challenge


@dataclass
class AuthResult:
    """Result of a verification attempt."""
    success: bool
    customer_id: Optional[str] = None
    customer_profile: Optional[dict] = None
    message: str = ""                # "Verified!" or "That doesn't match."
    challenges_remaining: int = 0


class BaseAuthenticator(ABC):
    """
    Abstract interface for authentication strategies.

    Subclasses implement the specific verification logic:
    - KnowledgeAuthenticator: asks questions the customer should know
    - OTPAuthenticator: sends a code to phone/email
    - CallerIDAuthenticator: matches phone number automatically
    """

    @abstractmethod
    async def get_next_challenge(self, session_id: str) -> Optional[AuthChallenge]:
        """
        Return the next challenge to present to the caller.
        Returns None if all challenges are complete.
        """
        ...

    @abstractmethod
    async def verify_response(
        self, session_id: str, field_name: str, response: str
    ) -> AuthResult:
        """
        Verify the caller's response to a challenge.
        Returns AuthResult with success/failure and customer data if verified.
        """
        ...
