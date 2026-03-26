"""
Session model — tracks per-caller state throughout a conversation.

Each call/chat creates a Session that persists auth state, customer
profile, cached data, and conversation context. Comparable to:
  - OpenAI Assistants → Threads
  - Intercom Fin → Conversations
  - Zendesk AI → Tickets
"""

import uuid
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class Session:
    """
    Represents a single caller/user session.

    Created when a call or chat starts, persists throughout the
    conversation, destroyed when the session ends or times out.
    """

    # ── Identity ──────────────────────────────────────────────────
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    channel: str = "voice"                  # voice | chat | api
    tenant_id: str = "default"              # Multi-tenant support

    # ── Authentication State ──────────────────────────────────────
    authenticated: bool = False
    customer_id: Optional[str] = None
    customer_profile: Optional[Dict[str, Any]] = None   # Cached after auth
    auth_attempts: int = 0
    auth_challenge_index: int = 0           # Which challenge we're on
    pending_auth_field: Optional[str] = None  # Field being verified

    # ── Conversation ──────────────────────────────────────────────
    messages: List[Dict[str, str]] = field(default_factory=list)
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_activity: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    # ── Resolution Tracking ───────────────────────────────────────
    resolved: bool = False
    resolution_summary: Optional[str] = None
    escalated: bool = False
    escalation_reason: Optional[str] = None

    # ── Cached Data (for speed after auth) ────────────────────────
    cached_data: Dict[str, Any] = field(default_factory=dict)

    # ── Metadata ──────────────────────────────────────────────────
    metadata: Dict[str, Any] = field(default_factory=dict)

    def touch(self) -> None:
        """Update last activity timestamp."""
        self.last_activity = datetime.now(timezone.utc)

    def add_message(self, role: str, content: str) -> None:
        """Add a message to the conversation history."""
        self.messages.append({
            "role": role,
            "content": content,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
        self.touch()

    def get_duration_seconds(self) -> float:
        """How long this session has been active."""
        return (datetime.now(timezone.utc) - self.started_at).total_seconds()

    def to_dict(self) -> Dict[str, Any]:
        """Serialize session to dict (for logging, analytics, handoff)."""
        return {
            "id": self.id,
            "channel": self.channel,
            "authenticated": self.authenticated,
            "customer_id": self.customer_id,
            "auth_attempts": self.auth_attempts,
            "message_count": len(self.messages),
            "duration_seconds": round(self.get_duration_seconds(), 1),
            "resolved": self.resolved,
            "escalated": self.escalated,
            "started_at": self.started_at.isoformat(),
        }
