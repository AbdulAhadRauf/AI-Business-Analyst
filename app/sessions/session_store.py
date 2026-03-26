"""
Session store — manages session lifecycle (create, retrieve, cleanup).

Uses in-memory storage by default. Can be swapped to Redis for
production multi-instance deployments.
"""

import logging
from datetime import datetime, timezone
from typing import Dict, Optional

from app.sessions.session import Session

logger = logging.getLogger(__name__)


class SessionStore:
    """
    In-memory session store.

    For production with multiple server instances, replace with
    RedisSessionStore that serializes sessions to Redis.
    """

    def __init__(self, timeout_seconds: int = 3600):
        self._sessions: Dict[str, Session] = {}
        self._timeout = timeout_seconds

    def create_session(
        self,
        channel: str = "voice",
        tenant_id: str = "default",
        **metadata,
    ) -> Session:
        """Create a new session and store it."""
        session = Session(
            channel=channel,
            tenant_id=tenant_id,
            metadata=metadata,
        )
        self._sessions[session.id] = session
        logger.info(
            "Created session %s (channel=%s, tenant=%s)",
            session.id, channel, tenant_id,
        )
        return session

    def get_session(self, session_id: str) -> Optional[Session]:
        """Retrieve a session by ID. Returns None if expired or not found."""
        session = self._sessions.get(session_id)
        if session is None:
            return None

        # Check timeout
        age = (datetime.now(timezone.utc) - session.last_activity).total_seconds()
        if age > self._timeout:
            logger.info("Session %s timed out (%.0fs idle)", session_id, age)
            self.end_session(session_id)
            return None

        return session

    def end_session(
        self,
        session_id: str,
        resolved: bool = False,
        summary: Optional[str] = None,
    ) -> Optional[Session]:
        """End and remove a session. Returns the session for logging."""
        session = self._sessions.pop(session_id, None)
        if session:
            session.resolved = resolved
            session.resolution_summary = summary
            logger.info(
                "Ended session %s (duration=%.0fs, resolved=%s, messages=%d)",
                session_id,
                session.get_duration_seconds(),
                resolved,
                len(session.messages),
            )
        return session

    def get_active_count(self) -> int:
        """Number of active sessions."""
        return len(self._sessions)

    def cleanup_expired(self) -> int:
        """Remove all expired sessions. Returns count removed."""
        now = datetime.now(timezone.utc)
        expired = [
            sid for sid, s in self._sessions.items()
            if (now - s.last_activity).total_seconds() > self._timeout
        ]
        for sid in expired:
            self.end_session(sid)
        if expired:
            logger.info("Cleaned up %d expired sessions", len(expired))
        return len(expired)


# ── Singleton ───────────────────────────────────────────────────────

_store: Optional[SessionStore] = None


def get_session_store(timeout_seconds: int = 3600) -> SessionStore:
    """Get or create the global session store."""
    global _store
    if _store is None:
        _store = SessionStore(timeout_seconds=timeout_seconds)
    return _store
