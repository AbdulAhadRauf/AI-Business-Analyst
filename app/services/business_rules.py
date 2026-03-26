"""
Business rules — priority sorting, context strings, freshness labels.
"""

import logging
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

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
