"""
Voice optimiser – generates natural-language summaries
suitable for text-to-speech playback. Keeps responses concise
so they work well in a voice conversation.
"""

from typing import Any, Dict, List


def generate_voice_summary(
    source: str,
    data: List[Dict[str, Any]],
    total: int,
) -> str:
    """
    Produce a short, spoken summary like:
      "You have 12 open support tickets, 5 are high priority."
    """
    count = len(data)
    if count == 0:
        return f"I didn't find any {source} records matching your query."

    # ── Support tickets ─────────────────────────────────────────────
    if source == "Support Tickets":
        open_count = sum(1 for r in data if r.get("status") == "open")
        high_count = sum(1 for r in data if r.get("priority") == "high")
        parts = [f"I found {total} support tickets"]
        if open_count:
            parts.append(f"{open_count} are currently open")
        if high_count:
            parts.append(f"{high_count} are high priority")
        return ". ".join(parts) + "."

    # ── CRM customers ──────────────────────────────────────────────
    if source == "CRM":
        active_count = sum(1 for r in data if r.get("status") == "active")
        return (
            f"I found {total} customers in the CRM. "
            f"{active_count} are active. "
            f"Showing the first {count}."
        )

    # ── Analytics ──────────────────────────────────────────────────
    if source == "Analytics":
        if data and "average" in data[0]:
            # Summary mode
            d = data[0]
            return (
                f"Over the last {d.get('period_days', '?')} days, "
                f"the average {d.get('metric', 'metric')} was {d.get('average')}. "
                f"The trend is {d.get('trend', 'flat')}."
            )
        return f"I found {total} analytics data points. Showing {count}."

    # ── Generic fallback ───────────────────────────────────────────
    if total > count:
        return f"I found {total} {source} records. Showing the first {count}."
    return f"Here are {count} {source} records."


def generate_voice_hint(source: str, total: int, returned: int) -> str:
    """
    A short hint prepended to voice responses so the user knows
    there's more data available.
    """
    if total > returned:
        remaining = total - returned
        return f"There are {remaining} more results. Ask me to show more if you'd like."
    return ""
