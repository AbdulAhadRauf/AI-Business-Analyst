"""
Data router — unified REST API for CRM, Support, and Analytics.
Uses a single _build_response() helper to eliminate copy-paste handlers.
"""

import math, logging
from typing import Optional
from datetime import datetime

from fastapi import APIRouter, Query, HTTPException

from app.connectors.crm_connector import CRMConnector
from app.connectors.support_connector import SupportConnector
from app.connectors.analytics_connector import AnalyticsConnector
from app.services.business_rules import sort_by_priority, generate_context_string, freshness_label
from app.services.voice_optimizer import generate_voice_summary, generate_voice_hint
from app.services.data_identifier import identify_data_type
from app.models.common import DataResponse, Metadata

logger = logging.getLogger(__name__)
router = APIRouter(tags=["Data"])

# ── Singleton connector instances ──────────────────────────────────────
crm = CRMConnector()
support = SupportConnector()
analytics = AnalyticsConnector()


# ── DRY response builder ──────────────────────────────────────────────
def _build_response(
    source: str,
    result: dict,
    *,
    page: int = 1,
    limit: int = 10,
    context: Optional[str] = None,
    post_sort=None,
) -> DataResponse:
    items = post_sort(result["items"]) if post_sort else result["items"]
    total = result["total"]
    total_pages = max(1, math.ceil(total / limit))

    return DataResponse(
        data=items,
        metadata=Metadata(
            total_results=total,
            returned_results=len(items),
            page=page,
            total_pages=total_pages,
            data_type=identify_data_type(items),
            data_freshness=freshness_label(),
            voice_hint=generate_voice_hint(source, total, len(items)) or None,
            query_context=context,
        ),
        voice_summary=generate_voice_summary(source, items, total),
    )


# ═══════════════════════════════════════════════════════════════════════
#  CRM
# ═══════════════════════════════════════════════════════════════════════

@router.get("/data/crm", response_model=DataResponse, summary="Query CRM customers")
def get_crm_data(
    status: Optional[str] = Query(None, description="active or inactive"),
    search: Optional[str] = Query(None, description="Search by name/email"),
    limit: int = Query(10, ge=1, le=100),
    page: int = Query(1, ge=1),
):
    result = crm.search(search, limit=limit) if search else crm.fetch(limit=limit, page=page, status=status)
    ctx = generate_context_string("CRM", result["total"], len(result["items"]), filters={"status": status, "search": search})
    return _build_response("CRM", result, page=page, limit=limit, context=ctx)


# ═══════════════════════════════════════════════════════════════════════
#  Support Tickets
# ═══════════════════════════════════════════════════════════════════════

@router.get("/data/support", response_model=DataResponse, summary="Query support tickets")
def get_support_data(
    priority: Optional[str] = Query(None, description="high, medium, or low"),
    status: Optional[str] = Query(None, description="open or closed"),
    customer_id: Optional[int] = Query(None),
    limit: int = Query(10, ge=1, le=100),
    page: int = Query(1, ge=1),
):
    result = support.fetch(limit=limit, page=page, priority=priority, status=status, customer_id=customer_id)
    ctx = generate_context_string("Support Tickets", result["total"], len(result["items"]),
                                  filters={"priority": priority, "status": status, "customer_id": customer_id})
    return _build_response("Support Tickets", result, page=page, limit=limit, context=ctx, post_sort=sort_by_priority)


# ═══════════════════════════════════════════════════════════════════════
#  Analytics
# ═══════════════════════════════════════════════════════════════════════

@router.get("/data/analytics", response_model=DataResponse, summary="Query analytics metrics")
def get_analytics_data(
    metric: Optional[str] = Query(None, description="e.g. daily_active_users"),
    days: int = Query(7, ge=1, le=365),
    limit: int = Query(10, ge=1, le=100),
    page: int = Query(1, ge=1),
):
    result = analytics.fetch(limit=limit, page=page, metric=metric, days=days)
    return _build_response("Analytics", result, page=page, limit=limit,
                           context=f"Analytics: {metric or 'all metrics'}, last {days} days")


@router.get("/data/analytics/summary", summary="Get analytics summary (voice-friendly)")
def get_analytics_summary(
    metric: Optional[str] = Query(None),
    days: int = Query(7, ge=1, le=365),
):
    summary_data = analytics.get_summary(metric=metric, days=days)
    return {
        "data": summary_data,
        "metadata": {"data_freshness": freshness_label(), "query_context": f"Summary of {metric or 'all metrics'}, last {days} days"},
        "voice_summary": generate_voice_summary("Analytics", [summary_data], 1),
    }


# ═══════════════════════════════════════════════════════════════════════
#  Tool Schema (inlined — no separate llm_service.py needed)
# ═══════════════════════════════════════════════════════════════════════

@router.get("/tools/schema", summary="LLM function calling tool definitions")
def get_tool_schema():
    return {
        "tools": crm.get_tool_definitions() + support.get_tool_definitions() + analytics.get_tool_definitions(),
        "description": "Available tools for querying business data via OpenAI-compatible function calling.",
    }
