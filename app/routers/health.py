"""
Health check endpoint.
"""

from fastapi import APIRouter

router = APIRouter(tags=["Health"])


@router.get("/health", summary="Application health check")
def health_check():
    return {"status": "ok", "service": "Universal Data Connector", "version": "1.0.0"}
