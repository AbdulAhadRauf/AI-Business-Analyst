"""
FastAPI application entry point.
Mounts routers, sets up CORS and logging.

This is the REST API server. For the voice interface,
run src/fastrtc_data_stream.py instead.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routers import health, data
from app.utils.logging import configure_logging
from app.config import settings

configure_logging()

app = FastAPI(
    title=settings.APP_NAME,
    description=(
        "A production-quality Universal Data Connector providing a unified "
        "interface for an LLM to access CRM, Support Ticket, and Analytics "
        "data through function calling. Includes a real-time voice conversation "
        "interface powered by FastRTC and Groq."
    ),
    version="1.0.0",
)

# ── CORS ────────────────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Routers ─────────────────────────────────────────────────────────────
app.include_router(health.router)
app.include_router(data.router)
