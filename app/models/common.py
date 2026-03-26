"""
Shared Pydantic models used across all data sources.
Includes response envelope, metadata, pagination, and voice optimization types.
"""

from pydantic import BaseModel, Field
from typing import Any, List, Optional
from enum import Enum
from datetime import datetime


class DataType(str, Enum):
    """Classification of the shape / nature of the returned data."""
    TABULAR = "tabular"
    TIME_SERIES = "time_series"
    HIERARCHICAL = "hierarchical"
    SUMMARY = "summary"
    EMPTY = "empty"


class Metadata(BaseModel):
    """Rich metadata returned alongside every data response."""
    total_results: int = Field(..., description="Total records matching the query")
    returned_results: int = Field(..., description="Number of records in this page")
    page: int = Field(1, description="Current page number")
    total_pages: int = Field(1, description="Total pages available")
    data_type: DataType = Field(DataType.TABULAR, description="Shape of the data")
    data_freshness: str = Field(
        ..., description="Human-readable freshness indicator, e.g. 'Data as of 2 minutes ago'"
    )
    voice_hint: Optional[str] = Field(
        None,
        description="Short spoken summary optimised for voice, e.g. 'You have 5 open tickets'",
    )
    query_context: Optional[str] = Field(
        None,
        description="Context about what was queried, e.g. 'Showing active customers'",
    )


class DataResponse(BaseModel):
    """Unified response envelope for every data endpoint."""
    data: List[Any] = Field(default_factory=list)
    metadata: Metadata
    voice_summary: Optional[str] = Field(
        None,
        description="A concise, natural-language summary suitable for TTS playback",
    )


class ErrorResponse(BaseModel):
    """Structured error response."""
    error: str
    detail: Optional[str] = None
    source: Optional[str] = None
