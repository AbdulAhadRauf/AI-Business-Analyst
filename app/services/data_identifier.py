"""
Data type identifier – classifies the shape of data returned by connectors.
"""

from typing import Any, Dict, List
from app.models.common import DataType


def identify_data_type(data: List[Dict[str, Any]]) -> DataType:
    """
    Inspect the first record's fields to classify the data shape.
    """
    if not data:
        return DataType.EMPTY

    sample = data[0]

    # Time-series: has a 'date' field and a 'value' or 'metric' field
    if "date" in sample and ("value" in sample or "metric" in sample):
        return DataType.TIME_SERIES

    # Hierarchical: has nested dicts or lists inside
    if any(isinstance(v, (dict, list)) for v in sample.values()):
        return DataType.HIERARCHICAL

    # Tabular: flat key-value records (CRM, support tickets, etc.)
    return DataType.TABULAR
