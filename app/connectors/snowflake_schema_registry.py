"""
Snowflake Schema Registry — defines table schemas and relationships
for multi-table SQL generation.

The LLM SQL generator uses this to understand what tables exist,
their columns, and how they relate via JOINs.

Currently only ORDERS exists in PIZZA_DB. When new tables are added
to Snowflake, add them here and the SQL generator will automatically
support JOINs across them.
"""

import logging
import re
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


# ── Table Schemas ──────────────────────────────────────────────────────
# Add new tables here as they are created in Snowflake.

TABLE_SCHEMAS: Dict[str, Dict[str, Any]] = {
    "ORDERS": {
        "columns": [
            {"name": "ORDER_ID", "type": "INTEGER", "description": "Unique order identifier"},
            {"name": "CUSTOMER_NAME", "type": "VARCHAR", "description": "Name of the customer"},
            {"name": "CUSTOMER_EMAIL", "type": "VARCHAR", "description": "Customer email address"},
            {"name": "PIZZA_NAME", "type": "VARCHAR", "description": "Name of the pizza ordered"},
            {"name": "QUANTITY", "type": "INTEGER", "description": "Number of pizzas ordered"},
            {"name": "PRICE", "type": "DECIMAL", "description": "Total price for this order line"},
            {"name": "ORDER_STATUS", "type": "VARCHAR", "description": "Status: preparing, out_for_delivery, delivered, cancelled"},
            {"name": "ORDER_DATE", "type": "TIMESTAMP", "description": "When the order was placed"},
            {"name": "PHONE_NUMBERS", "type": "VARCHAR", "description": "Customer phone number"},
        ],
        "primary_key": "ORDER_ID",
        "description": "Pizza orders with customer details and status",
    },
    # ── Future tables (uncomment when created in Snowflake) ────────
    # "CUSTOMERS": {
    #     "columns": [
    #         {"name": "CUSTOMER_ID", "type": "INTEGER", "description": "Unique customer identifier"},
    #         {"name": "CUSTOMER_NAME", "type": "VARCHAR", "description": "Full name of the customer"},
    #         {"name": "EMAIL", "type": "VARCHAR", "description": "Customer email address"},
    #         {"name": "PHONE", "type": "VARCHAR", "description": "Customer phone number"},
    #         {"name": "ADDRESS", "type": "VARCHAR", "description": "Delivery address"},
    #         {"name": "CITY", "type": "VARCHAR", "description": "City"},
    #         {"name": "LOYALTY_TIER", "type": "VARCHAR", "description": "Loyalty tier: bronze, silver, gold, platinum"},
    #         {"name": "CREATED_AT", "type": "TIMESTAMP", "description": "Account creation date"},
    #     ],
    #     "primary_key": "CUSTOMER_ID",
    #     "description": "Customer profiles with contact info and loyalty tier",
    # },
    # "ORDER_ITEMS": {
    #     "columns": [
    #         {"name": "ITEM_ID", "type": "INTEGER", "description": "Unique line item identifier"},
    #         {"name": "ORDER_ID", "type": "INTEGER", "description": "References ORDERS.ORDER_ID"},
    #         {"name": "PIZZA_NAME", "type": "VARCHAR", "description": "Name of the pizza"},
    #         {"name": "SIZE", "type": "VARCHAR", "description": "Pizza size: small, medium, large, extra_large"},
    #         {"name": "QUANTITY", "type": "INTEGER", "description": "Number of this item"},
    #         {"name": "UNIT_PRICE", "type": "DECIMAL", "description": "Price per unit"},
    #         {"name": "TOTAL_PRICE", "type": "DECIMAL", "description": "quantity * unit_price"},
    #     ],
    #     "primary_key": "ITEM_ID",
    #     "description": "Individual line items within an order",
    # },
}


# ── Table Relationships (for JOINs — populate when tables are added) ─

TABLE_RELATIONSHIPS: List[Dict[str, str]] = [
    # Uncomment when CUSTOMERS / ORDER_ITEMS tables exist:
    # {
    #     "left_table": "ORDERS",
    #     "left_column": "ORDER_ID",
    #     "right_table": "ORDER_ITEMS",
    #     "right_column": "ORDER_ID",
    #     "join_type": "LEFT JOIN",
    #     "description": "Each order has one or more line items",
    # },
    # {
    #     "left_table": "ORDERS",
    #     "left_column": "PHONE_NUMBERS",
    #     "right_table": "CUSTOMERS",
    #     "right_column": "PHONE",
    #     "join_type": "LEFT JOIN",
    #     "description": "Link orders to customer profiles via phone number",
    # },
]


# ── Known safe table names (for SQL injection prevention) ─────────────

ALLOWED_TABLES: Set[str] = set(TABLE_SCHEMAS.keys())


def validate_table_names(sql: str) -> bool:
    """
    Check that a SQL query only references known/allowed tables.

    Extracts table names from FROM and JOIN clauses and verifies
    they exist in ALLOWED_TABLES.

    Returns True if all tables are valid, False otherwise.
    """
    # Extract table names after FROM and JOIN keywords
    table_pattern = r'(?:FROM|JOIN)\s+(\w+)'
    found_tables = re.findall(table_pattern, sql, re.IGNORECASE)

    for table in found_tables:
        if table.upper() not in ALLOWED_TABLES:
            logger.warning(
                "SQL validation failed: unknown table '%s' in query: %s",
                table, sql,
            )
            return False

    return True


def get_schema_prompt() -> str:
    """
    Generate a comprehensive schema description for the LLM SQL generator.

    Includes table DDL, column descriptions, and relationships.
    Automatically adapts as new tables are added to TABLE_SCHEMAS.
    """
    sections = []

    sections.append("=== SNOWFLAKE DATABASE SCHEMA (PIZZA_DB.PUBLIC) ===\n")

    # Table definitions
    for table_name, schema in TABLE_SCHEMAS.items():
        sections.append(f"TABLE: {table_name}")
        sections.append(f"Description: {schema['description']}")
        sections.append(f"Primary Key: {schema['primary_key']}")
        sections.append("Columns:")
        for col in schema["columns"]:
            sections.append(
                f"  - {col['name']} ({col['type']}): {col['description']}"
            )
        sections.append("")

    # Relationships (if any tables have them)
    if TABLE_RELATIONSHIPS:
        sections.append("=== TABLE RELATIONSHIPS (for JOINs) ===\n")
        for rel in TABLE_RELATIONSHIPS:
            sections.append(
                f"  {rel['left_table']}.{rel['left_column']} → "
                f"{rel['right_table']}.{rel['right_column']} "
                f"({rel['join_type']}): {rel['description']}"
            )
        sections.append("")

    # Query examples
    sections.append("=== QUERY EXAMPLES ===\n")
    sections.append(
        "-- Get all orders for a customer by phone:\n"
        "SELECT * FROM ORDERS WHERE PHONE_NUMBERS = '1234567890' ORDER BY ORDER_DATE DESC\n"
    )
    sections.append(
        "-- Get order count and total spending:\n"
        "SELECT CUSTOMER_NAME, COUNT(*) AS total_orders, SUM(PRICE) AS total_spent\n"
        "FROM ORDERS GROUP BY CUSTOMER_NAME\n"
    )
    sections.append(
        "-- Filter by status:\n"
        "SELECT * FROM ORDERS WHERE ORDER_STATUS = 'delivered' AND PHONE_NUMBERS = '1234567890'\n"
    )
    sections.append(
        "-- Get most ordered pizzas:\n"
        "SELECT PIZZA_NAME, SUM(QUANTITY) AS total_qty FROM ORDERS\n"
        "WHERE PHONE_NUMBERS = '1234567890' GROUP BY PIZZA_NAME ORDER BY total_qty DESC\n"
    )

    return "\n".join(sections)


def get_table_names() -> List[str]:
    """Return list of all known table names."""
    return list(TABLE_SCHEMAS.keys())


def get_table_columns(table_name: str) -> List[str]:
    """Return column names for a specific table."""
    schema = TABLE_SCHEMAS.get(table_name.upper())
    if schema is None:
        return []
    return [col["name"] for col in schema["columns"]]
