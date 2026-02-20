"""
PostgreSQL connector implementation.

This module provides the PostgreSQLConnector class which implements the
DatabaseConnector interface for PostgreSQL data sources.
"""

import logging
from typing import Dict

from tapworks.core import DatabaseConnector

# Configure module logger
logger = logging.getLogger(__name__)


class PostgreSQLConnector(DatabaseConnector):
    """
    PostgreSQL connector for Databricks Lakeflow Connect pipelines.

    Implements database connector pattern with:
    - Two-level load balancing (gateways + pipelines)
    - Gateway configuration (catalog, schema, worker/driver types)
    - Connection management per row

    Required CSV columns:
    - source_database: Source PostgreSQL database name
    - source_schema: Source schema name (usually 'public')
    - source_table_name: Table name to ingest
    - target_catalog: Target Databricks catalog
    - target_schema: Target Databricks schema
    - target_table_name: Destination table name
    - connection_name: Databricks connection name for PostgreSQL

    Optional CSV columns:
    - project_name: Project identifier (default: 'postgres_ingestion')
    - prefix: Grouping prefix (default: project_name)
    - subgroup: Subgroup identifier (default: '01')
    - gateway_catalog: Gateway storage catalog (default: target_catalog)
    - gateway_schema: Gateway storage schema (default: target_schema)
    - gateway_worker_type: Worker node type (default: None)
    - gateway_driver_type: Driver node type (default: None)
    - schedule: Cron schedule (default: */15 * * * *)
    """

    @property
    def connector_type(self) -> str:
        return "postgresql"

    @property
    def required_columns(self) -> list:
        return [
            "source_database",
            "source_schema",
            "source_table_name",
            "target_catalog",
            "target_schema",
            "target_table_name",
            "connection_name",
        ]

    @property
    def default_values(self) -> dict:
        return {
            "schedule": "*/15 * * * *",
            "gateway_catalog": None,  # Will fall back to target_catalog
            "gateway_schema": None,  # Will fall back to target_schema
            "gateway_worker_type": None,
            "gateway_driver_type": None,
        }

    @property
    def supported_scd_types(self) -> list:
        """Return supported SCD types for PostgreSQL connector."""
        return ["SCD_TYPE_1", "SCD_TYPE_2"]


