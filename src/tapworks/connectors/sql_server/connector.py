"""
SQL Server connector implementation.

This module provides the SQLServerConnector class which implements the
DatabaseConnector interface for SQL Server data sources.
"""

import logging
from typing import Dict

from tapworks.core import DatabaseConnector

# Configure module logger
logger = logging.getLogger(__name__)


class SQLServerConnector(DatabaseConnector):
    """
    SQL Server connector for Databricks Lakeflow Connect pipelines.

    Implements database connector pattern with:
    - Two-level load balancing (gateways + pipelines)
    - Gateway configuration (catalog, schema, worker/driver types)
    - Connection management per row

    Required CSV columns:
    - source_database: Source SQL Server database name
    - source_schema: Source schema name (usually 'dbo')
    - source_table_name: Table name to ingest
    - target_catalog: Target Databricks catalog
    - target_schema: Target Databricks schema
    - target_table_name: Destination table name
    - connection_name: Databricks connection name for SQL Server

    Optional CSV columns:
    - project_name: Project identifier (default: 'sqlserver_ingestion')
    - prefix: Grouping prefix (default: project_name)
    - subgroup: Subgroup identifier (default: '01')
    - gateway_catalog: Gateway storage catalog (default: target_catalog)
    - gateway_schema: Gateway storage schema (default: target_schema)
    - gateway_worker_type: Worker node type (e.g., 'm5d.large', default: None)
    - gateway_driver_type: Driver node type (e.g., 'c5a.8xlarge', default: None)
    - schedule: Cron schedule (e.g., '0 0 * * *', default: None)
    """

    @property
    def connector_type(self) -> str:
        """Return connector type identifier."""
        return 'sql_server'

    @property
    def required_columns(self) -> list:
        """
        Return required columns for SQL Server input CSV.

        All these columns must be present and non-empty in the input.
        """
        return [
            'source_database',
            'source_schema',
            'source_table_name',
            'target_catalog',
            'target_schema',
            'target_table_name',
            'connection_name'
        ]

    @property
    def default_values(self) -> dict:
        """
        Return default values for optional SQL Server columns.

        These values are used when columns are missing or empty.
        """
        return {
            'schedule': '*/15 * * * *',
            'gateway_catalog': None,  # Will fall back to target_catalog
            'gateway_schema': None,   # Will fall back to target_schema
            'gateway_worker_type': None,
            'gateway_driver_type': None
        }

    @property
    def supported_scd_types(self) -> list:
        """Return supported SCD types for SQL Server connector."""
        return ["SCD_TYPE_1", "SCD_TYPE_2"]

