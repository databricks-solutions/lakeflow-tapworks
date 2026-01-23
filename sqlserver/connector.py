"""
SQL Server connector implementation using OOP architecture.

This module provides the SQLServerConnector class which implements the
DatabaseConnector interface for SQL Server data sources.
"""

import sys
from pathlib import Path

# Add parent directory to path to import utilities
sys.path.insert(0, str(Path(__file__).parent.parent))
from utilities.connectors import DatabaseConnector


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
        return 'sqlserver'

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

    def generate_yaml_files(self, df, output_dir, targets):
        """
        Generate YAML files for SQL Server connector with gateways.

        Creates a complete DAB structure:
        - databricks.yml (root configuration)
        - resources/gateways.yml (gateway definitions with cluster config)
        - resources/pipelines.yml (pipeline definitions with table mappings)
        - resources/jobs.yml (scheduled jobs)

        Args:
            df: DataFrame with pipeline configuration including 'gateway' column
            output_dir: Output directory for DAB files
            targets: Dictionary of target environments
        """
        # Import SQL Server specific YAML generator
        from deployment.connector_settings_generator import generate_yaml_files

        generate_yaml_files(df=df, output_dir=output_dir, targets=targets)
