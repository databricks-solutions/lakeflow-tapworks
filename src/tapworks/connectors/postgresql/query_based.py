"""
PostgreSQL query-based connector implementation.

This module provides the PostgreSQLQueryBasedConnector class, which combines
PostgreSQLSource (source rules) with QueryBasedConnector (query-based ingestion).
"""

from tapworks.core import QueryBasedConnector

from .source import PostgreSQLSource


class PostgreSQLQueryBasedConnector(PostgreSQLSource, QueryBasedConnector):
    """
    PostgreSQL query-based connector for Databricks Lakeflow Connect pipelines.

    Implements query-based pattern with:
    - Single-level load balancing (pipelines only, no gateways)
    - Connection management per pipeline
    - connector_type QUERY_BASED; tables with cursor columns are read incrementally,
      tables without one are read as full snapshots
    - No logical replication, replication slot, or publication needed

    Required CSV columns:
    - source_database: Source PostgreSQL database name
    - source_schema: Source schema name (usually 'public')
    - source_table_name: Table name to ingest
    - target_catalog: Target Databricks catalog
    - target_schema: Target Databricks schema
    - target_table_name: Destination table name
    - connection_name: Databricks connection name for PostgreSQL
    - pipeline_catalog: Pipeline-level catalog for event log location
    - pipeline_schema: Pipeline-level schema for event log location

    Optional CSV columns:
    - cursor_columns: Comma-separated monotonically increasing columns (e.g., 'updated_at');
      without one, the table is read as a full snapshot
    - primary_keys: Comma-separated primary key columns; required for SCD_TYPE_1/SCD_TYPE_2
      tables that have no primary key in PostgreSQL
    - classic_compute: true to run on classic compute (default: serverless)
    - pipeline_worker_type / pipeline_driver_type: Node types for classic compute
    - project_name: Project identifier
    - prefix: Grouping prefix (default: project_name)
    - subgroup: Subgroup identifier (default: none)
    - schedule: Cron schedule (default: 0 * * * *)
    - scd_type: SCD_TYPE_1, SCD_TYPE_2, or APPEND_ONLY
    - deletion_condition: SQL condition marking soft-deleted rows (e.g., "is_deleted = true")
    - include_columns / exclude_columns: Comma-separated column lists
    """

    @property
    def connector_type(self) -> str:
        """Return connector type identifier."""
        return 'postgresql_query_based'

    @property
    def required_columns(self) -> list:
        """
        Return required columns for PostgreSQL query-based input CSV.

        All these columns must be present and non-empty in the input.
        """
        return [
            'source_database',
            'source_schema',
            'source_table_name',
            'target_catalog',
            'target_schema',
            'target_table_name',
            'connection_name',
            'pipeline_catalog',
            'pipeline_schema'
        ]

    @property
    def default_values(self) -> dict:
        """
        Return default values for optional PostgreSQL query-based columns.

        Same as the Oracle query-based connector: hourly, serverless, snapshot
        reads unless cursor_columns is set.
        """
        return {
            'schedule': '0 * * * *',
            'pipeline_catalog': None,
            'pipeline_schema': None,
            'cursor_columns': None,
            'classic_compute': None,  # Empty means serverless
            'pipeline_worker_type': None,
            'pipeline_driver_type': None,
        }

    @property
    def supported_scd_types(self) -> list:
        """Return supported SCD types for PostgreSQL query-based connector."""
        return ["SCD_TYPE_1", "SCD_TYPE_2", "APPEND_ONLY"]
