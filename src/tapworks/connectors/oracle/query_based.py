"""
Oracle query-based connector implementation.

This module provides the OracleQueryBasedConnector class, which combines
OracleSource (source rules) with QueryBasedConnector (query-based ingestion).
"""

from tapworks.core import QueryBasedConnector

from .source import OracleSource


class OracleQueryBasedConnector(OracleSource, QueryBasedConnector):
    """
    Oracle query-based connector for Databricks Lakeflow Connect pipelines.

    Implements query-based pattern with:
    - Single-level load balancing (pipelines only, no gateways)
    - Connection management per pipeline
    - connector_type QUERY_BASED; tables with cursor columns are read incrementally,
      tables without one are read as full snapshots
    - No Oracle log configuration needed; does not capture intermediate row states

    Required CSV columns:
    - source_database: Oracle service name
    - source_schema: Source schema name (case must match Oracle, usually uppercase)
    - source_table_name: Table name to ingest (case must match Oracle, usually uppercase)
    - target_catalog: Target Databricks catalog
    - target_schema: Target Databricks schema
    - target_table_name: Destination table name
    - connection_name: Databricks connection name for Oracle
    - pipeline_catalog: Pipeline-level catalog for event log location
    - pipeline_schema: Pipeline-level schema for event log location

    Optional CSV columns:
    - cursor_columns: Comma-separated monotonically increasing columns (e.g., 'UPDATED_AT');
      without one, the table is read as a full snapshot
    - project_name: Project identifier
    - prefix: Grouping prefix (default: project_name)
    - subgroup: Subgroup identifier (default: none)
    - schedule: Cron schedule (default: 0 * * * *)
    - scd_type: SCD_TYPE_1, SCD_TYPE_2, or APPEND_ONLY
    - primary_keys: Comma-separated primary key columns
    - deletion_condition: SQL condition marking soft-deleted rows (e.g., "IS_DELETED = 1")
    - include_columns / exclude_columns: Comma-separated column lists
    """

    @property
    def connector_type(self) -> str:
        """Return connector type identifier."""
        return 'oracle_query_based'

    @property
    def required_columns(self) -> list:
        """
        Return required columns for Oracle query-based input CSV.

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
        Return default values for optional Oracle query-based columns.

        Hourly, as in the Lakeflow Connect query-based job example.
        """
        return {
            'schedule': '0 * * * *',
            'pipeline_catalog': None,
            'pipeline_schema': None,
            'cursor_columns': None,
        }

    @property
    def supported_scd_types(self) -> list:
        """Return supported SCD types for Oracle query-based connector."""
        return ["SCD_TYPE_1", "SCD_TYPE_2", "APPEND_ONLY"]
