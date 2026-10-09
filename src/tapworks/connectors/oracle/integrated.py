"""
Oracle integrated CDC connector implementation.

This module provides the OracleIntegratedConnector class, which combines
OracleSource (source rules) with IntegratedCDCConnector (CDC without a gateway).
"""

from tapworks.core import IntegratedCDCConnector

from .source import OracleSource


class OracleIntegratedConnector(OracleSource, IntegratedCDCConnector):
    """
    Oracle integrated CDC connector for Databricks Lakeflow Connect pipelines.

    Implements integrated CDC pattern with:
    - Single-level load balancing (pipelines only, no gateways)
    - Connection management per pipeline
    - connector_type CDC on the PREVIEW channel (Beta; requires workspace enablement)

    Required CSV columns:
    - source_database: Oracle service name (CDB$ROOT service name for multitenant databases)
    - source_schema: Source schema name (case must match Oracle, usually uppercase)
    - source_table_name: Table name to ingest (case must match Oracle, usually uppercase)
    - target_catalog: Target Databricks catalog
    - target_schema: Target Databricks schema
    - target_table_name: Destination table name
    - connection_name: Databricks connection name for Oracle
    - pipeline_catalog: Pipeline-level catalog for event log location
    - pipeline_schema: Pipeline-level schema for event log location

    Optional CSV columns:
    - classic_compute: true to run on classic compute (default: serverless)
    - pipeline_worker_type / pipeline_driver_type: Node types for classic compute
    - project_name: Project identifier
    - prefix: Grouping prefix (default: project_name)
    - subgroup: Subgroup identifier (default: none)
    - staging_catalog: Catalog for staged change data (default: target_catalog)
    - staging_schema: Schema for staged change data (default: target_schema)
    - schedule: Cron schedule (default: 0 * * * *)
    - scd_type: SCD_TYPE_1 or SCD_TYPE_2
    - include_columns / exclude_columns: Comma-separated column lists
    """

    @property
    def connector_type(self) -> str:
        """Return connector type identifier."""
        return 'oracle_integrated'

    @property
    def required_columns(self) -> list:
        """
        Return required columns for Oracle input CSV.

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
        Return default values for optional Oracle columns.

        Each integrated CDC update runs for about 30 minutes, so the default
        schedule is hourly, as recommended in the Lakeflow Connect docs.
        """
        return {
            'schedule': '0 * * * *',
            'staging_catalog': None,  # Will fall back to target_catalog
            'staging_schema': None,   # Will fall back to target_schema
            'classic_compute': None,  # Empty means serverless
            'pipeline_worker_type': None,
            'pipeline_driver_type': None,
            'pipeline_catalog': None,
            'pipeline_schema': None,
        }

    @property
    def supported_scd_types(self) -> list:
        """Return supported SCD types for Oracle connector."""
        return ["SCD_TYPE_1", "SCD_TYPE_2"]
