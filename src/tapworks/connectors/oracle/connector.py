"""
Oracle connector implementation.

This module provides the OracleConnector class which implements the
IntegratedCDCConnector interface for Oracle data sources.
"""

import logging
import pandas as pd

from tapworks.core import IntegratedCDCConnector

# Configure module logger
logger = logging.getLogger(__name__)

# Columns holding Oracle identifiers, whose case must match how Oracle stores them
IDENTIFIER_COLUMNS = ['source_database', 'source_schema', 'source_table_name']


def warn_on_lowercase_identifiers(df: pd.DataFrame) -> None:
    """
    Log a warning for Oracle identifiers that contain lowercase letters.

    Oracle stores unquoted identifiers in uppercase, and Lakeflow Connect requires
    the case to match. Lowercase is only correct for quoted identifiers, so this
    warns rather than fails.
    """
    for column in IDENTIFIER_COLUMNS:
        if column not in df.columns:
            continue
        values = df[column].dropna().astype(str)
        lowercase = sorted(set(values[values != values.str.upper()]))
        if lowercase:
            logger.warning(
                f"{column} has values with lowercase letters: {lowercase[:5]}"
                f"{' ...' if len(lowercase) > 5 else ''}. Oracle stores unquoted identifiers "
                f"in uppercase; the case must match how Oracle stores the identifier."
            )


class OracleConnector(IntegratedCDCConnector):
    """
    Oracle connector for Databricks Lakeflow Connect pipelines (integrated CDC).

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
        return 'oracle'

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
            'pipeline_catalog': None,
            'pipeline_schema': None,
        }

    @property
    def supported_scd_types(self) -> list:
        """Return supported SCD types for Oracle connector."""
        return ["SCD_TYPE_1", "SCD_TYPE_2"]

    def _apply_connector_specific_normalization(self, df: pd.DataFrame) -> pd.DataFrame:
        """Warn about identifiers whose case may not match Oracle."""
        df = super()._apply_connector_specific_normalization(df)
        warn_on_lowercase_identifiers(df)
        return df
