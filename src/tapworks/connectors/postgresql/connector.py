"""
PostgreSQL connector implementation.

This module provides the PostgreSQLConnector class which implements the
DatabaseConnector interface for PostgreSQL data sources.
"""

import logging
import pandas as pd
from typing import Dict

from tapworks.core import DatabaseConnector
from tapworks.core.exceptions import ValidationError

# Configure module logger
logger = logging.getLogger(__name__)


class PostgreSQLConnector(DatabaseConnector):
    """
    PostgreSQL connector for Databricks Lakeflow Connect pipelines.

    Implements database connector pattern with:
    - Two-level load balancing (gateways + pipelines)
    - Gateway configuration (catalog, schema, worker/driver types)
    - Connection management per row
    - Source configurations with replication slot/publication per source database

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
    - slot_name: PostgreSQL replication slot name per source database
    - publication_name: PostgreSQL publication name per source database

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
            "pipeline_catalog",
            "pipeline_schema",
            "slot_name",
            "publication_name",
        ]

    @property
    def default_values(self) -> dict:
        return {
            "schedule": "*/15 * * * *",
            "gateway_catalog": None,  # Will fall back to target_catalog
            "gateway_schema": None,  # Will fall back to target_schema
            "pipeline_catalog": None,
            "pipeline_schema": None,
            "gateway_worker_type": None,
            "gateway_driver_type": None,
            "slot_name": None,
            "publication_name": None,
        }

    @property
    def supported_scd_types(self) -> list:
        """Return supported SCD types for PostgreSQL connector."""
        return ["SCD_TYPE_1", "SCD_TYPE_2"]

    def _validate_generated_names(self, df: pd.DataFrame) -> None:
        """
        Validate generated names with PostgreSQL-specific slot configuration checks.

        Extends base database validation with:
        - Consistency: same source_database must have same slot_name and publication_name
        """
        super()._validate_generated_names(df)

        for pipeline_group, group_df in df.groupby('pipeline_group'):
            for source_db, db_df in group_df.groupby('source_database'):
                unique_slots = db_df['slot_name'].unique()
                if len(unique_slots) > 1:
                    raise ValidationError(
                        f"Pipeline group '{pipeline_group}': source database '{source_db}' has "
                        f"conflicting slot_name values: {list(unique_slots)}. "
                        f"All tables from the same source database must use the same slot_name."
                    )

                unique_pubs = db_df['publication_name'].unique()
                if len(unique_pubs) > 1:
                    raise ValidationError(
                        f"Pipeline group '{pipeline_group}': source database '{source_db}' has "
                        f"conflicting publication_name values: {list(unique_pubs)}. "
                        f"All tables from the same source database must use the same publication_name."
                    )

    def _create_pipelines(self, df: pd.DataFrame, project_name: str) -> Dict:
        """
        Create pipeline YAML with PostgreSQL source_configurations.

        Extends base database pipeline creation to inject source_configurations
        with slot_config for each unique source_database.
        """
        result = super()._create_pipelines(df, project_name)

        for pipeline_group, group_df in df.groupby('pipeline_group'):
            names = self._generate_resource_names(pipeline_group)
            pipeline_key = names['pipeline_resource_name']

            source_configs = []
            for source_db in group_df['source_database'].unique():
                db_rows = group_df[group_df['source_database'] == source_db]
                first_row = db_rows.iloc[0]

                source_configs.append({
                    'catalog': {
                        'source_catalog': source_db,
                        'postgres': {
                            'slot_config': {
                                'slot_name': str(first_row['slot_name']).strip(),
                                'publication_name': str(first_row['publication_name']).strip(),
                            }
                        }
                    }
                })

            result['resources']['pipelines'][pipeline_key]['ingestion_definition']['source_configurations'] = source_configs

        return result


