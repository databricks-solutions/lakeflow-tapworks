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

    Optional CSV columns:
    - project_name: Project identifier (default: 'postgres_ingestion')
    - prefix: Grouping prefix (default: project_name)
    - subgroup: Subgroup identifier (default: '01')
    - gateway_catalog: Gateway storage catalog (default: target_catalog)
    - gateway_schema: Gateway storage schema (default: target_schema)
    - gateway_worker_type: Worker node type (default: None)
    - gateway_driver_type: Driver node type (default: None)
    - schedule: Cron schedule (default: */15 * * * *)
    - slot_name: PostgreSQL replication slot name per source database (default: None)
    - publication_name: PostgreSQL publication name per source database (default: databricks_publication)
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
        - Warning if no slot_name is provided (required for deployment)
        - All-or-none rule: within a pipeline group, either all source databases
          have slot_name or none do
        - Consistency: same source_database must have same slot_name and publication_name
        """
        super()._validate_generated_names(df)

        if 'slot_name' not in df.columns:
            logger.warning(
                "No 'slot_name' column provided. PostgreSQL ingestion pipelines require "
                "source_configurations with slot_config for deployment. "
                "Add 'slot_name' and optionally 'publication_name' columns to your CSV."
            )
            return

        has_slot = df['slot_name'].notna() & (df['slot_name'].astype(str).str.strip() != '')
        if not has_slot.any():
            logger.warning(
                "No 'slot_name' values provided. PostgreSQL ingestion pipelines require "
                "source_configurations with slot_config for deployment."
            )
            return

        for pipeline_group, group_df in df.groupby('pipeline_group'):
            group_has_slot = group_df['slot_name'].notna() & (group_df['slot_name'].astype(str).str.strip() != '')
            group_missing_slot = ~group_has_slot

            # All-or-none: within a pipeline, all source_databases must have slot config
            if group_has_slot.any() and group_missing_slot.any():
                missing_dbs = group_df.loc[group_missing_slot, 'source_database'].unique()
                raise ValidationError(
                    f"Pipeline group '{pipeline_group}': slot_name is set for some rows but missing for "
                    f"source databases: {list(missing_dbs)}. PostgreSQL requires slot configuration "
                    f"for all replication catalogs or none."
                )

            if not group_has_slot.any():
                continue

            # Consistency: same source_database must have same slot_name and publication_name
            for source_db, db_df in group_df.groupby('source_database'):
                slot_values = db_df['slot_name'].dropna()
                slot_values = slot_values[slot_values.astype(str).str.strip() != '']
                unique_slots = slot_values.unique()
                if len(unique_slots) > 1:
                    raise ValidationError(
                        f"Pipeline group '{pipeline_group}': source database '{source_db}' has "
                        f"conflicting slot_name values: {list(unique_slots)}. "
                        f"All tables from the same source database must use the same slot_name."
                    )

                if 'publication_name' in db_df.columns:
                    pub_values = db_df['publication_name'].dropna()
                    pub_values = pub_values[pub_values.astype(str).str.strip() != '']
                    unique_pubs = pub_values.unique()
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
        with slot_config for each unique source_database when slot_name is provided.
        """
        result = super()._create_pipelines(df, project_name)

        if 'slot_name' not in df.columns:
            return result

        for pipeline_group, group_df in df.groupby('pipeline_group'):
            names = self._generate_resource_names(pipeline_group)
            pipeline_key = names['pipeline_resource_name']

            has_slot = group_df['slot_name'].notna() & (group_df['slot_name'].astype(str).str.strip() != '')
            if not has_slot.any():
                continue

            source_configs = []
            for source_db in group_df['source_database'].unique():
                db_rows = group_df[group_df['source_database'] == source_db]
                slot_row = db_rows[db_rows['slot_name'].notna() & (db_rows['slot_name'].astype(str).str.strip() != '')].iloc[0]
                slot_name = str(slot_row['slot_name']).strip()

                publication_name = 'databricks_publication'
                if 'publication_name' in db_rows.columns:
                    pub_val = slot_row.get('publication_name')
                    if self._is_value_set(pub_val):
                        publication_name = str(pub_val).strip()

                source_configs.append({
                    'catalog': {
                        'source_catalog': source_db,
                        'postgres': {
                            'slot_config': {
                                'slot_name': slot_name,
                                'publication_name': publication_name
                            }
                        }
                    }
                })

            result['resources']['pipelines'][pipeline_key]['ingestion_definition']['source_configurations'] = source_configs

        return result


