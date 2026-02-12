"""
Salesforce connector implementation using OOP architecture.

This module provides the SalesforceConnector class which implements the
SaaSConnector interface for Salesforce data sources.
"""

import logging
import sys
import pandas as pd
from pathlib import Path
from typing import Dict
from collections import defaultdict

# Add parent directory to path to import utilities
sys.path.insert(0, str(Path(__file__).parent.parent))
from core import SaaSConnector

logger = logging.getLogger(__name__)


class SalesforceConnector(SaaSConnector):
    """
    Salesforce connector for Databricks Lakeflow Connect pipelines.

    Implements SaaS connector pattern with:
    - Single-level load balancing (pipelines only, no gateways)
    - OAuth connection management
    - Column filtering support (include/exclude)

    Required CSV columns:
    - source_database: Always 'Salesforce'
    - source_schema: Schema type ('standard' or 'custom')
    - source_table_name: Salesforce object name (e.g., 'Account', 'Contact')
    - target_catalog: Target Databricks catalog
    - target_schema: Target Databricks schema
    - target_table_name: Destination table name
    - connection_name: Databricks connection name for Salesforce

    Optional CSV columns:
    - project_name: Project identifier (default: 'salesforce_ingestion')
    - prefix: Grouping prefix (default: project_name)
    - subgroup: Subgroup identifier (default: '01')
    - schedule: Cron schedule (e.g., '0 0 * * *', default: '*/15 * * * *')
    - include_columns: Comma-separated list of columns to include (default: '')
    - exclude_columns: Comma-separated list of columns to exclude (default: '')
    - primary_keys: Comma-separated list of primary key columns (optional; supports composite keys)
    """

    @property
    def connector_type(self) -> str:
        """Return connector type identifier."""
        return 'salesforce'

    @property
    def required_columns(self) -> list:
        """
        Return required columns for Salesforce input CSV.

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
        Return default values for optional Salesforce columns.

        These values are used when columns are missing or empty.
        """
        return {
            'schedule': '*/15 * * * *'
        }

    @property
    def default_project_name(self) -> str:
        """Return default project name for Salesforce connector."""
        return 'sfdc_ingestion'

    @property
    def supported_scd_types(self) -> list:
        """Return supported SCD types for Salesforce connector."""
        return ["SCD_TYPE_1", "SCD_TYPE_2"]

    def _create_pipelines(self, df: pd.DataFrame, project_name: str) -> Dict:
        """
        Create pipeline YAML configuration from dataframe.

        Args:
            df: DataFrame with pipeline configuration
            project_name: Project name for resource naming

        Returns:
            Dictionary with pipeline YAML configuration
        """
        pipelines = {}

        # Group tables by pipeline_group
        groups = defaultdict(list)
        for idx, row in df.iterrows():
            groups[row['pipeline_group']].append(row)

        for pipeline_group in sorted(groups.keys()):
            group_tables = groups[pipeline_group]

            # Generate resource names
            names = self._generate_resource_names(pipeline_group)

            # Get catalog, schema, and connection_name from first table in group
            target_catalog = group_tables[0]['target_catalog']
            target_schema = group_tables[0]['target_schema']
            connection_name = group_tables[0]['connection_name']

            logger.debug(f"Pipeline {pipeline_group}: {len(group_tables)} tables -> {target_catalog}.{target_schema}")

            # Create pipeline definition
            pipeline_def = {
                "name": names['pipeline_name'],
                "catalog": target_catalog,
                "schema": target_schema,
                "ingestion_definition": {
                    "connection_name": connection_name,
                    "objects": []
                }
            }

            # Optional: tags (applied to the pipeline)
            tags = self._parse_tags(group_tables[0].get('tags'))
            if tags:
                pipeline_def["tags"] = tags

            # Add tables to this pipeline
            for item in group_tables:
                table_entry = {
                    "table": {
                        "source_schema": "objects",  # Salesforce uses 'objects' as source schema
                        "source_table": item["source_table_name"],
                        "destination_catalog": item["target_catalog"],
                        "destination_schema": item["target_schema"],
                        "destination_table": item["target_table_name"]
                    }
                }

                # Add table_configuration if include_columns or exclude_columns are specified
                table_config = {}

                if 'include_columns' in item and pd.notna(item['include_columns']) and item['include_columns'].strip():
                    include_cols = [col.strip() for col in str(item['include_columns']).split(',')]
                    table_config['include_columns'] = include_cols

                if 'exclude_columns' in item and pd.notna(item['exclude_columns']) and item['exclude_columns'].strip():
                    exclude_cols = [col.strip() for col in str(item['exclude_columns']).split(',')]
                    table_config['exclude_columns'] = exclude_cols

                # Optional: primary keys (supports composite keys)
                if (
                    'primary_keys' in item
                    and pd.notna(item['primary_keys'])
                    and str(item['primary_keys']).strip()
                ):
                    primary_keys = [
                        k.strip()
                        for k in str(item['primary_keys']).split(',')
                        if k.strip()
                    ]
                    if primary_keys:
                        table_config['primary_keys'] = primary_keys

                if table_config:
                    table_entry["table"]["table_configuration"] = table_config

                pipeline_def["ingestion_definition"]["objects"].append(table_entry)

            pipelines[names['pipeline_resource_name']] = pipeline_def

        return {'resources': {'pipelines': pipelines}}
