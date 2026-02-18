"""
Google Analytics 4 connector implementation using OOP architecture.

This module provides the GoogleAnalyticsConnector class which implements the
SaaSConnector interface for Google Analytics 4 data sources.
"""

import logging
import sys
import pandas as pd
from pathlib import Path
from typing import Dict
from collections import defaultdict

# Add parent directory to path to import utilities
sys.path.insert(0, str(Path(__file__).parent.parent))
from tapworks.core import SaaSConnector

logger = logging.getLogger(__name__)


class GoogleAnalyticsConnector(SaaSConnector):
    """
    Google Analytics 4 connector for Databricks Lakeflow Connect pipelines.

    Implements SaaS connector pattern with:
    - Single-level load balancing (pipelines only, no gateways)
    - BigQuery integration
    - Multiple tables per property support

    Required CSV columns:
    - source_catalog: GCP project ID (e.g., 'my-gcp-project')
    - source_schema: GA4 property ID (e.g., 'analytics_123456789')
    - tables: Comma-separated list of tables (e.g., 'events,events_intraday,users')
    - target_catalog: Target Databricks catalog
    - target_schema: Target Databricks schema
    - connection_name: Databricks connection name for BigQuery

    Optional CSV columns:
    - project_name: Project identifier (default: 'ga4_ingestion')
    - prefix: Grouping prefix (default: project_name)
    - subgroup: Subgroup identifier (default: '01')
    - schedule: Cron schedule (e.g., '0 */6 * * *', default: '0 */6 * * *')
    """

    @property
    def connector_type(self) -> str:
        """Return connector type identifier."""
        return 'ga4'

    @property
    def required_columns(self) -> list:
        """
        Return required columns for GA4 input CSV.

        All these columns must be present and non-empty in the input.
        """
        return [
            'source_catalog',
            'source_schema',
            'tables',
            'target_catalog',
            'target_schema',
            'connection_name'
        ]

    @property
    def default_values(self) -> dict:
        """
        Return default values for optional GA4 columns.

        These values are used when columns are missing or empty.
        """
        return {
            'schedule': '0 */6 * * *'  # Every 6 hours by default
        }

    @property
    def default_project_name(self) -> str:
        """Return default project name for GA4 connector."""
        return 'ga4_ingestion'

    @property
    def supported_scd_types(self) -> list:
        """Return supported SCD types for GA4 connector."""
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
        # Call parent to validate pipeline consistency
        super()._create_pipelines(df, project_name)

        pipelines = {}

        # Group properties by pipeline_group
        groups = defaultdict(list)
        for idx, row in df.iterrows():
            groups[row['pipeline_group']].append(row)

        for pipeline_group in sorted(groups.keys()):
            group_properties = groups[pipeline_group]

            # Generate resource names
            names = self._generate_resource_names(pipeline_group)

            # Get catalog, schema, and connection_name from first property in group
            target_catalog = group_properties[0]['target_catalog']
            target_schema = group_properties[0]['target_schema']
            connection_name = group_properties[0]['connection_name']

            logger.debug(f"Pipeline {pipeline_group}: {len(group_properties)} properties -> {target_catalog}.{target_schema}")

            # Build ingestion objects list
            ingestion_objects = []
            for row in group_properties:
                source_catalog = row['source_catalog']  # GCP project
                source_schema = row['source_schema']    # GA4 property
                tables_str = row['tables']               # Comma-separated table list

                # Parse tables
                if pd.notna(tables_str):
                    tables = [t.strip() for t in str(tables_str).split(',')]
                else:
                    tables = ['events', 'events_intraday', 'users']  # Default GA4 tables

                # Create table entries for each GA4 table
                for table in tables:
                    table_obj = {
                        "table": {
                            "source_catalog": source_catalog,
                            "source_schema": source_schema,
                            "source_table": table,
                            "destination_catalog": row['target_catalog'],
                            "destination_schema": row['target_schema'],
                            "destination_table": f"{source_schema}_{table}"
                        }
                    }
                    ingestion_objects.append(table_obj)

            pipeline_def = {
                "name": names["pipeline_name"],
                "catalog": target_catalog,
                "schema": target_schema,
                "ingestion_definition": {
                    "connection_name": connection_name,
                    "objects": ingestion_objects,
                },
            }

            # Optional: tags (applied to the pipeline)
            tags = self._parse_tags(group_properties[0].get("tags"))
            if tags:
                pipeline_def["tags"] = tags

            # Add pipeline
            pipelines[names["pipeline_resource_name"]] = pipeline_def

        return {'resources': {'pipelines': pipelines}}
