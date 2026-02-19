"""
ServiceNow connector implementation.

This module provides the ServiceNowConnector class which implements the
SaaSConnector interface for ServiceNow data sources.
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


class ServiceNowConnector(SaaSConnector):
    """
    ServiceNow connector for Databricks Lakeflow Connect pipelines.

    Implements SaaS connector pattern with:
    - Single-level load balancing (pipelines only, no gateways)
    - OAuth connection management
    - Column filtering support (include/exclude)
    - SCD type configuration (SCD_TYPE_1 or SCD_TYPE_2)

    Required CSV columns:
    - source_database: Always 'SERVICENOW'
    - source_schema: Schema name (typically 'default' for standard tables)
    - source_table_name: ServiceNow table name (e.g., 'sys_user', 'incident')
    - target_catalog: Target Databricks catalog
    - target_schema: Target Databricks schema
    - target_table_name: Destination table name
    - connection_name: Databricks connection name for ServiceNow

    Optional CSV columns:
    - project_name: Project identifier (default: 'servicenow_ingestion')
    - prefix: Grouping prefix (default: project_name)
    - subgroup: Subgroup identifier (default: '01')
    - schedule: Cron schedule (e.g., '*/15 * * * *', default: '*/15 * * * *')
    - include_columns: Comma-separated list of columns to include (default: '')
    - exclude_columns: Comma-separated list of columns to exclude (default: '')
    - scd_type: SCD type (SCD_TYPE_1 or SCD_TYPE_2, optional)
    """

    @property
    def connector_type(self) -> str:
        """Return connector type identifier."""
        return "servicenow"

    @property
    def required_columns(self) -> list:
        """
        Return required columns for ServiceNow input CSV.

        All these columns must be present and non-empty in the input.
        """
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
        """
        Return default values for optional ServiceNow columns.

        These values are used when columns are missing or empty.
        """
        return {"schedule": "*/15 * * * *"}

    @property
    def default_project_name(self) -> str:
        """Return default project name for ServiceNow connector."""
        return "servicenow_ingestion"

    @property
    def supported_scd_types(self) -> list:
        """Return supported SCD types for ServiceNow connector."""
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

        # Group tables by pipeline_group
        groups = defaultdict(list)
        for idx, row in df.iterrows():
            groups[row["pipeline_group"]].append(row)

        for pipeline_group in sorted(groups.keys()):
            group_tables = groups[pipeline_group]

            # Generate resource names
            names = self._generate_resource_names(pipeline_group)

            # Get catalog, schema, and connection_name from first table in group
            target_catalog = group_tables[0]["target_catalog"]
            target_schema = group_tables[0]["target_schema"]
            connection_name = group_tables[0]["connection_name"]

            logger.debug(f"Pipeline {pipeline_group}: {len(group_tables)} tables -> {target_catalog}.{target_schema}")

            # Create pipeline definition
            pipeline_def = {
                "name": names["pipeline_name"],
                "catalog": target_catalog,
                "schema": target_schema,
                "ingestion_definition": {
                    "connection_name": connection_name,
                    "objects": [],
                },
            }

            # Optional: tags (applied to the pipeline)
            tags = self._parse_tags(group_tables[0].get("tags"))
            if tags:
                pipeline_def["tags"] = tags

            # Add tables to this pipeline
            for item in group_tables:
                table_entry = {
                    "table": {
                        "source_schema": item["source_schema"],
                        "source_table": item["source_table_name"],
                        "destination_catalog": item["target_catalog"],
                        "destination_schema": item["target_schema"],
                        "destination_table": item["target_table_name"],
                    }
                }

                # Add table_configuration if include_columns, exclude_columns, or scd_type are specified
                table_config = {}

                if (
                    "include_columns" in item
                    and pd.notna(item["include_columns"])
                    and item["include_columns"].strip()
                ):
                    include_cols = [
                        col.strip() for col in str(item["include_columns"]).split(",")
                    ]
                    table_config["include_columns"] = include_cols

                if (
                    "exclude_columns" in item
                    and pd.notna(item["exclude_columns"])
                    and item["exclude_columns"].strip()
                ):
                    exclude_cols = [
                        col.strip() for col in str(item["exclude_columns"]).split(",")
                    ]
                    table_config["exclude_columns"] = exclude_cols

                # Add SCD type if specified and valid
                scd_type = self._validate_scd_type(
                    item.get("scd_type"), item["source_table_name"]
                )
                if scd_type:
                    table_config["scd_type"] = scd_type

                if table_config:
                    table_entry["table"]["table_configuration"] = table_config

                pipeline_def["ingestion_definition"]["objects"].append(table_entry)

            pipelines[names["pipeline_resource_name"]] = pipeline_def

        return {"resources": {"pipelines": pipelines}}
