"""
Workday Reports connector implementation using OOP architecture.

This module provides the WorkdayReportsConnector class which implements the
SaaSConnector interface for Workday Reports data sources.
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


class WorkdayReportsConnector(SaaSConnector):
    """
    Workday Reports connector for Databricks Lakeflow Connect pipelines.

    Implements SaaS connector pattern with:
    - Single-level load balancing (pipelines only, no gateways)
    - OAuth connection management
    - Column filtering support (include/exclude)
    - SCD type configuration (SCD_TYPE_1 or SCD_TYPE_2)
    - Primary key configuration (required for Workday reports)

    Required CSV columns:
    - source_url: Workday report URL (e.g., 'https://wd2-impl-services1.workday.com/ccx/service/...')
    - target_catalog: Target Databricks catalog
    - target_schema: Target Databricks schema
    - target_table_name: Destination table name
    - connection_name: Databricks connection name for Workday
    - primary_keys: Comma-separated list of primary key columns

    Optional CSV columns:
    - project_name: Project identifier (default: 'workday_reports_ingestion')
    - prefix: Grouping prefix (default: project_name)
    - priority: Priority/subgroup identifier (default: '01')
    - schedule: Cron schedule (e.g., '0 */6 * * *', default: '0 */6 * * *')
    - include_columns: Comma-separated list of columns to include (default: '')
    - exclude_columns: Comma-separated list of columns to exclude (default: '')
    - scd_type: SCD type (SCD_TYPE_1 or SCD_TYPE_2, default: 'SCD_TYPE_1')
    """

    @property
    def connector_type(self) -> str:
        """Return connector type identifier."""
        return "workday_reports"

    @property
    def required_columns(self) -> list:
        """
        Return required columns for Workday Reports input CSV.

        All these columns must be present and non-empty in the input.
        """
        return [
            "source_url",
            "target_catalog",
            "target_schema",
            "target_table_name",
            "connection_name",
            "primary_keys",
        ]

    @property
    def default_values(self) -> dict:
        """
        Return default values for optional Workday Reports columns.

        These values are used when columns are missing or empty.
        """
        return {
            "schedule": "0 */6 * * *",  # Every 6 hours by default
            "scd_type": "SCD_TYPE_1",
        }

    @property
    def default_project_name(self) -> str:
        """Return default project name for Workday Reports connector."""
        return "workday_reports_ingestion"

    def _apply_connector_specific_normalization(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply Workday Reports-specific normalization including priority mapping.

        Maps 'priority' column to 'subgroup' if present.
        """
        # Call parent normalization first
        df = super()._apply_connector_specific_normalization(df)

        # Map priority to subgroup if priority column exists
        if "priority" in df.columns:
            # Use priority as subgroup if subgroup wasn't explicitly set
            mask = (df["subgroup"] == "01") | df["subgroup"].isna()
            df.loc[mask, "subgroup"] = df.loc[mask, "priority"].astype(str).str.zfill(2)

        return df

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

        # Group reports by pipeline_group
        groups = defaultdict(list)
        for idx, row in df.iterrows():
            groups[row["pipeline_group"]].append(row)

        for pipeline_group in sorted(groups.keys()):
            group_reports = groups[pipeline_group]

            # Generate resource names
            names = self._generate_resource_names(pipeline_group)

            # Get catalog, schema, and connection_name from first report in group
            target_catalog = group_reports[0]["target_catalog"]
            target_schema = group_reports[0]["target_schema"]
            connection_name = group_reports[0]["connection_name"]

            logger.debug(f"Pipeline {pipeline_group}: {len(group_reports)} reports -> {target_catalog}.{target_schema}")

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

            # Add reports to this pipeline
            for item in group_reports:
                report_entry = {
                    "report": {
                        "source_url": item["source_url"],
                        "destination_catalog": item["target_catalog"],
                        "destination_schema": item["target_schema"],
                        "destination_table": item["target_table_name"],
                    }
                }

                # Add table_configuration (primary_keys are required for Workday Reports)
                table_config = {}

                # Add primary keys (required for Workday Reports)
                if (
                    "primary_keys" in item
                    and pd.notna(item["primary_keys"])
                    and item["primary_keys"].strip()
                ):
                    primary_keys = [
                        key.strip() for key in str(item["primary_keys"]).split(",")
                    ]
                    table_config["primary_keys"] = primary_keys
                else:
                    raise ValueError(
                        f"Missing required primary_keys for report '{item['target_table_name']}'. "
                        f"Workday Reports require at least one primary key column. "
                        f"Please specify primary_keys in your CSV (e.g., 'Employee_ID' or 'Worker_ID,Effective_Date')"
                    )

                # Add include_columns if specified
                if (
                    "include_columns" in item
                    and pd.notna(item["include_columns"])
                    and item["include_columns"].strip()
                ):
                    include_cols = [
                        col.strip() for col in str(item["include_columns"]).split(",")
                    ]
                    table_config["include_columns"] = include_cols

                # Add exclude_columns if specified
                if (
                    "exclude_columns" in item
                    and pd.notna(item["exclude_columns"])
                    and item["exclude_columns"].strip()
                ):
                    exclude_cols = [
                        col.strip() for col in str(item["exclude_columns"]).split(",")
                    ]
                    table_config["exclude_columns"] = exclude_cols

                # Add SCD type if specified
                if (
                    "scd_type" in item
                    and pd.notna(item["scd_type"])
                    and item["scd_type"].strip()
                ):
                    scd_type = str(item["scd_type"]).strip().upper()
                    # Validate SCD type
                    if scd_type in ["SCD_TYPE_1", "SCD_TYPE_2"]:
                        table_config["scd_type"] = scd_type
                    else:
                        logger.warning(f"Invalid scd_type '{scd_type}' for report {item['target_table_name']}, skipping")

                if table_config:
                    report_entry["report"]["table_configuration"] = table_config

                pipeline_def["ingestion_definition"]["objects"].append(report_entry)

            pipelines[names["pipeline_resource_name"]] = pipeline_def

        return {"resources": {"pipelines": pipelines}}
