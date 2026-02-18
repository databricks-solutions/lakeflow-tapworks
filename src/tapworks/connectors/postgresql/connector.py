"""
PostgreSQL connector implementation using OOP architecture.

This module provides the PostgreSQLConnector class which implements the
DatabaseConnector interface for PostgreSQL data sources.
"""

import sys
import os
import yaml
import logging
import pandas as pd
from pathlib import Path
from typing import Dict

# Add parent directory to path to import core utilities
sys.path.insert(0, str(Path(__file__).parent.parent))
from tapworks.core import DatabaseConnector, YAMLGenerationError

# Configure module logger
logger = logging.getLogger(__name__)


class PostgreSQLConnector(DatabaseConnector):
    """
    PostgreSQL connector for Databricks Lakeflow Connect pipelines.

    Implements database connector pattern with:
    - Two-level load balancing (gateways + pipelines)
    - Gateway configuration (catalog, schema, worker/driver types)
    - Connection management per row

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
        }

    @property
    def default_project_name(self) -> str:
        return "postgresql_ingestion"

    @property
    def supported_scd_types(self) -> list:
        """Return supported SCD types for PostgreSQL connector."""
        return ["SCD_TYPE_1", "SCD_TYPE_2"]

    def generate_yaml_files(self, df: pd.DataFrame, output_dir: str, targets: Dict[str, Dict]):
        """
        Generate YAML files for PostgreSQL connector with gateways.

        Creates a complete DAB structure per project_name:
        - databricks.yml (root configuration)
        - resources/gateways.yml (gateway definitions)
        - resources/pipelines.yml (pipeline definitions)
        - resources/jobs.yml (scheduled jobs)

        Raises:
            YAMLGenerationError: If file writing fails
        """
        for project, project_df in df.groupby("project_name"):
            project_output_dir = Path(output_dir) / str(project)
            resources_dir = project_output_dir / "resources"

            try:
                resources_dir.mkdir(parents=True, exist_ok=True)
            except OSError as e:
                raise YAMLGenerationError(f"Failed to create directory {resources_dir}: {e}")

            gateways_yaml = self._create_gateways(project_df, str(project))
            pipelines_yaml = self._create_pipelines(project_df, str(project))
            jobs_yaml = self._create_jobs(project_df, str(project))
            databricks_yaml = self._create_databricks_yml(
                project_name=str(project),
                targets=targets,
                default_target="dev",
            )

            # Write YAML files with retry logic
            databricks_yml_path = project_output_dir / "databricks.yml"
            gateway_yml_path = resources_dir / "gateways.yml"
            pipeline_yml_path = resources_dir / "pipelines.yml"
            jobs_yml_path = resources_dir / "jobs.yml"

            self._write_yaml_file(databricks_yml_path, databricks_yaml)
            self._write_yaml_file(gateway_yml_path, gateways_yaml)
            self._write_yaml_file(pipeline_yml_path, pipelines_yaml)
            self._write_yaml_file(jobs_yml_path, jobs_yaml)

            logger.info(f"Created DAB for project: {project}")

