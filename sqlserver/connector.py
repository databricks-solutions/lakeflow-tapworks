"""
SQL Server connector implementation using OOP architecture.

This module provides the SQLServerConnector class which implements the
DatabaseConnector interface for SQL Server data sources.
"""

import sys
import os
import yaml
import logging
import pandas as pd
from pathlib import Path
from typing import Dict

# Add parent directory to path to import utilities
sys.path.insert(0, str(Path(__file__).parent.parent))
from core import DatabaseConnector, YAMLGenerationError

# Configure module logger
logger = logging.getLogger(__name__)


class SQLServerConnector(DatabaseConnector):
    """
    SQL Server connector for Databricks Lakeflow Connect pipelines.

    Implements database connector pattern with:
    - Two-level load balancing (gateways + pipelines)
    - Gateway configuration (catalog, schema, worker/driver types)
    - Connection management per row

    Required CSV columns:
    - source_database: Source SQL Server database name
    - source_schema: Source schema name (usually 'dbo')
    - source_table_name: Table name to ingest
    - target_catalog: Target Databricks catalog
    - target_schema: Target Databricks schema
    - target_table_name: Destination table name
    - connection_name: Databricks connection name for SQL Server

    Optional CSV columns:
    - project_name: Project identifier (default: 'sqlserver_ingestion')
    - prefix: Grouping prefix (default: project_name)
    - subgroup: Subgroup identifier (default: '01')
    - gateway_catalog: Gateway storage catalog (default: target_catalog)
    - gateway_schema: Gateway storage schema (default: target_schema)
    - gateway_worker_type: Worker node type (e.g., 'm5d.large', default: None)
    - gateway_driver_type: Driver node type (e.g., 'c5a.8xlarge', default: None)
    - schedule: Cron schedule (e.g., '0 0 * * *', default: None)
    """

    @property
    def connector_type(self) -> str:
        """Return connector type identifier."""
        return 'sqlserver'

    @property
    def required_columns(self) -> list:
        """
        Return required columns for SQL Server input CSV.

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
        Return default values for optional SQL Server columns.

        These values are used when columns are missing or empty.
        """
        return {
            'schedule': '*/15 * * * *',
            'gateway_catalog': None,  # Will fall back to target_catalog
            'gateway_schema': None,   # Will fall back to target_schema
            'gateway_worker_type': None,
            'gateway_driver_type': None
        }

    @property
    def supported_scd_types(self) -> list:
        """Return supported SCD types for SQL Server connector."""
        return ["SCD_TYPE_1", "SCD_TYPE_2"]

    def _create_gateways(self, df: pd.DataFrame, project_name: str) -> Dict:
        """
        Create gateway YAML configuration from dataframe.

        Args:
            df: DataFrame with gateway configuration
            project_name: Project name for resource naming

        Returns:
            Dictionary with gateway YAML configuration
        """
        gateways = {}
        unique_gateways = df.groupby('gateway').first()

        for gateway_id, row in unique_gateways.iterrows():
            gateway_name = f"{project_name}_gateway_{gateway_id}"
            pipeline_name = f"{project_name}_pipeline_{gateway_name}"

            gateway_catalog = row['gateway_catalog']
            gateway_schema = row['gateway_schema']
            worker_type = row.get('gateway_worker_type')
            driver_type = row.get('gateway_driver_type')

            gateway_config = {
                'name': gateway_name,
                'gateway_definition': {
                    'connection_name': row['connection_name'],
                    'gateway_storage_catalog': gateway_catalog,
                    'gateway_storage_schema': gateway_schema,
                    'gateway_storage_name': gateway_name,
                },
                'schema': gateway_schema,
                'continuous': True,
                'catalog': gateway_catalog
            }

            # Add cluster configuration if node types are provided
            has_worker_type = self._is_value_set(worker_type)
            has_driver_type = self._is_value_set(driver_type)

            if has_worker_type or has_driver_type:
                cluster_config = {'num_workers': 1}
                if has_worker_type:
                    cluster_config['node_type_id'] = worker_type
                if has_driver_type:
                    cluster_config['driver_node_type_id'] = driver_type
                gateway_config['clusters'] = [cluster_config]

            gateways[pipeline_name] = gateway_config

        return {'resources': {'pipelines': gateways}}

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

        for pipeline_group, group_df in df.groupby('pipeline_group'):
            gateway_id = group_df.iloc[0]['gateway']

            # Generate resource names
            names = self._generate_resource_names(pipeline_group)

            # Get target catalog and schema
            target_catalog = group_df.iloc[0]['target_catalog']
            target_schema = group_df.iloc[0]['target_schema']

            tables = [{
                'table': {
                    'source_catalog': row['source_database'],
                    'source_schema': row['source_schema'],
                    'source_table': row['source_table_name'],
                    'destination_catalog': row['target_catalog'],
                    'destination_schema': row['target_schema'],
                    'destination_table': row['target_table_name']
                }
            } for _, row in group_df.iterrows()]

            pipelines[names['pipeline_resource_name']] = {
                'name': names['pipeline_name'],
                'configuration': {
                    'pipelines.***REMOVED***': str(self.DEFAULT_TIMEOUT_SECONDS)
                },
                'ingestion_definition': {
                    'ingestion_gateway_id': f"${{resources.pipelines.{project_name}_pipeline_{project_name}_gateway_{gateway_id}.id}}",
                    'objects': tables
                },
                'schema': target_schema,
                'catalog': target_catalog
            }

        return {'resources': {'pipelines': pipelines}}

    def generate_yaml_files(self, df: pd.DataFrame, output_dir: str, targets: Dict[str, Dict]):
        """
        Generate YAML files for SQL Server connector with gateways.

        Creates a complete DAB structure:
        - databricks.yml (root configuration)
        - resources/gateways.yml (gateway definitions with cluster config)
        - resources/pipelines.yml (pipeline definitions with table mappings)
        - resources/jobs.yml (scheduled jobs)

        Args:
            df: DataFrame with pipeline configuration including 'gateway' column
            output_dir: Output directory for DAB files
            targets: Dictionary of target environments

        Raises:
            YAMLGenerationError: If file writing fails
        """
        # Group by project_name and create separate DAB packages
        for project, project_df in df.groupby('project_name'):
            project_output_dir = Path(output_dir) / str(project)
            logger.info(f"Creating DAB for project: {project}")
            logger.debug(f"  Tables: {len(project_df)}")
            logger.debug(f"  Pipelines: {project_df['pipeline_group'].nunique()}")
            logger.debug(f"  Output: {project_output_dir}")

            # Create directory structure
            resources_dir = project_output_dir / 'resources'
            try:
                resources_dir.mkdir(parents=True, exist_ok=True)
            except OSError as e:
                raise YAMLGenerationError(f"Failed to create directory {resources_dir}: {e}")

            # Generate YAML content for this project
            gateways_yaml = self._create_gateways(project_df, str(project))
            pipelines_yaml = self._create_pipelines(project_df, str(project))
            jobs_yaml = self._create_jobs(project_df, str(project))
            databricks_yaml = self._create_databricks_yml(
                project_name=str(project),
                targets=targets,
                default_target='dev'
            )

            # Write YAML files with retry logic
            databricks_yml_path = project_output_dir / 'databricks.yml'
            gateway_yml_path = resources_dir / 'gateways.yml'
            pipeline_yml_path = resources_dir / 'pipelines.yml'
            jobs_yml_path = resources_dir / 'jobs.yml'

            self._write_yaml_file(databricks_yml_path, databricks_yaml)
            self._write_yaml_file(gateway_yml_path, gateways_yaml)
            self._write_yaml_file(pipeline_yml_path, pipelines_yaml)
            self._write_yaml_file(jobs_yml_path, jobs_yaml)
