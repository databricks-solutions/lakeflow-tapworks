"""
SQL Server connector implementation using OOP architecture.

This module provides the SQLServerConnector class which implements the
DatabaseConnector interface for SQL Server data sources.
"""

import sys
import os
import yaml
import pandas as pd
from pathlib import Path
from typing import Dict

# Add parent directory to path to import utilities
sys.path.insert(0, str(Path(__file__).parent.parent))
from utilities.connectors import DatabaseConnector
from utilities import convert_cron_to_quartz, create_jobs, create_databricks_yml, generate_resource_names


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
            has_worker_type = pd.notna(worker_type) and worker_type != '' and worker_type is not None
            has_driver_type = pd.notna(driver_type) and driver_type != '' and driver_type is not None

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
            names = generate_resource_names(pipeline_group, 'sqlserver')

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
                    'pipelines.cdcApplierFetchMetadataTimeoutSeconds': '600'
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
        """
        # Group by project_name and create separate DAB packages
        for project, project_df in df.groupby('project_name'):
            project_output_dir = os.path.join(output_dir, str(project))
            print(f"\nCreating DAB for project: {project}")
            print(f"  Tables: {len(project_df)}")
            print(f"  Pipelines: {project_df['pipeline_group'].nunique()}")
            print(f"  Output: {project_output_dir}")

            # Create directory structure
            resources_dir = os.path.join(project_output_dir, 'resources')
            os.makedirs(resources_dir, exist_ok=True)

            # Generate YAML content for this project
            gateways_yaml = self._create_gateways(project_df, str(project))
            pipelines_yaml = self._create_pipelines(project_df, str(project))
            jobs_yaml = create_jobs(project_df, str(project), connector_type='sqlserver')
            databricks_yaml = create_databricks_yml(
                project_name=str(project),
                targets=targets,
                default_target='dev'
            )

            # Define output paths
            databricks_yml_path = os.path.join(project_output_dir, 'databricks.yml')
            gateway_yml_path = os.path.join(resources_dir, 'gateways.yml')
            pipeline_yml_path = os.path.join(resources_dir, 'pipelines.yml')
            jobs_yml_path = os.path.join(resources_dir, 'jobs.yml')

            # Write YAML files
            with open(databricks_yml_path, 'w') as f:
                yaml.dump(databricks_yaml, f, default_flow_style=False, sort_keys=False)

            with open(gateway_yml_path, 'w') as f:
                yaml.dump(gateways_yaml, f, default_flow_style=False, sort_keys=False)

            with open(pipeline_yml_path, 'w') as f:
                yaml.dump(pipelines_yaml, f, default_flow_style=False, sort_keys=False)

            with open(jobs_yml_path, 'w') as f:
                yaml.dump(jobs_yaml, f, default_flow_style=False, sort_keys=False)

        print(f"Generated DAB project structure in: {output_dir}")
        print(f"  - {databricks_yml_path}")
        print(f"  - {gateway_yml_path}")
        print(f"  - {pipeline_yml_path}")
        print(f"  - {jobs_yml_path}")
