"""
Salesforce connector implementation using OOP architecture.

This module provides the SalesforceConnector class which implements the
SaaSConnector interface for Salesforce data sources.
"""

import sys
import os
import yaml
import pandas as pd
from pathlib import Path
from typing import Dict
from collections import defaultdict

# Add parent directory to path to import utilities
sys.path.insert(0, str(Path(__file__).parent.parent))
from core import SaaSConnector


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

        print("\n" + "-"*80)
        print("Pipeline Details:")
        print("-"*80)

        for pipeline_group in sorted(groups.keys()):
            group_tables = groups[pipeline_group]

            # Generate resource names
            names = self._generate_resource_names(pipeline_group)

            # Get catalog, schema, and connection_name from first table in group
            target_catalog = group_tables[0]['target_catalog']
            target_schema = group_tables[0]['target_schema']
            connection_name = group_tables[0]['connection_name']

            print(f"\nPipeline: {pipeline_group}")
            print(f"  Name: {names['pipeline_resource_name']}")
            print(f"  Target: {target_catalog}.{target_schema}")
            print(f"  Connection: {connection_name}")
            print(f"  Tables: {len(group_tables)}")

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

                if table_config:
                    table_entry["table"]["table_configuration"] = table_config

                pipeline_def["ingestion_definition"]["objects"].append(table_entry)

                # Show table in output
                table_name = item["source_table_name"]
                dest = f"{item['target_table_name']}"
                col_info = ""
                if 'include_columns' in table_config:
                    col_info = f" [includes: {len(table_config['include_columns'])} cols]"
                elif 'exclude_columns' in table_config:
                    col_info = f" [excludes: {len(table_config['exclude_columns'])} cols]"
                print(f"    - {table_name} → {dest}{col_info}")

            pipelines[names['pipeline_resource_name']] = pipeline_def

        return {'resources': {'pipelines': pipelines}}

    def generate_yaml_files(self, df: pd.DataFrame, output_dir: str, targets: Dict[str, Dict]):
        """
        Generate YAML files for Salesforce connector without gateways.

        Creates a DAB structure:
        - databricks.yml (root configuration)
        - resources/pipelines.yml (pipeline definitions)
        - resources/jobs.yml (scheduled jobs)

        Args:
            df: DataFrame with pipeline configuration
            output_dir: Output directory for DAB files
            targets: Dictionary of target environments
        """
        print("\n" + "="*80)
        print("GENERATING DATABRICKS ASSET BUNDLE YAML")
        print("="*80)

        # Validate required columns
        required_columns = [
            'source_table_name', 'target_catalog', 'target_schema',
            'target_table_name', 'pipeline_group', 'connection_name', 'schedule', 'project_name'
        ]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        print(f"\nConfiguration:")
        print(f"  Total tables: {len(df)}")
        print(f"  Unique pipelines: {df['pipeline_group'].nunique()}")
        print(f"  Unique projects: {df['project_name'].nunique()}")

        # Group by project_name and create separate DAB packages
        for project, project_df in df.groupby('project_name'):
            project_output_dir = Path(output_dir) / str(project)
            print(f"\n  Creating DAB for project: {project}")
            print(f"    - Tables: {len(project_df)}")
            print(f"    - Pipelines: {project_df['pipeline_group'].nunique()}")
            print(f"    - Output: {project_output_dir}")

            # Generate YAML content for this project
            pipelines_yaml = self._create_pipelines(project_df, str(project))
            jobs_yaml = self._create_jobs(project_df, str(project))
            databricks_yaml = self._create_databricks_yml(
                project_name=str(project),
                targets=targets,
                default_target='dev'
            )

            # Create directory structure
            resources_dir = project_output_dir / 'resources'
            resources_dir.mkdir(parents=True, exist_ok=True)

            # Define output paths
            databricks_yml_path = project_output_dir / 'databricks.yml'
            pipelines_yml_path = resources_dir / 'pipelines.yml'
            jobs_yml_path = resources_dir / 'jobs.yml'

            # Write YAML files
            with open(databricks_yml_path, 'w') as f:
                yaml.dump(databricks_yaml, f, sort_keys=False, default_flow_style=False, indent=2)

            with open(pipelines_yml_path, 'w') as f:
                yaml.dump(pipelines_yaml, f, sort_keys=False, default_flow_style=False, indent=2)

            with open(jobs_yml_path, 'w') as f:
                yaml.dump(jobs_yaml, f, sort_keys=False, default_flow_style=False, indent=2)

        print("\n" + "="*80)
        print("YAML GENERATION COMPLETE")
        print("="*80)
        print(f"\nGenerated DAB project structure in: {output_dir}")
        print(f"  ✓ {databricks_yml_path}")
        print(f"  ✓ {pipelines_yml_path}")
        print(f"  ✓ {jobs_yml_path}")
