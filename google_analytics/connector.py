"""
Google Analytics 4 connector implementation using OOP architecture.

This module provides the GoogleAnalyticsConnector class which implements the
SaaSConnector interface for Google Analytics 4 data sources.
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
from utilities import convert_cron_to_quartz, create_jobs, create_databricks_yml, generate_resource_names


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

        # Group properties by pipeline_group
        groups = defaultdict(list)
        for idx, row in df.iterrows():
            groups[row['pipeline_group']].append(row)

        print("\n" + "-"*80)
        print("Pipeline Details:")
        print("-"*80)

        for pipeline_group in sorted(groups.keys()):
            group_properties = groups[pipeline_group]

            # Generate resource names
            names = generate_resource_names(pipeline_group, 'ga4')

            # Get catalog, schema, and connection_name from first property in group
            target_catalog = group_properties[0]['target_catalog']
            target_schema = group_properties[0]['target_schema']
            connection_name = group_properties[0]['connection_name']

            print(f"\nPipeline: {pipeline_group}")
            print(f"  Name: {names['pipeline_resource_name']}")
            print(f"  Target: {target_catalog}.{target_schema}")
            print(f"  Connection: {connection_name}")
            print(f"  Properties: {len(group_properties)}")

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

                print(f"    - {source_schema}: {', '.join(tables)}")

            # Add pipeline
            pipelines[names['pipeline_resource_name']] = {
                "name": names['pipeline_name'],
                "catalog": target_catalog,
                "schema": target_schema,
                "ingestion_definition": {
                    "connection_name": connection_name,
                    "objects": ingestion_objects
                }
            }

        return {'resources': {'pipelines': pipelines}}

    def generate_yaml_files(self, df: pd.DataFrame, output_dir: str, targets: Dict[str, Dict]):
        """
        Generate YAML files for GA4 connector without gateways.

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
        print("GENERATING DATABRICKS ASSET BUNDLE YAML FOR GA4")
        print("="*80)

        # Validate required columns
        required_columns = [
            'source_catalog', 'source_schema', 'tables',
            'target_catalog', 'target_schema', 'connection_name',
            'pipeline_group', 'schedule', 'project_name'
        ]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        print(f"\nConfiguration:")
        print(f"  Total properties: {len(df)}")
        print(f"  Unique pipelines: {df['pipeline_group'].nunique()}")
        print(f"  Unique projects: {df['project_name'].nunique()}")

        # Group by project_name and create separate DAB packages
        for project, project_df in df.groupby('project_name'):
            project_output_dir = Path(output_dir) / str(project)
            print(f"\n  Creating DAB for project: {project}")
            print(f"    - Properties: {len(project_df)}")
            print(f"    - Pipelines: {project_df['pipeline_group'].nunique()}")
            print(f"    - Output: {project_output_dir}")

            # Generate YAML content for this project
            pipelines_yaml = self._create_pipelines(project_df, str(project))
            jobs_yaml = create_jobs(project_df, str(project), connector_type='ga4')
            databricks_yaml = create_databricks_yml(
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
