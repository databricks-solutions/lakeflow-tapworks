#!/usr/bin/env python3
"""
Generates Databricks Asset Bundle (DAB) YAML for Google Analytics 4 ingestion pipelines.

Groups pipelines by pipeline_group (which uses prefix_priority format).
Creates separate YAML files for pipelines and jobs, similar to SQL Server structure.

Usage:
    # As a module
    from deployment.connector_settings_generator import generate_yaml_files
    generate_yaml_files(df, project_name, workspace_host, output_dir)

    # Command-line
    python connector_settings_generator.py <csv_path> [--project-name <name>] [--output-dir <path>]

Example:
    python connector_settings_generator.py ../load_balancing/examples/output_config.csv
    python connector_settings_generator.py config.csv --project-name my_project --output-dir dab_deployment
"""

import pandas as pd
import yaml
import sys
import argparse
import os
from pathlib import Path
from collections import defaultdict

# Add parent directory to path to import utilities
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from utilities import convert_cron_to_quartz, create_jobs, create_databricks_yml, generate_resource_names


def create_pipelines(df: pd.DataFrame, project_name: str) -> dict:
    """
    Create pipeline YAML configuration from dataframe.

    Creates one Delta Live Tables pipeline per unique pipeline_group value.

    Args:
        df (pd.DataFrame): Input dataframe containing GA4 property configurations
        project_name (str): Project name prefix for all resources

    Returns:
        dict: Pipelines YAML structure
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

        # Generate resource names using unified naming function
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

        # Warn if catalogs differ within same group
        catalogs_in_group = set(item['target_catalog'] for item in group_properties if pd.notna(item['target_catalog']))
        if len(catalogs_in_group) > 1:
            print(f"  ⚠ Warning: Different target_catalog values detected in group:")
            for c in catalogs_in_group:
                print(f"      {c}")
            print(f"  Using target_catalog from first property: {target_catalog}")

        # Warn if schemas differ within same group
        schemas_in_group = set(item['target_schema'] for item in group_properties if pd.notna(item['target_schema']))
        if len(schemas_in_group) > 1:
            print(f"  ⚠ Warning: Different target_schema values detected in group:")
            for s in schemas_in_group:
                print(f"      {s}")
            print(f"  Using target_schema from first property: {target_schema}")

        # Warn if connection_names differ within same group
        connections_in_group = set(item['connection_name'] for item in group_properties if pd.notna(item['connection_name']))
        if len(connections_in_group) > 1:
            print(f"  ⚠ Warning: Different connection_name values detected in group:")
            for c in connections_in_group:
                print(f"      {c}")
            print(f"  Using connection_name from first property: {connection_name}")

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

        # Add pipeline using actual values from CSV
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


def generate_yaml_files(
    df: pd.DataFrame,
    output_dir: str,
    targets: dict
) -> None:
    """
    Generate Databricks Asset Bundle YAML files for Google Analytics 4 ingestion pipelines.

    Creates separate DAB packages for each unique project_name in the dataframe.
    Each project gets its own directory with:
    - databricks.yml (root configuration)
    - resources/pipelines.yml (pipeline definitions)
    - resources/jobs.yml (job definitions)

    Args:
        df (pd.DataFrame): Pipeline configuration dataframe with columns:
            - source_catalog: GCP project ID
            - source_schema: GA4 property (e.g., analytics_123456789)
            - tables: Comma-separated list of tables (e.g., "events,events_intraday,users")
            - target_catalog: Target Databricks catalog
            - target_schema: Target Databricks schema
            - connection_name: GA4 connection name in Databricks
            - pipeline_group: Pipeline group identifier (e.g., "business_unit1_01")
            - schedule: Cron schedule expression
            - project_name: Project name (required)
        output_dir (str): Output directory for DAB project(s)
        targets (dict): Target environments configuration (required)
            Format: {'env_name': {'workspace_host': '...'}, ...}

    Returns:
        None (writes YAML files to disk)

    Note:
        - Always creates separate DAB package for each unique project_name
        - Output structure: output/{project_name}/databricks.yml
        - Each pipeline uses the target_catalog, target_schema, and connection_name
          from its rows in the CSV
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
        pipelines_yaml = create_pipelines(project_df, str(project))
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
    print(f"\nSummary:")
    print(f"  - Pipelines: {len(pipelines_yaml['resources']['pipelines'])}")
    print(f"  - Scheduled jobs: {len(jobs_yaml['resources']['jobs'])}")
    print(f"  - Total properties: {len(df)}")

    print("\nNext steps:")
    print("  1. Update workspace_host in databricks.yml if needed")
    print("  2. Ensure GA4 connection exists in Databricks UI:")
    print("     → Catalog → Connections → Create → Type: Google Analytics")
    print("  3. Review the generated YAML files")
    print("  4. Deploy using Databricks Asset Bundles:")
    print(f"     cd {output_dir}")
    print("     databricks bundle validate -t dev")
    print("     databricks bundle deploy -t dev")
    print("="*80 + "\n")


def main():
    parser = argparse.ArgumentParser(
        description='Generate Databricks Asset Bundle YAML for GA4 pipelines',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate from pipeline config CSV
  python connector_settings_generator.py ../load_balancing/examples/output_config.csv

  # With custom project name and output directory
  python connector_settings_generator.py config.csv --project-name my_ga4_project --output-dir dab_project

  # With workspace host
  python connector_settings_generator.py config.csv --workspace-host https://my-workspace.cloud.databricks.com

Input CSV must have columns:
  - source_catalog (GCP project)
  - source_schema (GA4 property)
  - tables (comma-separated: events,events_intraday,users)
  - target_catalog
  - target_schema
  - connection_name
  - pipeline_group (e.g., business_unit1_01)
  - schedule (cron expression)
        """
    )

    parser.add_argument(
        'csv_file',
        help='Input CSV file with pipeline configuration'
    )
    parser.add_argument(
        '--project-name',
        type=str,
        default='ga4_ingestion',
        help='Project name for the bundle (default: ga4_ingestion)'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default='dab_deployment',
        help='Output directory for DAB project (default: dab_deployment)'
    )
    parser.add_argument(
        '--workspace-host',
        type=str,
        default=None,
        help='Workspace host URL (optional, can be updated later in databricks.yml)'
    )

    args = parser.parse_args()

    try:
        # Read CSV
        print(f"\nReading configuration from: {args.csv_file}")
        df = pd.read_csv(args.csv_file)

        if df.empty:
            raise ValueError("Input CSV is empty")

        print(f"Loaded {len(df)} properties")

        # Build targets dict from CLI arguments
        workspace_url = args.workspace_host or "https://your-workspace.cloud.databricks.com"
        targets = {
            'dev': {'workspace_host': workspace_url}
        }

        # Generate YAML files
        generate_yaml_files(
            df=df,
            project_name=args.project_name,
            targets=targets,
            output_dir=args.output_dir
        )

    except FileNotFoundError:
        print(f"Error: File not found: {args.csv_file}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
