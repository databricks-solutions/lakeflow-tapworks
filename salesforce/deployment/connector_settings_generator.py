#!/usr/bin/env python3
"""
Generates Databricks Asset Bundle (DAB) YAML for Salesforce ingestion pipelines.

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
        df (pd.DataFrame): Input dataframe containing Salesforce object configurations
        project_name (str): Project name prefix for all resources

    Returns:
        dict: Pipelines YAML structure
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

        # Generate resource names using unified naming function
        names = generate_resource_names(pipeline_group, 'sfdc')

        # Get catalog, schema, and connection_name from first table in group
        target_catalog = group_tables[0]['target_catalog']
        target_schema = group_tables[0]['target_schema']
        connection_name = group_tables[0]['connection_name']

        print(f"\nPipeline: {pipeline_group}")
        print(f"  Name: {names['pipeline_resource_name']}")
        print(f"  Target: {target_catalog}.{target_schema}")
        print(f"  Connection: {connection_name}")
        print(f"  Tables: {len(group_tables)}")

        # Warn if catalogs differ within same group
        catalogs_in_group = set(item['target_catalog'] for item in group_tables if pd.notna(item['target_catalog']))
        if len(catalogs_in_group) > 1:
            print(f"  ⚠ Warning: Different target_catalog values detected in group:")
            for c in catalogs_in_group:
                print(f"      {c}")
            print(f"  Using target_catalog from first table: {target_catalog}")

        # Warn if schemas differ within same group
        schemas_in_group = set(item['target_schema'] for item in group_tables if pd.notna(item['target_schema']))
        if len(schemas_in_group) > 1:
            print(f"  ⚠ Warning: Different target_schema values detected in group:")
            for s in schemas_in_group:
                print(f"      {s}")
            print(f"  Using target_schema from first table: {target_schema}")

        # Warn if connection_names differ within same group
        connections_in_group = set(item['connection_name'] for item in group_tables if pd.notna(item['connection_name']))
        if len(connections_in_group) > 1:
            print(f"  ⚠ Warning: Different connection_name values detected in group:")
            for c in connections_in_group:
                print(f"      {c}")
            print(f"  Using connection_name from first table: {connection_name}")

        # Create pipeline definition using actual values from CSV
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


def generate_yaml_files(
    df: pd.DataFrame,
    output_dir: str,
    targets: dict
) -> None:
    """
    Generate Databricks Asset Bundle YAML files for Salesforce ingestion pipelines.

    Creates separate DAB packages for each unique project_name in the dataframe.
    Each project gets its own directory with:
    - databricks.yml (root configuration)
    - resources/pipelines.yml (pipeline definitions)
    - resources/jobs.yml (job definitions)

    Args:
        df (pd.DataFrame): Pipeline configuration dataframe with columns:
            - source_table_name: Salesforce object name
            - target_catalog: Target Databricks catalog
            - target_schema: Target Databricks schema
            - target_table_name: Target table name
            - pipeline_group: Pipeline group identifier (e.g., "business_unit1_01")
            - connection_name: Salesforce connection name
            - schedule: Cron schedule expression
            - project_name: Project name (required) - each unique value creates separate DAB
            - include_columns: (optional) Comma-separated list of columns to include
            - exclude_columns: (optional) Comma-separated list of columns to exclude
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

    Example:
        generate_yaml_files(df, 'output', targets)
        # Creates: output/project1/databricks.yml, output/project2/databricks.yml, etc.
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
        pipelines_yaml = create_pipelines(project_df, str(project))
        jobs_yaml = create_jobs(project_df, str(project), connector_type='sfdc')
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
    print(f"  - Total tables: {len(df)}")

    print("\nNext steps:")
    print("  1. Update workspace_host in databricks.yml if needed")
    print("  2. Ensure Salesforce connection exists in Databricks UI:")
    print("     → Catalog → Connections → Create → Type: Salesforce")
    print("  3. Review the generated YAML files")
    print("  4. Deploy using Databricks Asset Bundles:")
    print(f"     cd {output_dir}")
    print("     databricks bundle validate -t dev")
    print("     databricks bundle deploy -t dev")
    print("="*80 + "\n")


def main():
    """Main function for command-line usage."""
    parser = argparse.ArgumentParser(
        description="Generate Databricks Asset Bundle YAML for Salesforce pipelines",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate from pipeline config CSV
  python connector_settings_generator.py ../load_balancing/examples/output_config.csv

  # With custom project name and output directory
  python connector_settings_generator.py config.csv --project-name my_sfdc_project --output-dir dab_project

  # With workspace host
  python connector_settings_generator.py config.csv --workspace-host https://my-workspace.cloud.databricks.com
        """
    )

    parser.add_argument(
        'csv_path',
        type=str,
        help='Path to pipeline configuration CSV file'
    )
    parser.add_argument(
        '--project-name',
        type=str,
        default='sfdc_ingestion',
        help='Project name for the bundle (default: sfdc_ingestion)'
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

    # Check if input file exists
    csv_path = Path(args.csv_path)
    if not csv_path.exists():
        print(f"Error: File not found: {args.csv_path}")
        sys.exit(1)

    print(f"Reading configuration from: {args.csv_path}")

    try:
        # Load CSV
        df = pd.read_csv(args.csv_path)

        # Generate YAML files
        generate_yaml_files(
            df=df,
            project_name=args.project_name,
            workspace_host=args.workspace_host,
            output_dir=args.output_dir
        )

    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
