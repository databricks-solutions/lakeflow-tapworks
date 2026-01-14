#!/usr/bin/env python3
"""
Generates Databricks Asset Bundle (DAB) YAML for Salesforce ingestion pipelines.

Groups pipelines by pipeline_group (which uses prefix_priority format).

Usage:
    # As a module
    from deployment.generate_dab_yaml import generate_yaml_files
    generate_yaml_files(df, connection_name, output_path)

    # Command-line
    python generate_dab_yaml.py <csv_path> [--output <path>] [--connection <name>]

Example:
    python generate_dab_yaml.py ../load_balancing/examples/output_config.csv
    python generate_dab_yaml.py config.csv --output resources/pipelines.yml --connection my_sfdc_conn
"""

import pandas as pd
import yaml
import sys
import argparse
from pathlib import Path
from collections import defaultdict


def convert_cron_to_quartz(cron_expression: str) -> str:
    """
    Convert standard 5-field cron to Quartz 6-field cron format.

    Standard cron: minute hour day month day-of-week
    Quartz cron:   second minute hour day month day-of-week

    Examples:
        */15 * * * *  → 0 */15 * * * ?
        0 */6 * * *   → 0 0 */6 * * ?
        0 0 * * *     → 0 0 0 * * ?
        0 9 * * 1     → 0 0 9 ? * 1
    """
    parts = cron_expression.strip().split()

    if len(parts) != 5:
        # If already 6+ fields, assume it's Quartz format
        return cron_expression

    # Add seconds=0 at start
    minute, hour, day, month, dow = parts

    # In Quartz cron, you must use ? for either day-of-month OR day-of-week (not both can be *)
    # If day-of-week is *, use ? for day-of-week
    # If day-of-week is specified (not *), use ? for day-of-month
    if dow == '*':
        dow = '?'
    else:
        # day-of-week is specified, so day-of-month must be ?
        day = '?'

    quartz_cron = f"0 {minute} {hour} {day} {month} {dow}"
    return quartz_cron


def create_databricks_yml(project_name: str, workspace_host: str, default_catalog: str,
                          default_schema: str, connection_name: str) -> dict:
    """
    Create the main databricks.yml file for the SFDC DAB project.

    Args:
        project_name (str): Project name for the bundle
        workspace_host (str): Workspace host URL
        default_catalog (str): Default target catalog for Salesforce data
        default_schema (str): Default target schema for Salesforce data
        connection_name (str): Salesforce connection name

    Returns:
        dict: The databricks.yml structure

    Note:
        SFDC databricks.yml includes variables at the root level (unlike SQL Server).
        This allows the same bundle to be deployed to different environments by
        overriding variables.
    """
    return {
        'bundle': {
            'name': project_name
        },
        'include': [
            'resources/*.yml'
        ],
        'targets': {
            'dev': {
                'mode': 'development',
                'default': True,
                'workspace': {
                    'host': workspace_host
                }
            }
        },
        'variables': {
            'dest_catalog': {
                'description': 'Target catalog for Salesforce data',
                'default': default_catalog
            },
            'dest_schema': {
                'description': 'Target schema for Salesforce data',
                'default': default_schema
            },
            'sfdc_connection_name': {
                'description': 'Salesforce connection name in Databricks',
                'default': connection_name
            }
        }
    }


def generate_yaml_files(
    df: pd.DataFrame,
    connection_name: str,
    project_name: str = "sfdc_ingestion",
    workspace_host: str = None,
    output_dir: str = "dab_deployment"
) -> None:
    """
    Generate Databricks Asset Bundle YAML files for Salesforce ingestion pipelines.

    Creates a complete DAB structure with:
    - databricks.yml (root configuration)
    - resources/sfdc_pipeline.yml (pipeline and job definitions)

    Args:
        df (pd.DataFrame): Pipeline configuration dataframe with columns:
            - source_table_name: Salesforce object name
            - target_catalog: Target Databricks catalog
            - target_schema: Target Databricks schema
            - target_table_name: Target table name
            - pipeline_group: Pipeline group identifier (e.g., "business_unit1_01")
            - connection_name: Salesforce connection name
            - schedule: Cron schedule expression
            - include_columns: (optional) Comma-separated list of columns to include
            - exclude_columns: (optional) Comma-separated list of columns to exclude
        connection_name (str): Salesforce connection name in Databricks
        project_name (str): Project name for the bundle (default: "sfdc_ingestion")
        workspace_host (str): Workspace host URL (optional, can be set later)
        output_dir (str): Output directory for DAB project (default: "dab_deployment")

    Returns:
        None (writes YAML files to disk)
    """
    print("\n" + "="*80)
    print("GENERATING DATABRICKS ASSET BUNDLE YAML")
    print("="*80)

    # Validate required columns
    required_columns = [
        'source_table_name', 'target_catalog', 'target_schema',
        'target_table_name', 'pipeline_group', 'connection_name', 'schedule'
    ]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    # Get default catalog and schema from first entry
    default_catalog = df['target_catalog'].iloc[0]
    default_schema = df['target_schema'].iloc[0]

    print(f"\nConfiguration:")
    print(f"  Target: {default_catalog}.{default_schema}")
    print(f"  Connection: {connection_name}")
    print(f"  Total tables: {len(df)}")

    # Group tables by pipeline_group
    groups = defaultdict(list)
    for idx, row in df.iterrows():
        groups[row['pipeline_group']].append(row)

    print(f"  Unique pipelines: {len(groups)}")

    # Build pipeline resources YAML (variables will be in databricks.yml)
    pipeline_resources_yaml = {
        "resources": {
            "pipelines": {},
            "jobs": {}
        }
    }

    print("\n" + "-"*80)
    print("Pipeline Details:")
    print("-"*80)

    # Generate a pipeline for each group
    for pipeline_group in sorted(groups.keys()):
        group_tables = groups[pipeline_group]

        # Create safe pipeline name (replace underscores/hyphens if needed for resource naming)
        pipeline_name = f"pipeline_sfdc_{pipeline_group}"
        pipeline_display = f"SFDC Ingestion - {pipeline_group}"

        # Get schedule from first table in group
        schedule = group_tables[0]['schedule']

        print(f"\nPipeline: {pipeline_group}")
        print(f"  Name: {pipeline_name}")
        print(f"  Schedule: {schedule}")
        print(f"  Tables: {len(group_tables)}")

        # Warn if schedules differ within same group
        schedules_in_group = set(item['schedule'] for item in group_tables if pd.notna(item['schedule']))
        if len(schedules_in_group) > 1:
            print(f"  ⚠ Warning: Different schedules detected in group:")
            for s in schedules_in_group:
                print(f"      {s}")
            print(f"  Using schedule from first table: {schedule}")

        # Create pipeline definition
        pipeline_def = {
            "name": pipeline_display,
            "catalog": "${var.dest_catalog}",
            "ingestion_definition": {
                "connection_name": "${var.sfdc_connection_name}",
                "objects": []
            }
        }

        # Add tables to this pipeline
        for item in group_tables:
            table_entry = {
                "table": {
                    "source_schema": "objects",  # Salesforce uses 'objects' as source schema
                    "source_table": item["source_table_name"],
                    "destination_catalog": "${var.dest_catalog}",
                    "destination_schema": "${var.dest_schema}",
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

        pipeline_resources_yaml["resources"]["pipelines"][pipeline_name] = pipeline_def

        # Create job to schedule this pipeline
        if pd.notna(schedule) and schedule.strip():
            job_name = f"job_sfdc_{pipeline_group}"
            job_display = f"SFDC Pipeline Scheduler - {pipeline_group}"
            quartz_schedule = convert_cron_to_quartz(schedule)

            job_def = {
                "name": job_display,
                "schedule": {
                    "quartz_cron_expression": quartz_schedule,
                    "timezone_id": "UTC"
                },
                "tasks": [
                    {
                        "task_key": "run_sfdc_pipeline",
                        "pipeline_task": {
                            "pipeline_id": f"${{resources.pipelines.{pipeline_name}.id}}"
                        }
                    }
                ]
            }

            pipeline_resources_yaml["resources"]["jobs"][job_name] = job_def

    # Create directory structure
    resources_dir = Path(output_dir) / 'resources'
    resources_dir.mkdir(parents=True, exist_ok=True)

    # Generate databricks.yml
    databricks_yaml = create_databricks_yml(
        project_name=project_name,
        workspace_host=workspace_host or "https://your-workspace.cloud.databricks.com",
        default_catalog=default_catalog,
        default_schema=default_schema,
        connection_name=connection_name
    )

    # Define output paths
    databricks_yml_path = Path(output_dir) / 'databricks.yml'
    pipeline_yml_path = resources_dir / 'sfdc_pipeline.yml'

    # Write databricks.yml
    with open(databricks_yml_path, 'w') as f:
        yaml.dump(databricks_yaml, f, sort_keys=False, default_flow_style=False)

    # Write pipeline resources
    with open(pipeline_yml_path, 'w') as f:
        yaml.dump(pipeline_resources_yaml, f, sort_keys=False, default_flow_style=False)

    print("\n" + "="*80)
    print("YAML GENERATION COMPLETE")
    print("="*80)
    print(f"\nGenerated DAB project structure in: {output_dir}")
    print(f"  ✓ {databricks_yml_path}")
    print(f"  ✓ {pipeline_yml_path}")
    print(f"\nSummary:")
    print(f"  - Pipelines: {len(pipeline_resources_yaml['resources']['pipelines'])}")
    print(f"  - Scheduled jobs: {len(pipeline_resources_yaml['resources']['jobs'])}")
    print(f"  - Total tables: {len(df)}")

    print("\nNext steps:")
    print("  1. Update workspace_host in databricks.yml if needed")
    print("  2. Ensure Salesforce connection exists in Databricks UI:")
    print("     → Catalog → Connections → Create → Type: Salesforce")
    print(f"     → Connection name: {connection_name}")
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
  python generate_dab_yaml.py ../load_balancing/examples/output_config.csv

  # With custom output path
  python generate_dab_yaml.py config.csv --output resources/my_pipeline.yml

  # With custom connection name
  python generate_dab_yaml.py config.csv --connection my_sfdc_connection
        """
    )

    parser.add_argument(
        'csv_path',
        type=str,
        help='Path to pipeline configuration CSV file'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='resources/sfdc_pipeline.yml',
        help='Output path for YAML file (default: resources/sfdc_pipeline.yml)'
    )
    parser.add_argument(
        '--connection',
        type=str,
        default=None,
        help='Salesforce connection name (default: use value from CSV)'
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

        # Get connection name from CSV or command-line argument
        if args.connection:
            connection_name = args.connection
        else:
            connection_name = df['connection_name'].iloc[0]

        # Generate YAML
        generate_yaml_files(
            df=df,
            connection_name=connection_name,
            output_path=args.output
        )

    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
