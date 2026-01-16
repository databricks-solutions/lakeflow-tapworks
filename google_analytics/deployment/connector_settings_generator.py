#!/usr/bin/env python3
"""
Generates Databricks Asset Bundle (DAB) YAML for Google Analytics 4 ingestion pipelines.

Groups pipelines by pipeline_group (which uses prefix_priority format).

Usage:
    # As a module
    from deployment.connector_settings_generator import generate_yaml_files
    generate_yaml_files(df, output_path)

    # Command-line
    python connector_settings_generator.py <csv_path> [--output <path>]

Example:
    python connector_settings_generator.py ../load_balancing/examples/output_config.csv
    python connector_settings_generator.py config.csv --output resources/pipelines.yml
"""

import pandas as pd
import yaml
import sys
import argparse
from pathlib import Path
from collections import defaultdict

# Add parent directory to path to import utilities
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from utilities import convert_cron_to_quartz


def generate_yaml_files(
    df: pd.DataFrame,
    output_path: str = "resources/ga4_pipeline.yml"
) -> None:
    """
    Generate Databricks Asset Bundle YAML for Google Analytics 4 ingestion pipelines.

    Creates one pipeline per unique pipeline_group value, with scheduled jobs
    for each pipeline based on the schedule column.

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
        output_path (str): Output path for YAML file

    Returns:
        None (writes YAML file to disk)

    Note:
        Each pipeline uses the target_catalog, target_schema, and connection_name
        from its rows in the CSV. Different pipeline groups can target different
        catalogs, schemas, or use different connections.
    """
    print("\n" + "="*80)
    print("GENERATING DATABRICKS ASSET BUNDLE YAML FOR GA4")
    print("="*80)

    # Validate required columns
    required_columns = [
        'source_catalog', 'source_schema', 'tables',
        'target_catalog', 'target_schema', 'connection_name',
        'pipeline_group', 'schedule'
    ]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    print(f"\nConfiguration:")
    print(f"  Total properties: {len(df)}")

    # Group properties by pipeline_group
    groups = defaultdict(list)
    for idx, row in df.iterrows():
        groups[row['pipeline_group']].append(row)

    print(f"  Unique pipelines: {len(groups)}")

    # Build combined YAML with all pipeline groups
    combined_yaml = {
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
        group_properties = groups[pipeline_group]

        # Create safe pipeline name
        pipeline_name = f"pipeline_ga4_{pipeline_group}"
        pipeline_display = f"GA4 Ingestion - {pipeline_group}"

        # Get catalog, schema, connection_name, and schedule from first property in group
        target_catalog = group_properties[0]['target_catalog']
        target_schema = group_properties[0]['target_schema']
        connection_name = group_properties[0]['connection_name']
        schedule = group_properties[0]['schedule']

        print(f"\nPipeline: {pipeline_group}")
        print(f"  Name: {pipeline_name}")
        print(f"  Target: {target_catalog}.{target_schema}")
        print(f"  Connection: {connection_name}")
        print(f"  Schedule: {schedule}")
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

        # Warn if schedules differ within same group
        schedules_in_group = set(item['schedule'] for item in group_properties if pd.notna(item['schedule']))
        if len(schedules_in_group) > 1:
            print(f"  ⚠ Warning: Different schedules detected in group:")
            for s in schedules_in_group:
                print(f"      {s}")
            print(f"  Using schedule from first property: {schedule}")

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

        # Add pipeline to YAML using actual values from CSV
        combined_yaml["resources"]["pipelines"][pipeline_name] = {
            "name": pipeline_display,
            "catalog": target_catalog,
            "schema": target_schema,
            "ingestion_definition": {
                "connection_name": connection_name,
                "objects": ingestion_objects
            }
        }

        # Add scheduled job for this pipeline
        job_name = f"job_ga4_{pipeline_group}"
        job_display = f"GA4 Pipeline Scheduler - {pipeline_group}"

        # Convert to Quartz cron
        quartz_cron = convert_cron_to_quartz(schedule)

        combined_yaml["resources"]["jobs"][job_name] = {
            "name": job_display,
            "schedule": {
                "quartz_cron_expression": quartz_cron,
                "timezone_id": "UTC"
            },
            "tasks": [
                {
                    "task_key": "run_ga4_pipeline",
                    "pipeline_task": {
                        "pipeline_id": f"${{resources.pipelines.{pipeline_name}.id}}"
                    }
                }
            ]
        }

        print(f"  Job: {job_name}")
        print(f"    Quartz cron: {quartz_cron}")

    # Write YAML to file
    output_path_obj = Path(output_path)
    output_path_obj.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, 'w') as f:
        yaml.dump(combined_yaml, f, default_flow_style=False, sort_keys=False, indent=2)

    print("\n" + "="*80)
    print(f"✓ YAML generated successfully: {output_path}")
    print("="*80)
    print("\nSummary:")
    print(f"  Pipelines created: {len(combined_yaml['resources']['pipelines'])}")
    print(f"  Jobs created: {len(combined_yaml['resources']['jobs'])}")
    print(f"  Total properties: {len(df)}")
    print()


def main():
    parser = argparse.ArgumentParser(
        description='Generate Databricks Asset Bundle YAML for GA4 pipelines',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example:
  python connector_settings_generator.py config.csv
  python connector_settings_generator.py config.csv --output resources/ga4_pipeline.yml

Input CSV must have columns:
  - source_catalog (GCP project)
  - source_schema (GA4 property)
  - tables (comma-separated: events,events_intraday,users)
  - target_catalog
  - target_schema
  - pipeline_group (e.g., business_unit1_01)
  - schedule (cron expression)
        """
    )

    parser.add_argument(
        'csv_file',
        help='Input CSV file with pipeline configuration'
    )
    parser.add_argument(
        '--output', '-o',
        default='resources/ga4_pipeline.yml',
        help='Output YAML file path (default: resources/ga4_pipeline.yml)'
    )

    args = parser.parse_args()

    try:
        # Read CSV
        print(f"\nReading configuration from: {args.csv_file}")
        df = pd.read_csv(args.csv_file)

        if df.empty:
            raise ValueError("Input CSV is empty")

        print(f"Loaded {len(df)} properties")

        # Generate YAML
        generate_yaml_files(df, args.output)

        print("Next steps:")
        print(f"  1. Review YAML: {args.output}")
        print(f"  2. Update GA4 connection name in YAML")
        print(f"  3. Deploy: databricks bundle deploy -t dev")
        print()

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
