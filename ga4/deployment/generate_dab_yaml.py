#!/usr/bin/env python3
"""
Generates Databricks Asset Bundle (DAB) YAML for Google Analytics 4 ingestion pipelines.

Groups pipelines by pipeline_group (which uses prefix_priority format).

Usage:
    # As a module
    from deployment.generate_dab_yaml import generate_ga4_yaml
    generate_ga4_yaml(df, output_path)

    # Command-line
    python generate_dab_yaml.py <csv_path> [--output <path>]

Example:
    python generate_dab_yaml.py ../load_balancing/examples/output_config.csv
    python generate_dab_yaml.py config.csv --output resources/pipelines.yml
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


def generate_ga4_yaml(
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
            - pipeline_group: Pipeline group identifier (e.g., "business_unit1_01")
            - schedule: Cron schedule expression
        output_path (str): Output path for YAML file

    Returns:
        None (writes YAML file to disk)
    """
    print("\n" + "="*80)
    print("GENERATING DATABRICKS ASSET BUNDLE YAML FOR GA4")
    print("="*80)

    # Validate required columns
    required_columns = [
        'source_catalog', 'source_schema', 'tables',
        'target_catalog', 'target_schema', 'pipeline_group', 'schedule'
    ]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    # Get default catalog and schema from first entry
    default_catalog = df['target_catalog'].iloc[0]
    default_schema = df['target_schema'].iloc[0]

    print(f"\nConfiguration:")
    print(f"  Target: {default_catalog}.{default_schema}")
    print(f"  Total properties: {len(df)}")

    # Group properties by pipeline_group
    groups = defaultdict(list)
    for idx, row in df.iterrows():
        groups[row['pipeline_group']].append(row)

    print(f"  Unique pipelines: {len(groups)}")

    # Build combined YAML with all pipeline groups
    combined_yaml = {
        "variables": {
            "dest_catalog": {"default": default_catalog},
            "dest_schema": {"default": default_schema},
        },
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

        # Get schedule from first property in group
        schedule = group_properties[0]['schedule']

        print(f"\nPipeline: {pipeline_group}")
        print(f"  Name: {pipeline_name}")
        print(f"  Schedule: {schedule}")
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
                        "destination_catalog": "${var.dest_catalog}",
                        "destination_schema": "${var.dest_schema}",
                        "destination_table": f"{source_schema}_{table}"
                    }
                }
                ingestion_objects.append(table_obj)

            print(f"    - {source_schema}: {', '.join(tables)}")

        # Add pipeline to YAML
        combined_yaml["resources"]["pipelines"][pipeline_name] = {
            "name": pipeline_display,
            "catalog": "${var.dest_catalog}",
            "ingestion_definition": {
                "connection_name": "ga4_connection",  # GA4 connection (placeholder)
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
  python generate_dab_yaml.py config.csv
  python generate_dab_yaml.py config.csv --output resources/ga4_pipeline.yml

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
        generate_ga4_yaml(df, args.output)

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
