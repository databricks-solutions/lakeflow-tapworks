#!/usr/bin/env python3
"""
Generates Databricks Asset Bundle (DAB) YAML for Salesforce ingestion pipelines.
based on the configuration defined in a CSV file

Usage:
    python generate_salesforce_pipeline.py <csv_path> [output_dir] [--connection <name>]

Example:
    python generate_salesforce_pipeline.py salesforce_config.csv
    python generate_salesforce_pipeline.py salesforce_config.csv --connection my-sfdc-conn
    python generate_salesforce_pipeline.py /path/to/config.csv /path/to/output --connection my-sfdc-conn
"""

import csv
import yaml
import json
import sys
from pathlib import Path

def read_config_csv(csv_path: str) -> list:
    """Read configuration from CSV file."""
    config_data = []
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            item = {
                'group': int(row['pipeline_group']),
                'source_table': row['source_table_name'],
                'target_table': row['target_table_name'],
                'target_catalog': row['target_catalog'],
                'target_schema': row['target_schema'],
                'connection_name': row['connection_name']
            }

            # Parse schedule if present
            if 'schedule' in row and row['schedule'].strip():
                item['schedule'] = row['schedule'].strip()

            # Parse include_columns if present
            if 'include_columns' in row and row['include_columns'].strip():
                item['include_columns'] = [col.strip() for col in row['include_columns'].split(',')]

            # Parse exclude_columns if present
            if 'exclude_columns' in row and row['exclude_columns'].strip():
                item['exclude_columns'] = [col.strip() for col in row['exclude_columns'].split(',')]

            config_data.append(item)
    return config_data

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

def json_to_yaml(json_data: list, pipeline_name: str, pipeline_display: str,
                 connection_name: str) -> str:
    """
    Convert JSON config to YAML format for Databricks Asset Bundles.
    Based on test_dabs_SFDC.py implementation.
    """
    # Get default catalog and schema from first entry
    default_catalog = json_data[0].get("target_catalog", "bronze")
    default_schema = json_data[0].get("target_schema", "salesforce")

    yaml_dict = {
        "variables": {
            "dest_catalog": {"default": default_catalog},
            "dest_schema": {"default": default_schema},
        },
        "resources": {
            "pipelines": {
                pipeline_name: {
                    "name": pipeline_display,
                    "catalog": "${var.dest_catalog}",
                    "ingestion_definition": {
                        "connection_name": connection_name,
                        "objects": []
                    }
                }
            }
        }
    }

    # Add each table as an object in the ingestion definition
    for item in json_data:
        table_entry = {
            "table": {
                "source_schema": "objects",  # Salesforce uses 'objects' as source schema
                "source_table": item["source_table"],
                "destination_catalog": "${var.dest_catalog}",
                "destination_schema": "${var.dest_schema}",
            }
        }

        # If columns present, add include_columns
        if "columns" in item and item["columns"]:
            table_entry["table"]["table_configuration"] = {
                "include_columns": item["columns"]
            }

        yaml_dict["resources"]["pipelines"][pipeline_name]["ingestion_definition"]["objects"].append(table_entry)

    return yaml.dump(yaml_dict, sort_keys=False, default_flow_style=False)

def main():
    """Main execution function."""
    print("\nSalesforce Lakeflow Connect - Pipeline Generator\n")

    # Parse command-line arguments
    if len(sys.argv) < 2:
        print("[ERROR] CSV path required")
        print("\nUsage:")
        print(f"  {sys.argv[0]} <csv_path> [output_dir]")
        print("\nExample:")
        print(f"  {sys.argv[0]} salesforce_sample_config.csv")
        print(f"  {sys.argv[0]} /path/to/config.csv /path/to/output")
        sys.exit(1)

    csv_path = sys.argv[0]

    # If output_dir not provided, auto-detect based on CSV location
    if len(sys.argv) == 2:
        output_dir = Path(sys.argv[1])
    else:
        # Auto-detect: assume structure is project_root/csv_file and output is project_root/dab/resources
        csv_file = Path(csv_path)
        if csv_file.is_absolute():
            project_root = csv_file.parent
        else:
            project_root = Path.cwd()
        output_dir = project_root / "salesforce_dab" / "resources"

    output_dir.mkdir(parents=True, exist_ok=True)

    # Read config
    print(f"Reading configuration from {csv_path}...")
    try:
        config_data = read_config_csv(csv_path)
        print(f"✓ Loaded {len(config_data)} tables")
    except FileNotFoundError:
        print(f"✗ File not found: {csv_path}")
        sys.exit(1)
    except Exception as e:
        print(f"✗ Error reading CSV: {e}")
        sys.exit(1)

    # Extract common parameters and validate consistency
    connection_name = config_data[0]['connection_name']
    target_catalog = config_data[0]['target_catalog']
    target_schema = config_data[0]['target_schema']

    # Validate all rows use same connection and target
    inconsistent_rows = []
    for idx, row in enumerate(config_data, start=2):  # Start at 2 for CSV line number
        if row['connection_name'] != connection_name:
            inconsistent_rows.append(f"Line {idx}: Different connection_name '{row['connection_name']}'")
        if row['target_catalog'] != target_catalog:
            inconsistent_rows.append(f"Line {idx}: Different target_catalog '{row['target_catalog']}'")
        if row['target_schema'] != target_schema:
            inconsistent_rows.append(f"Line {idx}: Different target_schema '{row['target_schema']}'")

    if inconsistent_rows:
        print("\n⚠ Warning: Inconsistent configuration detected:")
        for warning in inconsistent_rows[:5]:  # Show first 5 warnings
            print(f"  {warning}")
        if len(inconsistent_rows) > 5:
            print(f"  ... and {len(inconsistent_rows) - 5} more")
        print("  Using values from first row.\n")

    print(f"Target: {target_catalog}.{target_schema}")
    print(f"Connection: ${{var.sfdc_connection_name}} (from CSV: {connection_name})")

    # Group tables by pipeline_group
    from collections import defaultdict
    groups = defaultdict(list)
    for item in config_data:
        groups[item['group']].append(item)

    print(f"\nGenerating {len(groups)} pipeline(s)...")
    for group_id, tables in sorted(groups.items()):
        print(f"  Pipeline {group_id}: {len(tables)} tables")

    # Build combined YAML with all pipeline groups
    combined_yaml = {
        "variables": {
            "dest_catalog": {"default": target_catalog},
            "dest_schema": {"default": target_schema},
            "sfdc_connection_name": {"default": connection_name},
        },
        "resources": {
            "pipelines": {},
            "jobs": {}
        }
    }

    # Collect schedules per group (use first table's schedule)
    group_schedules = {}

    # Generate a pipeline for each group
    for group_id in sorted(groups.keys()):
        group_tables = groups[group_id]
        pipeline_name = f"pipeline_sfdc_ingestion_group_{group_id}"
        pipeline_display = f"sfdc_ingestion_pipeline_group_{group_id}"

        # Get schedule from first table in group
        schedule = group_tables[0].get('schedule')
        if schedule:
            group_schedules[group_id] = schedule

            # Warn if schedules differ within same group
            schedules_in_group = set(item.get('schedule') for item in group_tables if item.get('schedule'))
            if len(schedules_in_group) > 1:
                print(f"\n⚠ Warning: Pipeline group {group_id} has different schedules:")
                for s in schedules_in_group:
                    print(f"    {s}")
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
                    "source_schema": "objects",
                    "source_table": item["source_table"],
                    "destination_catalog": "${var.dest_catalog}",
                    "destination_schema": "${var.dest_schema}",
                }
            }

            # Add table_configuration if include_columns or exclude_columns are specified
            table_config = {}
            if 'include_columns' in item:
                table_config['include_columns'] = item['include_columns']
            if 'exclude_columns' in item:
                table_config['exclude_columns'] = item['exclude_columns']

            if table_config:
                table_entry["table"]["table_configuration"] = table_config

            pipeline_def["ingestion_definition"]["objects"].append(table_entry)

        combined_yaml["resources"]["pipelines"][pipeline_name] = pipeline_def

        # Create job to schedule this pipeline
        if schedule:
            job_name = f"job_sfdc_pipeline_group_{group_id}"
            job_display = f"sfdc_pipeline_scheduler_group_{group_id}"
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

            combined_yaml["resources"]["jobs"][job_name] = job_def

    # Write combined YAML
    pipeline_file = output_dir / "sfdc_pipeline.yml"
    with open(pipeline_file, 'w') as f:
        f.write(yaml.dump(combined_yaml, sort_keys=False, default_flow_style=False))
    print(f"\n✓ Generated: {pipeline_file}")

    # Display summary
    print(f"\nPipeline summary:")
    for group_id in sorted(groups.keys()):
        group_tables = groups[group_id]
        table_names = ', '.join([item['source_table'] for item in group_tables])
        schedule_info = f" [scheduled: {group_schedules[group_id]}]" if group_id in group_schedules else ""
        print(f"  Pipeline {group_id}: {table_names}{schedule_info}")

    if group_schedules:
        print(f"\n✓ Generated {len(group_schedules)} scheduled job(s)")
        print("  Jobs will automatically trigger pipelines based on cron schedules")
    else:
        print("\n⚠ No schedules found in CSV - pipelines created without jobs")
        print("  Add 'schedule' column to CSV to enable automatic scheduling")

    print("\nNext steps:")
    print("  1. Set sfdc_connection_name in dab/databricks.yml")
    print("  2. Create Salesforce connection in Databricks UI (OAuth required)")
    print("     → Catalog → Connections → Create → Type: Salesforce")
    dab_dir = output_dir.parent.parent if output_dir.parent.name == "resources" else output_dir.parent
    print(f"  3. Deploy: cd {dab_dir} && databricks bundle deploy -t dev")
    if group_schedules:
        print("  4. Jobs will run automatically on schedule")
        print("     (or manually trigger: databricks pipelines start-update <pipeline_id>)")
    else:
        print("  4. Start pipeline: databricks pipelines start-update <pipeline_id>")
    print()

if __name__ == "__main__":
    main()
