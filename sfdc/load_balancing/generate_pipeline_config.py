#!/usr/bin/env python3
"""
Salesforce Pipeline Configuration Generator

Groups Salesforce tables into pipelines based on prefix + priority combinations.

Each unique combination of (prefix, priority) creates a separate pipeline.
Pipeline naming: {prefix}_{priority}

Example:
    - prefix="business_unit1", priority="01" → pipeline "business_unit1_01"
    - prefix="business_unit1", priority="02" → pipeline "business_unit1_02"
    - prefix="business_unit2", priority="01" → pipeline "business_unit2_01"

Usage:
    python generate_pipeline_config.py
    python generate_pipeline_config.py --input-csv my_config.csv --output-csv output.csv
"""

import pandas as pd
import argparse
import sys
from pathlib import Path


def generate_pipeline_config(
    df: pd.DataFrame,
    default_connection_name: str = "sfdc_connection",
    default_schedule: str = "*/15 * * * *"
) -> pd.DataFrame:
    """
    Generate pipeline configuration from Salesforce table list using prefix + priority grouping.

    Logic:
    - Each unique combination of (prefix, priority) becomes one pipeline
    - Pipeline name format: {prefix}_{priority}
    - Tables with same prefix+priority are grouped together
    - No load balancing - grouping is explicit via prefix+priority

    Args:
        df (pd.DataFrame): Input dataframe with columns:
            - source_database: Source database (usually "Salesforce")
            - source_schema: Source schema (e.g., "standard", "custom")
            - source_table_name: Source Salesforce object name
            - target_catalog: Target Databricks catalog
            - target_schema: Target Databricks schema
            - target_table_name: Target table name
            - prefix: Pipeline prefix (e.g., "business_unit1", "business_unit2")
            - priority: Priority level (e.g., "01", "02", "03")
            - connection_name: Databricks Salesforce connection name (optional)
            - schedule: Cron schedule expression (optional)
        default_connection_name (str): Default connection name if not in CSV
        default_schedule (str): Default schedule if not in CSV

    Returns:
        pd.DataFrame: Configuration dataframe with additional columns:
            - pipeline_group: Pipeline group identifier (prefix_priority)
            - connection_name: Salesforce connection name
            - schedule: Cron schedule expression

    Raises:
        ValueError: If required columns are missing
    """
    # Make a copy to avoid modifying the original dataframe
    df = df.copy()

    # Validate required columns
    required_columns = [
        'source_database', 'source_schema', 'source_table_name',
        'target_catalog', 'target_schema', 'target_table_name',
        'prefix', 'priority'
    ]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(
            f"Missing required columns: {missing_columns}\n"
            f"Required columns: {', '.join(required_columns)}"
        )

    # Add connection_name if not present
    if 'connection_name' not in df.columns:
        print(f"Warning: 'connection_name' column not found. Using default: {default_connection_name}")
        df['connection_name'] = default_connection_name
    else:
        # Fill empty connection_name values with default
        df['connection_name'] = df['connection_name'].fillna(default_connection_name)
        df.loc[df['connection_name'].str.strip() == '', 'connection_name'] = default_connection_name

    # Add schedule if not present
    if 'schedule' not in df.columns:
        print(f"Warning: 'schedule' column not found. Using default: {default_schedule}")
        df['schedule'] = default_schedule
    else:
        # Fill empty schedule values with default
        df['schedule'] = df['schedule'].fillna(default_schedule)
        df.loc[df['schedule'].str.strip() == '', 'schedule'] = default_schedule

    # Generate pipeline_group by combining prefix + priority
    df['pipeline_group'] = df['prefix'].astype(str) + '_' + df['priority'].astype(str)

    print("\n" + "="*80)
    print("SALESFORCE PIPELINE CONFIGURATION")
    print("="*80)

    # Get unique pipeline groups
    unique_pipelines = df['pipeline_group'].unique()
    print(f"\nTotal tables processed: {len(df)}")
    print(f"Total unique pipelines: {len(unique_pipelines)}")

    print("\n" + "-"*80)
    print("Pipeline Breakdown:")
    print("-"*80)

    # Group by pipeline and show details
    for pipeline_name in sorted(unique_pipelines):
        pipeline_df = df[df['pipeline_group'] == pipeline_name]

        # Extract prefix and priority
        prefix = pipeline_df['prefix'].iloc[0]
        priority = pipeline_df['priority'].iloc[0]
        connection = pipeline_df['connection_name'].iloc[0]
        schedule = pipeline_df['schedule'].iloc[0]

        print(f"\nPipeline: {pipeline_name}")
        print(f"  Prefix: {prefix}")
        print(f"  Priority: {priority}")
        print(f"  Connection: {connection}")
        print(f"  Schedule: {schedule}")
        print(f"  Tables: {len(pipeline_df)}")

        # Show table list
        for idx, row in pipeline_df.iterrows():
            print(f"    - {row['source_table_name']} → {row['target_catalog']}.{row['target_schema']}.{row['target_table_name']}")

    print("\n" + "="*80)
    print("Summary by Prefix:")
    print("="*80)

    # Group by prefix to show high-level summary
    for prefix in sorted(df['prefix'].unique()):
        prefix_df = df[df['prefix'] == prefix]
        priorities = sorted([str(p) for p in prefix_df['priority'].unique()])

        print(f"\nPrefix: {prefix}")
        print(f"  Priorities: {', '.join(priorities)}")
        print(f"  Pipelines: {prefix_df['pipeline_group'].nunique()}")
        print(f"  Total tables: {len(prefix_df)}")

    print("\n" + "="*80)

    # Reorder columns for output
    output_columns = [
        'source_database', 'source_schema', 'source_table_name',
        'target_catalog', 'target_schema', 'target_table_name',
        'prefix', 'priority', 'pipeline_group',
        'connection_name', 'schedule'
    ]

    # Add any additional columns that might be in the input (e.g., include_columns, exclude_columns)
    extra_columns = [col for col in df.columns if col not in output_columns]
    if extra_columns:
        output_columns.extend(extra_columns)

    df_output = df[output_columns]

    return df_output


def main():
    """Main function for command-line usage"""
    parser = argparse.ArgumentParser(
        description="Generate Salesforce pipeline configuration using prefix + priority grouping",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Use default example file
  python generate_pipeline_config.py

  # Specify input and output files
  python generate_pipeline_config.py --input-csv my_tables.csv --output-csv my_output.csv

  # With custom defaults
  python generate_pipeline_config.py \\
    --input-csv my_tables.csv \\
    --connection my_sfdc_conn \\
    --schedule "*/30 * * * *"
        """
    )

    parser.add_argument(
        '--input-csv',
        type=str,
        default='examples/example_config.csv',
        help='Path to input CSV file (default: examples/example_config.csv)'
    )
    parser.add_argument(
        '--output-csv',
        type=str,
        default='examples/output_config.csv',
        help='Path to output CSV file (default: examples/output_config.csv)'
    )
    parser.add_argument(
        '--connection',
        type=str,
        default='sfdc_connection',
        help='Default Salesforce connection name (default: sfdc_connection)'
    )
    parser.add_argument(
        '--schedule',
        type=str,
        default='*/15 * * * *',
        help='Default cron schedule (default: */15 * * * *)'
    )

    args = parser.parse_args()

    # Check if input file exists
    input_path = Path(args.input_csv)
    if not input_path.exists():
        print(f"Error: Input file not found: {args.input_csv}")
        print(f"\nPlease create an input CSV with the following columns:")
        print("  - source_database, source_schema, source_table_name")
        print("  - target_catalog, target_schema, target_table_name")
        print("  - prefix, priority")
        print("  - connection_name (optional)")
        print("  - schedule (optional)")
        sys.exit(1)

    print(f"Reading input CSV: {args.input_csv}")

    try:
        # Load input CSV
        input_df = pd.read_csv(args.input_csv)

        # Generate pipeline configuration
        output_df = generate_pipeline_config(
            df=input_df,
            default_connection_name=args.connection,
            default_schedule=args.schedule
        )

        # Ensure output directory exists
        output_path = Path(args.output_csv)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Write output CSV
        output_df.to_csv(args.output_csv, index=False)
        print(f"\n✓ Output written to: {args.output_csv}")

    except Exception as e:
        print(f"\n✗ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
