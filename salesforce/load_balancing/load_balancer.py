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

# Add parent directory to path to import utilities
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from utilities import process_input_config, load_input_csv, generate_saas_pipeline_config


def generate_pipeline_config(
    df: pd.DataFrame,
    max_tables_per_pipeline: int = 250
) -> pd.DataFrame:
    """
    Generate pipeline configuration from Salesforce table list using prefix + priority grouping.

    This function expects a clean DataFrame (output from read_input_config).

    Logic:
    - Each unique combination of (prefix, priority) becomes one base pipeline group
    - If a group has more tables than max_tables_per_pipeline, it's split into sub-groups
    - Pipeline name format: {prefix}_{priority} or {prefix}_{priority}_g{NN} if split
    - Example: business_unit1_01_g01, business_unit1_01_g02, etc.

    Args:
        df (pd.DataFrame): Clean input dataframe (from read_input_config) with columns:
            - source_database, source_schema, source_table_name
            - target_catalog, target_schema, target_table_name
            - prefix, priority
            - connection_name, schedule (already validated)
            - include_columns, exclude_columns (optional, already present)
        max_tables_per_pipeline (int): Maximum tables per pipeline (default: 250)
            If a prefix+priority group exceeds this, it will be split into multiple pipelines

    Returns:
        pd.DataFrame: Configuration dataframe with additional column:
            - pipeline_group: Pipeline group identifier (prefix_priority or prefix_priority_gNN)

    Raises:
        ValueError: If required columns are missing
    """
    # Use shared SaaS pipeline configuration function
    return generate_saas_pipeline_config(df, max_tables_per_pipeline)


def main():
    """Main function for command-line usage"""
    parser = argparse.ArgumentParser(
        description="Generate Salesforce pipeline configuration using prefix + priority grouping",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Use default example file
  python load_balancer.py

  # Specify input and output files
  python load_balancer.py --input-csv my_tables.csv --output-csv my_output.csv

  # With custom schedule
  python load_balancer.py \\
    --input-csv my_tables.csv \\
    --schedule "*/30 * * * *"

Note: connection_name is now a required column in the CSV file.
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
        '--schedule',
        type=str,
        default='*/15 * * * *',
        help='Default cron schedule (default: */15 * * * *)'
    )

    args = parser.parse_args()

    print(f"Reading input CSV: {args.input_csv}")

    try:
        # Load input CSV using dedicated function
        input_df = load_input_csv(args.input_csv)

        # Define required and optional columns for Salesforce
        required_columns = [
            'source_database', 'source_schema', 'source_table_name',
            'target_catalog', 'target_schema', 'target_table_name',
            'prefix', 'priority', 'connection_name'
        ]
        default_values = {
            'schedule': args.schedule,
            'include_columns': '',
            'exclude_columns': ''
        }

        # Normalize and validate configuration
        normalized_df = process_input_config(
            df=input_df,
            required_columns=required_columns,
            default_values=default_values
        )

        # Generate pipeline configuration
        max_tables_per_pipeline = 250

        print("\n" + "="*80)
        print("SALESFORCE PIPELINE CONFIGURATION")
        print("="*80)

        output_df = generate_pipeline_config(df=normalized_df, max_tables_per_pipeline=max_tables_per_pipeline)

        # Get unique pipeline groups
        unique_pipelines = output_df['pipeline_group'].unique()
        print(f"\nTotal tables processed: {len(output_df)}")
        print(f"Total unique pipelines: {len(unique_pipelines)}")
        print(f"Max tables per pipeline: {max_tables_per_pipeline}")

        print("\n" + "-"*80)
        print("Pipeline Breakdown:")
        print("-"*80)

        # Group by pipeline and show details
        for pipeline_name in sorted(unique_pipelines):
            pipeline_df = output_df[output_df['pipeline_group'] == pipeline_name]

            # Extract prefix and priority
            prefix = pipeline_df['prefix'].iloc[0]
            priority = pipeline_df['priority'].iloc[0]
            connection = pipeline_df['connection_name'].iloc[0]
            schedule = pipeline_df['schedule'].iloc[0]

            # Check if this was split
            split_indicator = " (split)" if "_g" in pipeline_name else ""

            print(f"\nPipeline: {pipeline_name}{split_indicator}")
            print(f"  Prefix: {prefix}")
            print(f"  Priority: {priority}")
            print(f"  Connection: {connection}")
            print(f"  Schedule: {schedule}")
            print(f"  Tables: {len(pipeline_df)}")

            # Show table list
            for _, row in pipeline_df.iterrows():
                print(f"    - {row['source_table_name']} → {row['target_catalog']}.{row['target_schema']}.{row['target_table_name']}")

        print("\n" + "="*80)
        print("Summary by Prefix:")
        print("="*80)

        # Group by prefix to show high-level summary
        for prefix in sorted(output_df['prefix'].unique()):
            prefix_df = output_df[output_df['prefix'] == prefix]
            priorities = sorted([str(p) for p in prefix_df['priority'].unique()])

            print(f"\nPrefix: {prefix}")
            print(f"  Priorities: {', '.join(priorities)}")
            print(f"  Pipelines: {prefix_df['pipeline_group'].nunique()}")
            print(f"  Total tables: {len(prefix_df)}")

        print("\n" + "="*80)

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
