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


def load_input_csv(
    input_csv: str
) -> pd.DataFrame:
    """
    Load and validate input CSV configuration file.

    Args:
        input_csv (str): Path to input CSV file

    Returns:
        pd.DataFrame: Loaded configuration dataframe

    Raises:
        FileNotFoundError: If input file does not exist
        ValueError: If CSV is empty or cannot be parsed
    """
    input_path = Path(input_csv)

    if not input_path.exists():
        raise FileNotFoundError(
            f"Input file not found: {input_csv}\n\n"
            f"Please create an input CSV with the following columns:\n"
            f"  - source_database, source_schema, source_table_name\n"
            f"  - target_catalog, target_schema, target_table_name\n"
            f"  - prefix, priority\n"
            f"  - connection_name (optional)\n"
            f"  - schedule (optional)"
        )

    try:
        df = pd.read_csv(input_csv)
    except Exception as e:
        raise ValueError(f"Failed to parse CSV file: {e}")

    if df.empty:
        raise ValueError(f"Input CSV is empty: {input_csv}")

    print(f"✓ Loaded {len(df)} rows from {input_csv}")

    return df


def process_input_config(
    df: pd.DataFrame,
    required_columns: list,
    optional_columns: dict,
    override_input_config: dict = None
) -> pd.DataFrame:
    """
    Validate and normalize input configuration DataFrame.

    This function ensures all required columns are present, adds optional columns
    with defaults if missing, and fills empty/NaN values appropriately.

    The input DataFrame can come from any source (CSV, Delta table, or code).
    The output DataFrame will have all required and optional columns with clean values
    (no NaN, empty strings replaced with defaults).

    Args:
        df (pd.DataFrame): Input DataFrame from any source (CSV, Delta, code)
        required_columns (list): List of required column names that must be present.
            Example for Salesforce:
            [
                'source_database', 'source_schema', 'source_table_name',
                'target_catalog', 'target_schema', 'target_table_name',
                'prefix', 'priority', 'connection_name'
            ]
        optional_columns (dict): Dictionary of optional columns with their default values.
            Missing columns will be added, NaN/empty values will be filled with defaults.
            Example:
            {
                'schedule': '*/15 * * * *',
                'include_columns': '',
                'exclude_columns': ''
            }
        override_input_config (dict, optional): Dictionary of column overrides.
            Values in these columns will be replaced with the override value for ALL rows.
            This is useful for forcing specific values across the entire configuration.
            Example:
            {
                'schedule': '*/30 * * * *',  # Override schedule for all rows
                'target_catalog': 'bronze'   # Force all to bronze catalog
            }

    Returns:
        pd.DataFrame: Normalized DataFrame with all required and optional columns,
                     NaN values filled, empty strings replaced with defaults,
                     and any overrides applied

    Raises:
        ValueError: If required columns are missing
        ValueError: If DataFrame is empty

    Example Usage:
        >>> required = [
        ...     'source_database', 'source_schema', 'source_table_name',
        ...     'target_catalog', 'target_schema', 'target_table_name',
        ...     'prefix', 'priority', 'connection_name'
        ... ]
        >>> optional = {
        ...     'schedule': '*/15 * * * *',
        ...     'include_columns': '',
        ...     'exclude_columns': ''
        ... }
        >>> override = {'schedule': '*/30 * * * *'}  # Override schedule for all
        >>> normalized_df = process_input_config(df, required, optional, override)
    """
    # Make a copy to avoid modifying the original dataframe
    df = df.copy()

    # Check if dataframe is empty
    if df.empty:
        raise ValueError("Input DataFrame is empty")

    # Validate required columns exist
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(
            f"Missing required columns: {missing_columns}\n"
            f"Required columns: {', '.join(required_columns)}"
        )

    # Add optional columns if not present and handle NaN/empty values
    for col_name, default_value in optional_columns.items():
        if col_name not in df.columns:
            print(f"Info: '{col_name}' column not found. Adding with default: {default_value}")
            df[col_name] = default_value
        else:
            # Fill NaN values with default
            df[col_name] = df[col_name].fillna(default_value)

            # Replace empty strings with default (for string columns)
            if isinstance(default_value, str):
                mask = df[col_name].astype(str).str.strip() == ''
                df.loc[mask, col_name] = default_value

    # Apply overrides if provided
    if override_input_config:
        for col_name, override_value in override_input_config.items():
            print(f"Info: Overriding '{col_name}' column with value: {override_value}")
            df[col_name] = override_value

    print(f"\n✓ Configuration validated: {len(df)} rows with all required and optional columns")

    return df


def generate_pipeline_config(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Generate pipeline configuration from Salesforce table list using prefix + priority grouping.

    This function expects a clean DataFrame (output from read_input_config).

    Logic:
    - Each unique combination of (prefix, priority) becomes one pipeline
    - Pipeline name format: {prefix}_{priority}
    - Tables with same prefix+priority are grouped together
    - No load balancing - grouping is explicit via prefix+priority

    Args:
        df (pd.DataFrame): Clean input dataframe (from read_input_config) with columns:
            - source_database, source_schema, source_table_name
            - target_catalog, target_schema, target_table_name
            - prefix, priority
            - connection_name, schedule (already validated)
            - include_columns, exclude_columns (optional, already present)

    Returns:
        pd.DataFrame: Configuration dataframe with additional column:
            - pipeline_group: Pipeline group identifier (prefix_priority)

    Raises:
        ValueError: If required columns are missing
    """
    # Make a copy to avoid modifying the original dataframe
    df = df.copy()

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
        optional_columns = {
            'schedule': args.schedule,
            'include_columns': '',
            'exclude_columns': ''
        }

        # Normalize and validate configuration
        normalized_df = process_input_config(
            df=input_df,
            required_columns=required_columns,
            optional_columns=optional_columns
        )

        # Generate pipeline configuration
        output_df = generate_pipeline_config(df=normalized_df)

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
