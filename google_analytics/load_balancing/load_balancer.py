#!/usr/bin/env python3
"""
Google Analytics 4 Pipeline Configuration Generator

Reads GA4 configuration CSV with prefix and priority columns and generates
pipeline groupings based on prefix+priority combination.

Similar to SFDC pattern: Each unique (prefix, priority) combination creates
a separate pipeline group named "{prefix}_{priority}".

Input CSV Format:
    source_catalog,source_schema,tables,target_catalog,target_schema,prefix,priority,schedule

Output:
    Adds 'pipeline_group' column with format: {prefix}_{priority}

Usage:
    python generate_pipeline_config.py input.csv output.csv
    python generate_pipeline_config.py --help
"""

import pandas as pd
import argparse
import sys
from pathlib import Path


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
            Example for Google Analytics:
            [
                'source_catalog', 'source_schema', 'tables',
                'target_catalog', 'target_schema',
                'prefix', 'priority'
            ]
        optional_columns (dict): Dictionary of optional columns with their default values.
            Missing columns will be added, NaN/empty values will be filled with defaults.
            Example:
            {
                'schedule': '0 */6 * * *'
            }
        override_input_config (dict, optional): Dictionary of column overrides.
            Values in these columns will be replaced with the override value for ALL rows.
            This is useful for forcing specific values across the entire configuration.
            Example:
            {
                'schedule': '0 */12 * * *',  # Override schedule for all rows
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
        ...     'source_catalog', 'source_schema', 'tables',
        ...     'target_catalog', 'target_schema',
        ...     'prefix', 'priority'
        ... ]
        >>> optional = {
        ...     'schedule': '0 */6 * * *'
        ... }
        >>> normalized_df = process_input_config(df, required, optional)
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
    df: pd.DataFrame
) -> pd.DataFrame:
    """
    Generate pipeline configuration with prefix+priority grouping.

    This function expects a clean DataFrame (output from process_input_config).

    Args:
        df: Clean input DataFrame (from process_input_config) with columns:
            - source_catalog, source_schema, tables
            - target_catalog, target_schema
            - prefix, priority
            - schedule (already validated)

    Returns:
        DataFrame with pipeline_group column added
    """
    # Make a copy to avoid modifying the original dataframe
    df = df.copy()

    # Ensure prefix and priority are strings for consistent concatenation
    df['prefix'] = df['prefix'].astype(str)
    df['priority'] = df['priority'].astype(str).str.zfill(2)  # Pad to 2 digits

    # Generate pipeline_group: prefix_priority
    df['pipeline_group'] = df['prefix'] + '_' + df['priority']

    # Print summary
    print(f"Pipeline Configuration Summary:")
    print(f"  Total properties: {len(df)}")
    print(f"  Unique prefixes: {df['prefix'].nunique()}")
    print(f"  Unique priorities: {df['priority'].nunique()}")
    print(f"  Total pipeline groups: {df['pipeline_group'].nunique()}")
    print()

    # Show grouping details
    print("Pipeline Groups:")
    for group_name in sorted(df['pipeline_group'].unique()):
        group_df = df[df['pipeline_group'] == group_name]
        print(f"  {group_name}:")
        print(f"    Properties: {len(group_df)}")
        print(f"    Schemas: {', '.join(group_df['source_schema'].unique())}")
    print()

    return df


def write_output_csv(df: pd.DataFrame, output_file: str):
    """Write configuration with pipeline_group to CSV"""
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(output_file, index=False)
    print(f"Configuration saved to: {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description='Generate GA4 pipeline configuration with prefix+priority grouping',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate pipeline config from input CSV
  python generate_pipeline_config.py input.csv output.csv

  # Use custom default schedule
  python generate_pipeline_config.py input.csv output.csv --schedule "0 */12 * * *"

Input CSV Format:
  source_catalog,source_schema,tables,target_catalog,target_schema,prefix,priority,schedule
  my-gcp-project,analytics_123456789,events,ga4_connector,google_analytics,business_unit1,01,0 */6 * * *
  my-gcp-project,analytics_987654321,events,ga4_connector,google_analytics,business_unit1,02,0 */6 * * *

Output:
  Adds 'pipeline_group' column: business_unit1_01, business_unit1_02, etc.
        """
    )

    parser.add_argument(
        'input_csv',
        help='Input CSV file with GA4 properties'
    )
    parser.add_argument(
        'output_csv',
        nargs='?',
        default=None,
        help='Output CSV file with pipeline_group (default: input_config.csv)'
    )
    parser.add_argument(
        '--schedule', '-s',
        default='0 */6 * * *',
        help='Default cron schedule for pipelines (default: 0 */6 * * *)'
    )

    args = parser.parse_args()

    # Default output to same as input if not specified
    output_file = args.output_csv or args.input_csv.replace('.csv', '_config.csv')

    try:
        print("=" * 70)
        print("GA4 Pipeline Configuration Generator")
        print("=" * 70)
        print()

        # Step 1: Load input CSV
        df = load_input_csv(args.input_csv)

        # Step 2: Define required and optional columns for Google Analytics
        required_columns = [
            'source_catalog', 'source_schema', 'tables',
            'target_catalog', 'target_schema',
            'prefix', 'priority'
        ]
        optional_columns = {
            'schedule': args.schedule
        }

        # Step 3: Normalize and validate configuration
        normalized_df = process_input_config(
            df=df,
            required_columns=required_columns,
            optional_columns=optional_columns
        )

        # Step 4: Generate configuration with pipeline groups
        df_with_groups = generate_pipeline_config(normalized_df)

        # Write output
        write_output_csv(df_with_groups, output_file)

        print()
        print("=" * 70)
        print("Configuration generation complete!")
        print("=" * 70)
        print()
        print("Next steps:")
        print(f"  1. Review: {output_file}")
        print(f"  2. Generate YAML: python deployment/generate_dab_yaml.py {output_file}")
        print()

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
